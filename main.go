package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	logging "cloud.google.com/go/logging/apiv2"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/spf13/viper"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

type Config struct {
	ProgramName string   `mapstructure:"program_name,omitempty" json:"program_name,omitempty"`
	Protocol    string   `mapstructure:"protocol,omitempty" json:"protocol,omitempty"`
	Host        string   `mapstructure:"host,omitempty" json:"host,omitempty"`
	Port        int      `mapstructure:"port,omitempty" json:"port,omitempty"`
	Tls         bool     `mapstructure:"tls,omitempty" json:"tls,omitempty"`
	Resources   []string `mapstructure:"resources" json:"resources"`
	Query       string   `mapstructure:"query" json:"query"`
	Credential  string   `mapstructure:"credential,omitempty" json:"credential,omitempty"`
	Debug       bool     `mapstructure:"debug,omitempty" json:"debug,omitempty"`
}

var debug = false

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := readConfig()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	debug = cfg.Debug

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	l, err := NewSyslog(cfg.ProgramName, cfg.Protocol, addr, cfg.Tls)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer l.Close()

	gcp, err := NewLoggingClient(ctx, &LoggingClientConfig{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer gcp.Close()
	var wg sync.WaitGroup
	wg.Add(1)

	fmt.Printf("Reading log from:\n - %s\nQuery:\n %s\nSyslog : %s %s\n",
		strings.Join(cfg.Resources, "\n - "),
		cfg.Query,
		cfg.Protocol,
		addr,
	)
	go func() {
		for {
			var stream *TailStream
			select {
			case <-ctx.Done():
				if stream != nil {
					stream.Close()
				}
				wg.Done()
				return
			default:
				stream, err := gcp.Tail(cfg.Resources, cfg.Query)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
				for log := range stream.Read() {
					i, err := l.Write(log)
					if debug {
						fmt.Printf("Byte send: %d\n", i)
					}
					if err != nil {
						fmt.Println(err)
					}
				}
			}
		}
	}()
	<-ctx.Done()
	fmt.Println("Shutting down")
	wg.Wait()
}

func readConfig() (*Config, error) {
	viper.SetDefault("debug", false)
	viper.SetDefault("program_name", "gcp_syslog")
	viper.SetDefault("protocol", "udp")
	viper.SetDefault("host", "localhost")
	viper.SetDefault("port", 514)
	viper.SetDefault("tls", false)
	viper.AddConfigPath(".")
	viper.SetConfigName("config.yml")
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}
	var config Config
	err = viper.Unmarshal(&config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func NewSyslog(name string, protocol string, address string, secure bool) (*Syslog, error) {
	var conn io.WriteCloser
	if secure {
		// TODO: implement secure
		c, err := tls.Dial(protocol, address, &tls.Config{InsecureSkipVerify: true})
		if err != nil {
			return nil, err
		}
		conn = c
	} else {
		c, err := net.Dial(protocol, address)
		if err != nil {
			return nil, err
		}
		conn = c
	}
	return &Syslog{Name: name, conn: conn}, nil
}

type Syslog struct {
	Name    string
	conn    io.WriteCloser
}

func (s *Syslog) Write(b []byte) (int, error) {
	timestamp := time.Now().Format(time.RFC3339)
	header := fmt.Sprintf("%s %s:", timestamp, s.Name)
	data := fmt.Sprintf("{\"gcp\":%s,\"integration\":\"gcp\"}", b)
	send, err := fmt.Fprintf(s.conn, "%s %s\n", header, data)
	if debug {
		fmt.Printf("%s %s\n", header, data)
	}
	if err != nil {
		return 0, err
	}
	
	return send, nil
}

func (s *Syslog) Close() error {
	return s.conn.Close()
}

type LoggingClientConfig struct {
	CredentialsFile string
}

type LoggingClient struct {
	ctx    context.Context
	config *LoggingClientConfig
	client *logging.Client
}

func NewLoggingClient(ctx context.Context, config *LoggingClientConfig) (*LoggingClient, error) {
	opts := make([]option.ClientOption, 0)
	if len(strings.TrimSpace(config.CredentialsFile)) > 0 {
		opts = append(opts, option.WithCredentialsFile(config.CredentialsFile))
	}
	client, err := logging.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &LoggingClient{
		ctx:    ctx,
		config: config,
		client: client,
	}, nil
}

type TailStream struct {
	stream loggingpb.LoggingServiceV2_TailLogEntriesClient
	ch     chan []byte
}

func (t *TailStream) Read() chan []byte {
	return t.ch
}
func (t *TailStream) Close() error {
	return t.stream.CloseSend()
}

func (s *LoggingClient) Tail(resources []string, filter string) (*TailStream, error) {
	ch := make(chan []byte)
	stream, err := s.client.TailLogEntries(s.ctx)
	if err != nil {
		return nil, err
	}
	req := &loggingpb.TailLogEntriesRequest{
		ResourceNames: resources,
		Filter:        filter,
		BufferWindow:  &durationpb.Duration{Seconds: 1},
	}
	if err := stream.Send(req); err != nil {
		return nil, err
	}
	go func() {
		defer close(ch)
		for {
			entries, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return
			}
			for _, v := range entries.Entries {
				j, err := protojson.Marshal(v)
				if err != nil {
					fmt.Println(err)
				}
				ch <- j
			}
		}
	}()
	return &TailStream{stream: stream, ch: ch}, err
}

func (s *LoggingClient) Close() error {
	if err := s.client.Close(); err != nil {
		return err
	}
	return nil
}
