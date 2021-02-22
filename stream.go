package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/ZwickyTransientFacility/ztf-go-archivist/schema"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/lz4"
)

func init() {
	kafka.RegisterCompressionCodec(lz4.NewCompressionCodec())
}

type AlertStream struct {
	kafkaStream *kafka.Reader

	alertReader *schema.AlertReader
}

func NewAlertStream(brokerAddr, groupID, topic string) (*AlertStream, error) {
	// Resolve the IP manually so we don't use an IPv6 address. Kafka's listener
	// doesn't seem to like IPv6.
	host, port, err := splitHostPort(brokerAddr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	addr, err := lookupIPv4(ctx, host)
	if err != nil {
		log.Fatalf("unable to resolve broker: %v", err)
	}
	brokerIPPort := addr.String() + ":" + port
	log.Printf("resolved broker address %q to %q", brokerAddr, brokerIPPort)

	conf := kafka.ReaderConfig{
		Brokers:     []string{brokerIPPort},
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.FirstOffset,
		RetentionTime: 24 * 14 * time.Hour,
	}
	if err = conf.Validate(); err != nil {
		return nil, err
	}
	kr := kafka.NewReader(conf)
	return &AlertStream{kafkaStream: kr}, nil
}

func (as *AlertStream) NextAlert(ctx context.Context) (*schema.Alert, error) {
	if as.alertReader == nil {
		ar, err := as.NextAlertReader(ctx)
		if err != nil {
			return nil, err
		}
		as.alertReader = ar
		return as.NextAlert(ctx)
	}

	alert, err := as.alertReader.Read()
	if err != nil {
		if err == io.EOF {
			as.alertReader = nil
			return as.NextAlert(ctx)
		}
		return nil, err
	}
	return alert, nil
}

func (as *AlertStream) NextAlertReader(ctx context.Context) (*schema.AlertReader, error) {
	m, err := as.kafkaStream.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get message from Kafka: %w", err)
	}
	body := bytes.NewReader(m.Value)
	alertReader, err := schema.NewAlertReader(body)
	if err != nil {
		return nil, fmt.Errorf("unable to deserialize message: %w", err)
	}
	return alertReader, nil
}

func (as *AlertStream) Close() error {
	return as.kafkaStream.Close()
}

// splitHostPort splits a string on the first colon to pull a hostname and a
// port out separately from an address.
func splitHostPort(hostport string) (host, port string, err error) {
	parts := strings.SplitN(hostport, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid hostport: %v", hostport)
	}
	return parts[0], parts[1], nil
}

// lookupIPv4 returns the first IPv4 address returned by the system resolver for
// the given hostname. If the hostname does not resolve to any IPv4 addresses,
// an error is returned.
func lookupIPv4(ctx context.Context, hostname string) (addr net.IP, err error) {
	res := new(net.Resolver)
	addrs, err := res.LookupIPAddr(ctx, hostname)
	if err != nil {
		return nil, err
	}
	for _, a := range addrs {
		ipv4 := a.IP.To4()
		if ipv4 != nil {
			return ipv4, nil
		}
	}
	return nil, fmt.Errorf("no ipv4 addresses found for %s", hostname)
}
