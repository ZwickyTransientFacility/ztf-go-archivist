package main

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/ZwickyTransientFacility/ztf-go-archivist/schema"
	"github.com/segmentio/kafka-go"
)

type AlertStream struct {
	kafkaStream *kafka.Reader

	alertReader *schema.AlertReader
}

func NewAlertStream(brokerAddr, groupID, topic string) (*AlertStream, error) {
	conf := kafka.ReaderConfig{
		Brokers:  []string{brokerAddr},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB

		StartOffset: kafka.FirstOffset,

		// Logger:      kafka.LoggerFunc(log.Printf),
		// ErrorLogger: kafka.LoggerFunc(log.Printf),
	}
	err := conf.Validate()
	if err != nil {
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
