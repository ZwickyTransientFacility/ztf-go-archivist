package main

import (
	"archive/tar"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/lz4"
)

const (
	broker   = "partnership.alerts.ztf.uw.edu:9092"
	brokerIP = "138.197.215.247:9092"
	groupID  = "ztf-go-archivist-dev"

	messageTimeout = 5 * time.Second
	maxRuntime     = 7 * time.Hour
)

func printHelp() {
	fmt.Println(`usage: ztf-go-archivist TOPIC DESTINATION

This command reads ZTF Alert data from the provided TOPIC, bundles it
into a TAR file, and writes it to DESTINATION.
`)
}

func shouldPrintHelp() bool {
	if len(os.Args) != 3 {
		return true
	}
	helpStatements := []string{"help", "-h", "--help"}
	for _, h := range helpStatements {
		if os.Args[1] == h || os.Args[2] == h {
			return true
		}
	}
	return false
}

func main() {
	if shouldPrintHelp() {
		printHelp()
		os.Exit(1)
	}

	start := time.Now()

	// Connect to the broker to receive alerts.
	topic := os.Args[1]
	kafka.RegisterCompressionCodec(lz4.NewCompressionCodec())
	stream, err := NewAlertStream(brokerIP, groupID, topic)
	if err != nil {
		log.Fatalf("fatal err setting up stream: %v", err)
	}

	// Prepare a .tar file as the destination for storing the alerts.
	tarFilePath := os.Args[2]
	tarFile, err := os.Create(tarFilePath)
	if err != nil {
		log.Fatalf("unable to create tar file at %v: %v", tarFilePath, err)
	}
	tarWriter := tar.NewWriter(tarFile)

	// Showtime: read alerts and write them to the .tar file.
	ctx := context.Background()
	n := 0
	for {
		if time.Since(start) > maxRuntime {
			break
		}

		ctx, _ := context.WithTimeout(ctx, messageTimeout)
		alert, err := stream.NextAlert(ctx)
		if err != nil {
			if err == context.DeadlineExceeded {
				log.Printf("no message in last %s", messageTimeout)
				continue
			}
			log.Fatalf("error retrieving data: %v", err)
			return
		}
		err = writeAlert(tarWriter, alert)
		if err != nil {
			log.Fatalf("error writing to tar: %v", err)
		}
		n += 1
	}

	err = tarWriter.Close()
	if err != nil {
		log.Fatalf("error closing tar file: %v", err)
	}

	err = stream.Close()
	if err != nil {
		log.Fatalf("error closing kafka consumer: %v", err)
	}

	fmt.Printf("done, wrote %d alerts to disk at %v\n", n, tarFilePath)
}
