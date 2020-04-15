package main

import (
	"archive/tar"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	groupID = "ztf-go-archivist-dev-swnelson"

	messageTimeout = 5 * time.Second
	maxRuntime     = 7 * time.Hour
	updateInterval = 10 * time.Second
)

var usage = `usage: ztf-go-archivist BROKER TOPIC DESTINATION

This command reads ZTF Alert data from the provided TOPIC at BROKER, bundles it
into a TAR file, and writes it to DESTINATION.
`

func main() {
	if shouldPrintUsage() {
		fmt.Println(usage)
		os.Exit(1)
	}

	broker := os.Args[1]
	topic := os.Args[2]
	tarFilePath := os.Args[3]

	err := run(broker, topic, tarFilePath)
	if err != nil {
		log.Fatalf("fatal error: %v", err)
	}
}

func run(broker, topic, tarFilePath string) error {
	// Connect to Kafka
	stream, err := NewAlertStream(broker, groupID, topic)
	if err != nil {
		return fmt.Errorf("unable to set up alert stream: %w", err)
	}

	// Prepare a .tar file as the destination for storing the alerts.
	tarFile, err := os.Create(tarFilePath)
	if err != nil {
		return fmt.Errorf("unable to create tar file at %q: %w", tarFilePath, err)
	}
	tarWriter := tar.NewWriter(tarFile)

	// Periodically print out our progress
	progressUpdates := make(chan progressReport, 10)
	go printProgress(progressUpdates)

	// Read alerts and write them to the .tar file.
	n, err := tarAlertStream(stream, tarWriter, progressUpdates)
	if err != nil {
		return fmt.Errorf("error processing alert stream: %w", err)
	}

	// Clean up
	close(progressUpdates)

	if err = tarWriter.Close(); err != nil {
		log.Fatalf("error closing tar file: %v", err)
	}
	if err = stream.Close(); err != nil {
		log.Fatalf("error closing kafka consumer: %v", err)
	}

	fmt.Printf("done, wrote %d alerts to disk at %v\n", n, tarFilePath)
	return nil
}

func shouldPrintUsage() bool {
	if len(os.Args) != 4 {
		return true
	}
	helpStatements := []string{"help", "-h", "--help"}
	for _, h := range helpStatements {
		if os.Args[1] == h || os.Args[2] == h || os.Args[3] == h {
			return true
		}
	}
	return false
}
