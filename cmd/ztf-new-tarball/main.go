package main

import (
	"archive/tar"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ZwickyTransientFacility/ztf-go-archivist/internal/stream"
	"github.com/ZwickyTransientFacility/ztf-go-archivist/internal/tarball"
	"github.com/ZwickyTransientFacility/ztf-go-archivist/internal/ui"
)

const (
	messageTimeout = 5 * time.Second
	updateInterval = 10 * time.Second
)

var config = tarball.TarStreamConfig{
	MessageTimeout: 5 * time.Second,
	UpdateInterval: 60 * time.Second,
	MaxRuntime:     12 * time.Hour,
	MaxQuietPeriod: 12 * time.Hour,
}

func init() {
	flag.DurationVar(&config.MaxRuntime, "max-runtime", config.MaxRuntime, "maximum amount of time to run and process the stream")
	flag.DurationVar(&config.MaxQuietPeriod, "max-quiet-period", config.MaxQuietPeriod, "stop processing if no data has been received for this long")
}

var (
	broker = flag.String("broker", "partnership.alerts.ztf.uw.edu:9092",
		"hostport of the Kafka broker to connect to")
	topic = flag.String("topic", "",
		"topic name to read from the broker, like 'ztf_20200415_programid1'")
	tarFilePath = flag.String("dest", "",
		"filepath to write the tar file to")
	group = flag.String("group", "ztf-go-archivist-dev",
		"Kafka consumer group to register under for offset tracking")

	usage = func() {
		fmt.Fprint(os.Stderr, `ztf-new-tarball

This command reads ZTF Alert data from a Kafka broker and writes it to a
.tar archive file.
`)
		flag.PrintDefaults()
	}
)

func main() {
	flag.Usage = usage
	flag.Parse()
	err := run(*broker, *topic, *tarFilePath, *group)
	if err != nil {
		log.Fatalf("fatal error: %v", err)
	}
}

func run(broker, topic, tarFilePath, groupID string) error {
	log.Printf("connecting Kafka, broker=%q topic=%q groupID=%q", broker, topic, groupID)
	// Connect to Kafka
	stream, err := stream.NewAlertStream(broker, groupID, topic)
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
	config.Progress = make(chan ui.ProgressReport, 10)
	go ui.PrintProgress(config.Progress)

	// Read alerts and write them to the .tar file.
	n, err := tarball.TarAlertStream(stream, tarWriter, config)
	if err != nil {
		return fmt.Errorf("error processing alert stream: %w", err)
	}

	// Clean up
	close(config.Progress)

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
