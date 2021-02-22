package main

import (
	"archive/tar"
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ZwickyTransientFacility/ztf-go-archivist/internal/gzappend"
	"github.com/ZwickyTransientFacility/ztf-go-archivist/internal/stream"
	"github.com/ZwickyTransientFacility/ztf-go-archivist/internal/tarball"
)

/*
   This program takes in a mapping of ZTF Kafka Topic names to .tar.gz files. It
   also takes a consumer group name.

   For each ZTF Kafka Topic, it tries to ensure that all messages from the topic
   are in the .tar.gz file.

   To do this, first it makes sure the consumer group has a committed offset
   position for the topic. If not, things are too uncertain to proceed, so we
   skip that topic entirely.

   But if the consumer group does have some stored offset for the topic, we
   subscribe to the topic to request any additional messages. Those additional
   messages are appended to the .tar.gz file.

   In most cases, we expect there to be no more messages at all. If so, we
   immediately move on to check the next topic.
*/

// Constants

var config = tarball.TarStreamConfig{
	UpdateInterval: time.Minute,
	MaxRuntime:     12 * time.Hour,
	MessageTimeout: 20 * time.Second,
	MaxQuietPeriod: time.Minute,
	Progress:       nil,
}

// Flags

var (
	targets    targetFlags = make(targetFlags)
	brokerAddr string
	cgroup     string
)

func init() {
	flag.Var(&targets, "target", "topic:tarpath pair to append to, can be specified multiple times")
	flag.StringVar(&cgroup, "group", "consumer group to run under", "ztf-go-archivist")
	flag.StringVar(&brokerAddr, "broker", "partnership.alerts.ztf.uw.edu:9092",
		"hostport of the Kafka broker to connect to")
}

// Run the program. targets should be a mapping of ZTF topic names to
// filepaths of associated tarballs.
func run(cgroup string, targets map[string]string) error {
	ctx := context.Background()

	// Winnow the set of targets to ones that are missing data.
	missingData, err := filter(ctx, cgroup, targets)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(len(missingData))
	for topic, tarDest := range missingData {
		s, err := stream.NewAlertStream(brokerAddr, cgroup, topic)
		if err != nil {
			return err
		}
		go func(file string, stream *stream.AlertStream) {
			appendStreamToTarfile(file, stream)
			defer wg.Done()
		}(tarDest, s)
	}

	wg.Wait()
	return nil
}

func appendStreamToTarfile(file string, stream *stream.AlertStream) error {
	defer stream.Close()

	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	gzWriter := gzappend.NewGzipAppender(f)
	defer gzWriter.Close()

	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	n, err := tarball.TarAlertStream(stream, tarWriter, config)
	if err != nil {
		return err
	}
	log.Printf("appended %d alerts to %q", n, file)

	return nil
}

// Winnow the set of targets to ones that are missing data.
func filter(ctx context.Context, cgroup string, targets map[string]string) (map[string]string, error) {
	missingData := make(map[string]string)

	topics := make([]string, 0)
	for topic, _ := range targets {
		topics = append(topics, topic)
	}
	lagByTopic, err := stream.ConsumerLag(ctx, brokerAddr, cgroup, topics)
	if err != nil {
		return nil, err
	}
	for topic, lag := range lagByTopic {
		if lag > 0 {
			missingData[topic] = targets[topic]
		}
	}
	return missingData, nil
}

type targetFlags map[string]string

func (t targetFlags) String() string {
	tgtStrings := make([]string, 0)
	for topic, tarPath := range t {
		tgtStrings = append(tgtStrings, topic+":"+tarPath)
	}
	return strings.Join(tgtStrings, ",")
}

func (t targetFlags) Set(value string) error {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return errors.New("format should be topic:tarpath")
	}
	t[parts[0]] = parts[1]
	return nil
}

func main() {
	flag.Parse()
	err := run(cgroup, targets)
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
