package tarball

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/ZwickyTransientFacility/ztf-go-archivist/internal/schema"
	"github.com/ZwickyTransientFacility/ztf-go-archivist/internal/stream"
	"github.com/ZwickyTransientFacility/ztf-go-archivist/internal/ui"
	"github.com/actgardner/gogen-avro/container"
)

type TarStreamConfig struct {
	UpdateInterval time.Duration
	MaxRuntime     time.Duration
	MessageTimeout time.Duration
	MaxQuietPeriod time.Duration

	Progress chan ui.ProgressReport
}

func TarAlertStream(stream *stream.AlertStream, tarWriter *tar.Writer, conf TarStreamConfig) (int, error) {
	var (
		total          = 0
		batch          = ui.ProgressReport{}
		progressTicker = time.NewTicker(conf.UpdateInterval)
		overallTimer   = time.NewTimer(conf.MaxRuntime)
		lastMessage    = time.Now()
	)
	defer progressTicker.Stop()
	defer overallTimer.Stop()

	for {
		// Emit progress updates, and eventually exit
		select {
		case <-overallTimer.C:
			// Time's up!
			total += batch.NEvents
			return total, nil
		case <-progressTicker.C:
			conf.Progress <- batch
			total += batch.NEvents
			batch = ui.ProgressReport{}
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), conf.MessageTimeout)
		defer cancel()
		alert, err := stream.NextAlert(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// Could just be a quiet period.
				if time.Since(lastMessage) > conf.MaxQuietPeriod {
					// We've had a long silence. There's probably no more data coming.
					return total, nil
				}
				continue
			}
			total += batch.NEvents
			return total, fmt.Errorf("error retrieving data: %v", err)
		}

		err = writeAlert(tarWriter, alert)
		if err != nil {
			log.Fatalf("error writing to tar: %v", err)
		}
		batch.NEvents += 1
		lastMessage = time.Now()
	}
}

func writeAlert(w *tar.Writer, a *schema.Alert) error {
	// Each Alert gets a full OCF framing wrapper with no compression. This is
	// wildly inefficient, but it's what has been done historically, and it's very
	// simple.
	buf := bytes.NewBuffer(nil)
	aw, err := schema.NewAlertWriter(buf, container.Null, 64)
	if err != nil {
		return fmt.Errorf("making an alert writer: %w", err)
	}
	err = aw.WriteRecord(a)
	if err != nil {
		return fmt.Errorf("writing alert: %w", err)
	}
	err = aw.Flush()
	if err != nil {
		return fmt.Errorf("flushing alert write: %w", err)
	}

	h := &tar.Header{
		Name:     strconv.FormatInt(a.Candid, 10) + ".avro",
		Size:     int64(buf.Len()),
		ModTime:  time.Now(),
		Mode:     0x744,
		Typeflag: tar.TypeReg,
		Uid:      0,
		Gid:      0,
		Uname:    "root",
		Gname:    "root",
	}

	err = w.WriteHeader(h)
	if err != nil {
		return err
	}

	_, err = w.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}
