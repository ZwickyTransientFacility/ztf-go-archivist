package main

import (
	"archive/tar"
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/ZwickyTransientFacility/ztf-go-archivist/schema"
	"github.com/actgardner/gogen-avro/container"
)

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
