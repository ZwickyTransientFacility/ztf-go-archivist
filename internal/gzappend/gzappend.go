package gzappend

import (
	"compress/gzip"
	"os"
)

// GzipAppender wraps a gzip.Writer with special behavior around closing the
// appender.
//
// This is used to wrap gzipped tarballs of alert data which are already on
// disk. We open these files in order to append in case there is any new data
// that we missed. But we don't want to spuriously add bytes to the end of these
// files, since that would change their MD5 checksum for no good reason. In
// order to do that, we need to be careful about when we call
// gzip.Writer.Close(), since that call will add a gzip block header to the
// underlying file, even though there's no data in the gzip black.
//
// GzipAppender passes all Write calls through to the gzip.Writer which it
// wraps.
//
// When GzipAppender.Close() is called, it only passes the call through to the
// gzip.Writer if it has received a non-empty Write call.
type GzipAppender struct {
	anyWrites bool
	gzw       *gzip.Writer
}

func NewGzipAppender(f *os.File) *GzipAppender {
	gzWriter, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		panic(err) // This can only happen if we provide an invalid compression level
	}
	return &GzipAppender{
		anyWrites: false,
		gzw:       gzWriter,
	}
}

func (a *GzipAppender) Write(data []byte) (int, error) {
	if len(data) > 0 {
		a.anyWrites = true
	}
	return a.gzw.Write(data)
}

func (a *GzipAppender) Close() error {
	if !a.anyWrites {
		return nil
	}
	return a.gzw.Close()
}
