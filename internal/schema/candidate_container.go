// Code generated by github.com/actgardner/gogen-avro. DO NOT EDIT.
/*
 * SOURCES:
 *     alert.avsc
 *     candidate.avsc
 *     cutout.avsc
 *     prv_candidate.avsc
 */
package schema

import (
	"io"

	"github.com/actgardner/gogen-avro/container"
	"github.com/actgardner/gogen-avro/vm"
	"github.com/actgardner/gogen-avro/compiler"
)

func NewCandidateWriter(writer io.Writer, codec container.Codec, recordsPerBlock int64) (*container.Writer, error) {
	str := NewCandidate()
	return container.NewWriter(writer, codec, recordsPerBlock, str.Schema())
}

// container reader
type CandidateReader struct {
	r io.Reader
	p *vm.Program
}

func NewCandidateReader(r io.Reader) (*CandidateReader, error){
	containerReader, err := container.NewReader(r)
	if err != nil {
		return nil, err
	}

	t := NewCandidate()
	deser, err := compiler.CompileSchemaBytes([]byte(containerReader.AvroContainerSchema()), []byte(t.Schema()))
	if err != nil {
		return nil, err
	}

	return &CandidateReader {
		r: containerReader,
		p: deser,
	}, nil
}

func (r CandidateReader) Read() (*Candidate, error) {
	t := NewCandidate()
        err := vm.Eval(r.r, r.p, t)
	return t, err
}
