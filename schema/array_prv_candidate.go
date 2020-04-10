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

	"github.com/actgardner/gogen-avro/vm/types"
	"github.com/actgardner/gogen-avro/vm"
)

func writeArrayPrv_candidate(r []*Prv_candidate, w io.Writer) error {
	err := vm.WriteLong(int64(len(r)),w)
	if err != nil || len(r) == 0 {
		return err
	}
	for _, e := range r {
		err = writePrv_candidate(e, w)
		if err != nil {
			return err
		}
	}
	return vm.WriteLong(0,w)
}



type ArrayPrv_candidateWrapper []*Prv_candidate

func (_ *ArrayPrv_candidateWrapper) SetBoolean(v bool) { panic("Unsupported operation") }
func (_ *ArrayPrv_candidateWrapper) SetInt(v int32) { panic("Unsupported operation") }
func (_ *ArrayPrv_candidateWrapper) SetLong(v int64) { panic("Unsupported operation") }
func (_ *ArrayPrv_candidateWrapper) SetFloat(v float32) { panic("Unsupported operation") }
func (_ *ArrayPrv_candidateWrapper) SetDouble(v float64) { panic("Unsupported operation") }
func (_ *ArrayPrv_candidateWrapper) SetBytes(v []byte) { panic("Unsupported operation") }
func (_ *ArrayPrv_candidateWrapper) SetString(v string) { panic("Unsupported operation") }
func (_ *ArrayPrv_candidateWrapper) SetUnionElem(v int64) { panic("Unsupported operation") }
func (_ *ArrayPrv_candidateWrapper) Get(i int) types.Field { panic("Unsupported operation") }
func (_ *ArrayPrv_candidateWrapper) AppendMap(key string) types.Field { panic("Unsupported operation") }
func (_ *ArrayPrv_candidateWrapper) Finalize() { }
func (_ *ArrayPrv_candidateWrapper) SetDefault(i int) { panic("Unsupported operation") }
func (r *ArrayPrv_candidateWrapper) AppendArray() types.Field {
	var v *Prv_candidate
	
	v = NewPrv_candidate()

 	
	*r = append(*r, v)
        
        return (*r)[len(*r)-1]
        
}