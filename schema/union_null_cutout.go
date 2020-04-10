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
	"fmt"

	"github.com/actgardner/gogen-avro/vm"
	"github.com/actgardner/gogen-avro/vm/types"
)


type UnionNullCutoutTypeEnum int
const (

	 UnionNullCutoutTypeEnumNull UnionNullCutoutTypeEnum = 0

	 UnionNullCutoutTypeEnumCutout UnionNullCutoutTypeEnum = 1

)

type UnionNullCutout struct {

	Null *types.NullVal

	Cutout *Cutout

	UnionType UnionNullCutoutTypeEnum
}

func writeUnionNullCutout(r *UnionNullCutout, w io.Writer) error {
	err := vm.WriteLong(int64(r.UnionType), w)
	if err != nil {
		return err
	}
	switch r.UnionType{
	
	case UnionNullCutoutTypeEnumNull:
		return vm.WriteNull(r.Null, w)
        
	case UnionNullCutoutTypeEnumCutout:
		return writeCutout(r.Cutout, w)
        
	}
	return fmt.Errorf("invalid value for *UnionNullCutout")
}

func NewUnionNullCutout() *UnionNullCutout {
	return &UnionNullCutout{}
}

func (_ *UnionNullCutout) SetBoolean(v bool) { panic("Unsupported operation") }
func (_ *UnionNullCutout) SetInt(v int32) { panic("Unsupported operation") }
func (_ *UnionNullCutout) SetFloat(v float32) { panic("Unsupported operation") }
func (_ *UnionNullCutout) SetDouble(v float64) { panic("Unsupported operation") }
func (_ *UnionNullCutout) SetBytes(v []byte) { panic("Unsupported operation") }
func (_ *UnionNullCutout) SetString(v string) { panic("Unsupported operation") }
func (r *UnionNullCutout) SetLong(v int64) { 
	r.UnionType = (UnionNullCutoutTypeEnum)(v)
}
func (r *UnionNullCutout) Get(i int) types.Field {
	switch (i) {
	
	case 0:
		
		
		return r.Null
		
	
	case 1:
		
		r.Cutout = NewCutout()
		
		
		return r.Cutout
		
	
	}
	panic("Unknown field index")
}
func (_ *UnionNullCutout) SetDefault(i int) { panic("Unsupported operation") }
func (_ *UnionNullCutout) AppendMap(key string) types.Field { panic("Unsupported operation") }
func (_ *UnionNullCutout) AppendArray() types.Field { panic("Unsupported operation") }
func (_ *UnionNullCutout) Finalize()  { }
