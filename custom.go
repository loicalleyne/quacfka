package quacfka

import (
	bufa "github.com/loicalleyne/bufarrow"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Cardinality determines whether a field is optional, required, or repeated.
type Cardinality protoreflect.Cardinality

// Constants as defined by the google.protobuf.Cardinality enumeration.
const (
	Optional Cardinality = 1 // appears zero or one times
	Required Cardinality = 2 // appears exactly one time; invalid with Proto3
	Repeated Cardinality = 3 // appears zero or more times
)

func (c *Cardinality) Get() bufa.Cardinality {
	switch *c {
	case Optional:
		return bufa.Optional
	case Required:
		return bufa.Required
	case Repeated:
		return bufa.Repeated
	}
	return bufa.Optional
}

type FieldType fieldType
type fieldType string

const (
	BOOL    FieldType = "bool"
	BYTES   FieldType = "[]byte"
	STRING  FieldType = "string"
	INT64   FieldType = "int64"
	FLOAT64 FieldType = "float64"
)

func (t *FieldType) Get() bufa.FieldType {
	switch *t {
	case BOOL:
		return bufa.BOOL
	case BYTES:
		return bufa.BYTES
	case STRING:
		return bufa.STRING
	case INT64:
		return bufa.INT64
	case FLOAT64:
		return bufa.FLOAT64
	}
	return ""
}

type CustomField struct {
	Name             string
	Type             FieldType
	FieldCardinality Cardinality
	IsPacked         bool
}

func WithCustomFields(p []CustomField) Option {
	return func(cfg config) {
		for _, c := range p {
			var b bufa.CustomField
			b.Name = c.Name
			b.Type = c.Type.Get()
			b.FieldCardinality = c.FieldCardinality.Get()
			b.IsPacked = c.IsPacked
			cfg.customFields = append(cfg.customFields, b)
		}
	}
}
