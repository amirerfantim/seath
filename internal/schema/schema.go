package schema

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ColumnDef struct {
	Field   string `yaml:"field" json:"field"`
	Column  string `yaml:"column" json:"column"`
	Type    string `yaml:"type" json:"type"`
	Include bool   `yaml:"include" json:"include"`
	Index   bool   `yaml:"index" json:"index"`
}

func Introspect(msg proto.Message) []ColumnDef {
	fields := msg.ProtoReflect().Descriptor().Fields()
	cols := make([]ColumnDef, 0, fields.Len())
	for i := range fields.Len() {
		fd := fields.Get(i)
		cols = append(cols, ColumnDef{
			Field:   string(fd.Name()),
			Column:  string(fd.Name()),
			Type:    pgType(fd),
			Include: true,
		})
	}
	return cols
}

func pgType(fd protoreflect.FieldDescriptor) string {
	if fd.IsList() || fd.IsMap() {
		return "JSONB"
	}
	switch fd.Kind() {
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		return "DOUBLE PRECISION"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return "INTEGER"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return "BIGINT"
	case protoreflect.BoolKind:
		return "BOOLEAN"
	case protoreflect.StringKind, protoreflect.BytesKind:
		return "TEXT"
	case protoreflect.EnumKind:
		return "INTEGER"
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if fd.Message().FullName() == "google.protobuf.Timestamp" {
			return "TIMESTAMPTZ"
		}
		return "JSONB"
	default:
		return "TEXT"
	}
}

// ExtractValues returns one value per ColumnDef, in order. Returns nil for unset message fields.
// Proto3 scalar defaults (false, 0, "") are inserted as-is (not NULL).
func ExtractValues(msg proto.Message, cols []ColumnDef) []interface{} {
	refl := msg.ProtoReflect()
	fds := refl.Descriptor().Fields()
	vals := make([]interface{}, len(cols))
	for i, col := range cols {
		fd := fds.ByName(protoreflect.Name(col.Field))
		if fd == nil {
			vals[i] = nil
			continue
		}
		vals[i] = extractValue(refl, fd)
	}
	return vals
}

func extractValue(refl protoreflect.Message, fd protoreflect.FieldDescriptor) interface{} {
	// Lists and maps always → JSONB string
	if fd.IsList() || fd.IsMap() {
		return marshalField(refl, fd)
	}

	// Nested messages: NULL when absent
	if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
		if !refl.Has(fd) {
			return nil
		}
		sub := refl.Get(fd).Message()
		if fd.Message().FullName() == "google.protobuf.Timestamp" {
			return timestampValue(sub)
		}
		b, err := protojson.Marshal(sub.Interface())
		if err != nil {
			return nil
		}
		return string(b)
	}

	// Scalars: always return value (proto3 default = valid value)
	v := refl.Get(fd)
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return v.Bool()
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return int32(v.Int())
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return int32(v.Uint())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return v.Int()
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return int64(v.Uint())
	case protoreflect.FloatKind:
		return float64(v.Float())
	case protoreflect.DoubleKind:
		return v.Float()
	case protoreflect.StringKind:
		return v.String()
	case protoreflect.BytesKind:
		return string(v.Bytes())
	case protoreflect.EnumKind:
		return int32(v.Enum())
	default:
		return nil
	}
}

// timestampValue reads seconds/nanos reflectively: recordings decode through
// dynamicpb, so the value is never a *timestamppb.Timestamp. Out-of-range
// timestamps become NULL rather than failing the whole batch.
func timestampValue(m protoreflect.Message) interface{} {
	fields := m.Descriptor().Fields()
	seconds, nanos := fields.ByNumber(1), fields.ByNumber(2)
	if seconds == nil || nanos == nil {
		return nil
	}
	ts := &timestamppb.Timestamp{
		Seconds: m.Get(seconds).Int(),
		Nanos:   int32(m.Get(nanos).Int()),
	}
	if !ts.IsValid() {
		return nil
	}
	return ts.AsTime()
}

func marshalField(refl protoreflect.Message, fd protoreflect.FieldDescriptor) interface{} {
	if !refl.Has(fd) {
		return nil
	}
	v := refl.Get(fd)
	if fd.IsList() {
		list := v.List()
		items := make([]interface{}, list.Len())
		for i := range list.Len() {
			item := list.Get(i)
			switch fd.Kind() {
			case protoreflect.MessageKind:
				b, _ := protojson.Marshal(item.Message().Interface())
				items[i] = json.RawMessage(b)
			default:
				items[i] = item.Interface()
			}
		}
		b, _ := json.Marshal(items)
		return string(b)
	}
	if fd.IsMap() {
		m := make(map[string]interface{})
		v.Map().Range(func(k protoreflect.MapKey, val protoreflect.Value) bool {
			m[k.String()] = val.Interface()
			return true
		})
		b, _ := json.Marshal(m)
		return string(b)
	}
	return nil
}
