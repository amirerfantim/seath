package recorder

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Legacy sampling modes remain readable so an in-place upgrade preserves every
// existing recording. New rules use ModeComposite whenever they have filters.
const (
	ModeRate       = "rate"
	ModeFieldRatio = "field_ratio"
	ModeFieldValue = "field_value"
	ModeComposite  = "composite"

	FilterExact        = "exact"
	FilterModuloRange  = "modulo_range"
	FilterFloatCompare = "float_compare"
)

const (
	MaxSampleValues  = 100
	MaxSampleFilters = 20
)

// SamplingField describes one top-level singular scalar the UI may filter.
// Capability flags keep the browser builder aligned with server validation.
type SamplingField struct {
	Field           string           `json:"field"`
	Kind            string           `json:"kind"`
	ProtoKind       string           `json:"proto_kind"`
	SupportsRatio   bool             `json:"supports_ratio"` // legacy UI contract
	SupportsExact   bool             `json:"supports_exact"`
	SupportsModulo  bool             `json:"supports_modulo"`
	SupportsCompare bool             `json:"supports_compare"`
	Options         []SamplingOption `json:"options,omitempty"`
}

type SamplingOption struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

func SamplingFields(tmpl proto.Message) []SamplingField {
	if tmpl == nil {
		return nil
	}
	fields := tmpl.ProtoReflect().Descriptor().Fields()
	out := make([]SamplingField, 0, fields.Len())
	for i := range fields.Len() {
		fd := fields.Get(i)
		kind, ok := samplingKind(fd)
		if !ok {
			continue
		}
		integer := kind == "integer"
		field := SamplingField{
			Field:           string(fd.Name()),
			Kind:            kind,
			ProtoKind:       fd.Kind().String(),
			SupportsRatio:   integer,
			SupportsExact:   kind != "float",
			SupportsModulo:  integer,
			SupportsCompare: kind == "float",
		}
		if fd.Kind() == protoreflect.EnumKind {
			values := fd.Enum().Values()
			field.Options = make([]SamplingOption, 0, values.Len())
			for j := range values.Len() {
				value := values.Get(j)
				name := string(value.Name())
				field.Options = append(field.Options, SamplingOption{Value: name, Label: name})
			}
		}
		out = append(out, field)
	}
	return out
}

func samplingKind(fd protoreflect.FieldDescriptor) (string, bool) {
	if fd.IsList() || fd.IsMap() {
		return "", false
	}
	switch fd.Kind() {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return "integer", true
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return "float", true
	case protoreflect.BoolKind:
		return "boolean", true
	case protoreflect.StringKind:
		return "string", true
	case protoreflect.EnumKind:
		return "enum", true
	default:
		return "", false
	}
}

// NormalizeSampling converts legacy exclusive rules to the composite model.
// It is descriptor-independent; newGate performs the type-aware validation.
func NormalizeSampling(s Sampling) (Sampling, error) {
	mode := s.Mode
	if mode == "" {
		mode = ModeRate
	}
	if len(s.Filters) > 0 {
		if mode != ModeRate && mode != ModeComposite {
			return Sampling{}, fmt.Errorf("sample_filters cannot be combined with legacy sample_mode %q", mode)
		}
		return Sampling{Mode: ModeComposite, Rate: s.Rate, Filters: cloneFilters(s.Filters)}, nil
	}

	switch mode {
	case ModeRate, ModeComposite:
		return Sampling{Mode: ModeRate, Rate: s.Rate, Filters: []SamplingFilter{}}, nil
	case ModeFieldRatio:
		return Sampling{
			Mode: ModeComposite,
			Rate: 100,
			Filters: []SamplingFilter{{
				Type: FilterModuloRange, Field: s.Field, Lower: intPtr(0), Upper: intPtr(s.Rate - 1),
			}},
		}, nil
	case ModeFieldValue:
		return Sampling{
			Mode: ModeComposite,
			Rate: 100,
			Filters: []SamplingFilter{{
				Type: FilterExact, Field: s.Field, Values: append([]string(nil), s.Values...),
			}},
		}, nil
	default:
		return Sampling{}, fmt.Errorf("unknown sample_mode %q", mode)
	}
}

func cloneFilters(filters []SamplingFilter) []SamplingFilter {
	out := make([]SamplingFilter, len(filters))
	for i, filter := range filters {
		out[i] = filter
		out[i].Values = append([]string(nil), filter.Values...)
		if filter.Lower != nil {
			lower := *filter.Lower
			out[i].Lower = &lower
		}
		if filter.Upper != nil {
			upper := *filter.Upper
			out[i].Upper = &upper
		}
		if filter.Value != nil {
			value := *filter.Value
			out[i].Value = &value
		}
	}
	return out
}

type scalarKey struct {
	kind     protoreflect.Kind
	signed   int64
	unsigned uint64
	boolean  bool
	text     string
	enum     protoreflect.EnumNumber
}

type compiledFilter struct {
	typeName  string
	fd        protoreflect.FieldDescriptor
	values    map[scalarKey]struct{}
	labels    []string
	lower     int
	upper     int
	operator  string
	value     float64
	valueText string
}

// gate is immutable and swapped atomically by Recorder. Percentage sampling is
// checked before decoding; payload filters are then evaluated in order and all
// must match.
type gate struct {
	rate    int64
	filters []compiledFilter
}

func newGate(spec JobSpec, tmpl proto.Message) (*gate, error) {
	sampling, err := NormalizeSampling(spec.Sampling())
	if err != nil {
		return nil, err
	}
	if sampling.Rate < 1 || sampling.Rate > 100 {
		return nil, fmt.Errorf("sample_rate must be 1-100, got %d", sampling.Rate)
	}
	if len(sampling.Filters) > MaxSampleFilters {
		return nil, fmt.Errorf("at most %d filters allowed, got %d", MaxSampleFilters, len(sampling.Filters))
	}
	if len(sampling.Filters) > 0 && tmpl == nil {
		return nil, fmt.Errorf("sample_filters need a proto template")
	}

	g := &gate{rate: int64(sampling.Rate), filters: make([]compiledFilter, 0, len(sampling.Filters))}
	totalValues := 0
	for i, filter := range sampling.Filters {
		compiled, valueCount, err := compileFilter(tmpl, filter)
		if err != nil {
			return nil, fmt.Errorf("filter %d: %w", i+1, err)
		}
		totalValues += valueCount
		if totalValues > MaxSampleValues {
			return nil, fmt.Errorf("at most %d exact values allowed across all filters", MaxSampleValues)
		}
		g.filters = append(g.filters, compiled)
	}
	return g, nil
}

func compileFilter(tmpl proto.Message, filter SamplingFilter) (compiledFilter, int, error) {
	fd, err := resolveSamplingField(tmpl, filter.Field)
	if err != nil {
		return compiledFilter{}, 0, err
	}
	kind, supported := samplingKind(fd)
	if !supported {
		return compiledFilter{}, 0, fmt.Errorf("field %q is %s and cannot be filtered", fd.Name(), fd.Kind())
	}

	switch filter.Type {
	case FilterExact:
		if kind == "float" {
			return compiledFilter{}, 0, fmt.Errorf("field %q is %s; use a numeric comparison", fd.Name(), fd.Kind())
		}
		if len(filter.Values) == 0 {
			return compiledFilter{}, 0, fmt.Errorf("exact filter on %q requires at least one value", fd.Name())
		}
		values := make(map[scalarKey]struct{}, len(filter.Values))
		labels := make([]string, 0, len(filter.Values))
		for _, raw := range filter.Values {
			key, label, err := parseScalar(fd, raw)
			if err != nil {
				return compiledFilter{}, 0, fmt.Errorf("value %q for field %q: %w", raw, fd.Name(), err)
			}
			if _, exists := values[key]; exists {
				continue
			}
			values[key] = struct{}{}
			labels = append(labels, label)
		}
		return compiledFilter{typeName: filter.Type, fd: fd, values: values, labels: labels}, len(values), nil

	case FilterModuloRange:
		if kind != "integer" {
			return compiledFilter{}, 0, fmt.Errorf("field %q is %s; modulo range needs an integer field", fd.Name(), fd.Kind())
		}
		if filter.Lower == nil || filter.Upper == nil {
			return compiledFilter{}, 0, fmt.Errorf("modulo range on %q requires lower and upper bounds", fd.Name())
		}
		if *filter.Lower < 0 || *filter.Lower > 99 || *filter.Upper < 0 || *filter.Upper > 99 {
			return compiledFilter{}, 0, fmt.Errorf("modulo bounds must be between 0 and 99")
		}
		if *filter.Lower > *filter.Upper {
			return compiledFilter{}, 0, fmt.Errorf("modulo lower bound %d must not exceed upper bound %d", *filter.Lower, *filter.Upper)
		}
		return compiledFilter{typeName: filter.Type, fd: fd, lower: *filter.Lower, upper: *filter.Upper}, 0, nil

	case FilterFloatCompare:
		if kind != "float" {
			return compiledFilter{}, 0, fmt.Errorf("field %q is %s; numeric comparison needs a float or double field", fd.Name(), fd.Kind())
		}
		if filter.Value == nil || math.IsNaN(*filter.Value) || math.IsInf(*filter.Value, 0) {
			return compiledFilter{}, 0, fmt.Errorf("numeric comparison on %q requires a finite value", fd.Name())
		}
		switch filter.Operator {
		case ">", ">=", "<", "<=", "=", "!=":
		default:
			return compiledFilter{}, 0, fmt.Errorf("unsupported numeric operator %q", filter.Operator)
		}
		return compiledFilter{
			typeName: filter.Type, fd: fd, operator: filter.Operator, value: *filter.Value,
			valueText: strconv.FormatFloat(*filter.Value, 'g', -1, 64),
		}, 0, nil

	default:
		return compiledFilter{}, 0, fmt.Errorf("unknown filter type %q", filter.Type)
	}
}

func intPtr(value int) *int { return &value }

func ValidateSampling(spec JobSpec, tmpl proto.Message) error {
	_, err := newGate(spec, tmpl)
	return err
}

func resolveSamplingField(tmpl proto.Message, name string) (protoreflect.FieldDescriptor, error) {
	if name == "" {
		return nil, fmt.Errorf("field is required")
	}
	fd := tmpl.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return nil, fmt.Errorf("field %q does not exist on %s", name, tmpl.ProtoReflect().Descriptor().FullName())
	}
	if fd.IsList() || fd.IsMap() {
		return nil, fmt.Errorf("field %q is repeated; filters need a singular scalar field", name)
	}
	return fd, nil
}

func parseScalar(fd protoreflect.FieldDescriptor, raw string) (scalarKey, string, error) {
	k := scalarKey{kind: fd.Kind()}
	trimmed := strings.TrimSpace(raw)
	switch fd.Kind() {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		v, err := strconv.ParseInt(trimmed, 10, 32)
		if err != nil {
			return scalarKey{}, "", fmt.Errorf("expected a 32-bit integer")
		}
		k.signed = v
		return k, strconv.FormatInt(v, 10), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		v, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return scalarKey{}, "", fmt.Errorf("expected a 64-bit integer")
		}
		k.signed = v
		return k, strconv.FormatInt(v, 10), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		v, err := strconv.ParseUint(trimmed, 10, 32)
		if err != nil {
			return scalarKey{}, "", fmt.Errorf("expected an unsigned 32-bit integer")
		}
		k.unsigned = v
		return k, strconv.FormatUint(v, 10), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		v, err := strconv.ParseUint(trimmed, 10, 64)
		if err != nil {
			return scalarKey{}, "", fmt.Errorf("expected an unsigned 64-bit integer")
		}
		k.unsigned = v
		return k, strconv.FormatUint(v, 10), nil
	case protoreflect.BoolKind:
		if trimmed != "true" && trimmed != "false" {
			return scalarKey{}, "", fmt.Errorf("expected true or false")
		}
		k.boolean = trimmed == "true"
		return k, trimmed, nil
	case protoreflect.StringKind:
		k.text = raw
		return k, strconv.Quote(raw), nil
	case protoreflect.EnumKind:
		value := fd.Enum().Values().ByName(protoreflect.Name(trimmed))
		if value == nil {
			n, err := strconv.ParseInt(trimmed, 10, 32)
			if err == nil {
				value = fd.Enum().Values().ByNumber(protoreflect.EnumNumber(n))
			}
		}
		if value == nil {
			return scalarKey{}, "", fmt.Errorf("expected a declared %s enum value", fd.Enum().Name())
		}
		k.enum = value.Number()
		return k, string(value.Name()), nil
	default:
		return scalarKey{}, "", fmt.Errorf("unsupported scalar type %s", fd.Kind())
	}
}

func (g *gate) preDecode() bool { return len(g.filters) == 0 }

func (g *gate) keepCount(n uint64) bool {
	return (n-1)%100 < uint64(g.rate)
}

func (g *gate) keepField(msg proto.Message) bool {
	refl := msg.ProtoReflect()
	for _, filter := range g.filters {
		switch filter.typeName {
		case FilterExact:
			if _, ok := filter.values[fieldScalar(refl, filter.fd)]; !ok {
				return false
			}
		case FilterModuloRange:
			remainder := fieldRemainderValue(refl, filter.fd)
			if remainder < filter.lower || remainder > filter.upper {
				return false
			}
		case FilterFloatCompare:
			if !compareFloat(refl.Get(filter.fd).Float(), filter.operator, filter.value) {
				return false
			}
		}
	}
	return true
}

func fieldRemainderValue(refl protoreflect.Message, fd protoreflect.FieldDescriptor) int {
	v := refl.Get(fd)
	switch fd.Kind() {
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return int(v.Uint() % 100)
	default:
		return int(((v.Int() % 100) + 100) % 100)
	}
}

func compareFloat(actual float64, operator string, expected float64) bool {
	switch operator {
	case ">":
		return actual > expected
	case ">=":
		return actual >= expected
	case "<":
		return actual < expected
	case "<=":
		return actual <= expected
	case "=":
		return actual == expected
	case "!=":
		return actual != expected
	default:
		return false
	}
}

func fieldScalar(refl protoreflect.Message, fd protoreflect.FieldDescriptor) scalarKey {
	v := refl.Get(fd)
	k := scalarKey{kind: fd.Kind()}
	switch fd.Kind() {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		k.signed = v.Int()
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		k.unsigned = v.Uint()
	case protoreflect.BoolKind:
		k.boolean = v.Bool()
	case protoreflect.StringKind:
		k.text = v.String()
	case protoreflect.EnumKind:
		k.enum = v.Enum()
	}
	return k
}

func (g *gate) describe() string {
	parts := make([]string, 0, len(g.filters)+1)
	if g.rate < 100 || len(g.filters) == 0 {
		parts = append(parts, fmt.Sprintf("%d%%", g.rate))
	}
	for _, filter := range g.filters {
		switch filter.typeName {
		case FilterExact:
			if len(filter.labels) == 1 {
				parts = append(parts, fmt.Sprintf("%s = %s", filter.fd.Name(), filter.labels[0]))
			} else {
				parts = append(parts, fmt.Sprintf("%s in %d values", filter.fd.Name(), len(filter.labels)))
			}
		case FilterModuloRange:
			parts = append(parts, fmt.Sprintf("%s%%100 %d–%d", filter.fd.Name(), filter.lower, filter.upper))
		case FilterFloatCompare:
			parts = append(parts, fmt.Sprintf("%s %s %s", filter.fd.Name(), filter.operator, filter.valueText))
		}
	}
	return strings.Join(parts, " · ")
}

func (g *gate) summary() string {
	count := len(g.filters)
	if count == 0 {
		return fmt.Sprintf("%d%%", g.rate)
	}
	filters := fmt.Sprintf("%d filters", count)
	if count == 1 {
		filters = "1 filter"
	}
	if g.rate == 100 {
		return filters
	}
	return fmt.Sprintf("%d%% · %s", g.rate, filters)
}
