package recorder

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"seath/internal/pg"
	"seath/internal/schema"
)

func scalarTemplate(t *testing.T) proto.Message {
	t.Helper()
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	repeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	field := func(name string, number int32, kind descriptorpb.FieldDescriptorProto_Type, label *descriptorpb.FieldDescriptorProto_Label, typeName string) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(number), Type: &kind, Label: label, TypeName: proto.String(typeName)}
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("sampling_test.proto"),
		Package: proto.String("samplingtest"),
		Syntax:  proto.String("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("ServiceType"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("NONE"), Number: proto.Int32(0)},
				{Name: proto.String("PRIMARY"), Number: proto.Int32(1)},
				{Name: proto.String("SECONDARY"), Number: proto.Int32(5)},
			},
		}},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Event"),
			Field: []*descriptorpb.FieldDescriptorProto{
				field("signed_id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, &optional, ""),
				field("unsigned_id", 2, descriptorpb.FieldDescriptorProto_TYPE_UINT64, &optional, ""),
				field("is_enabled", 3, descriptorpb.FieldDescriptorProto_TYPE_BOOL, &optional, ""),
				field("service_type", 4, descriptorpb.FieldDescriptorProto_TYPE_ENUM, &optional, ".samplingtest.ServiceType"),
				field("label", 5, descriptorpb.FieldDescriptorProto_TYPE_STRING, &optional, ""),
				field("score", 6, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, &optional, ""),
				field("payload", 7, descriptorpb.FieldDescriptorProto_TYPE_BYTES, &optional, ""),
				field("tags", 8, descriptorpb.FieldDescriptorProto_TYPE_STRING, &repeated, ""),
			},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return dynamicpb.NewMessage(file.Messages().ByName("Event"))
}

func event(t *testing.T, tmpl proto.Message, fields map[string]any) proto.Message {
	t.Helper()
	msg := tmpl.ProtoReflect().New()
	for name, raw := range fields {
		fd := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
		if fd == nil {
			t.Fatalf("unknown test field %q", name)
		}
		var value protoreflect.Value
		switch v := raw.(type) {
		case int64:
			value = protoreflect.ValueOfInt64(v)
		case uint64:
			value = protoreflect.ValueOfUint64(v)
		case bool:
			value = protoreflect.ValueOfBool(v)
		case string:
			value = protoreflect.ValueOfString(v)
		case float64:
			value = protoreflect.ValueOfFloat64(v)
		case protoreflect.EnumNumber:
			value = protoreflect.ValueOfEnum(v)
		default:
			t.Fatalf("unsupported test value %T", raw)
		}
		msg.Set(fd, value)
	}
	return msg.Interface()
}

func mustGate(t *testing.T, spec JobSpec, template ...proto.Message) *gate {
	t.Helper()
	msg := scalarTemplate(t)
	if len(template) > 0 {
		msg = template[0]
	}
	g, err := newGate(spec, msg)
	if err != nil {
		t.Fatalf("newGate(%+v): %v", spec, err)
	}
	return g
}

func TestNewGateRejectsBadConfig(t *testing.T) {
	scalars := scalarTemplate(t)
	tests := []struct {
		name string
		spec JobSpec
		tmpl proto.Message
	}{
		{"rate below range", JobSpec{SampleMode: ModeRate, SampleRate: 0}, scalars},
		{"rate above range", JobSpec{SampleMode: ModeRate, SampleRate: 101}, scalars},
		{"unknown mode", JobSpec{SampleMode: "sometimes", SampleRate: 10}, scalars},
		{"ratio without field", JobSpec{SampleMode: ModeFieldRatio, SampleRate: 10}, scalars},
		{"ratio on missing field", JobSpec{SampleMode: ModeFieldRatio, SampleField: "nope", SampleRate: 10}, scalars},
		{"ratio on float", JobSpec{SampleMode: ModeFieldRatio, SampleField: "score", SampleRate: 10}, scalars},
		{"ratio out of range", JobSpec{SampleMode: ModeFieldRatio, SampleField: "signed_id", SampleRate: 0}, scalars},
		{"value without values", JobSpec{SampleMode: ModeFieldValue, SampleField: "signed_id"}, scalars},
		{"value with too many values", JobSpec{SampleMode: ModeFieldValue, SampleField: "signed_id", SampleValues: make([]string, MaxSampleValues+1)}, scalars},
		{"value on float", JobSpec{SampleMode: ModeFieldValue, SampleField: "score", SampleValues: []string{"1.2"}}, scalars},
		{"value on bytes", JobSpec{SampleMode: ModeFieldValue, SampleField: "payload", SampleValues: []string{"x"}}, scalars},
		{"value on repeated", JobSpec{SampleMode: ModeFieldValue, SampleField: "tags", SampleValues: []string{"x"}}, scalars},
		{"bad signed integer", JobSpec{SampleMode: ModeFieldValue, SampleField: "signed_id", SampleValues: []string{"nope"}}, scalars},
		{"negative unsigned integer", JobSpec{SampleMode: ModeFieldValue, SampleField: "unsigned_id", SampleValues: []string{"-1"}}, scalars},
		{"bad boolean", JobSpec{SampleMode: ModeFieldValue, SampleField: "is_enabled", SampleValues: []string{"yes"}}, scalars},
		{"unknown enum", JobSpec{SampleMode: ModeFieldValue, SampleField: "service_type", SampleValues: []string{"UNKNOWN_VALUE"}}, scalars},
		{"too many filters", JobSpec{SampleRate: 100, SampleFilters: make([]SamplingFilter, MaxSampleFilters+1)}, scalars},
		{"exact float", JobSpec{SampleRate: 100, SampleFilters: []SamplingFilter{{Type: FilterExact, Field: "score", Values: []string{"1.2"}}}}, scalars},
		{"modulo on float", JobSpec{SampleRate: 100, SampleFilters: []SamplingFilter{{Type: FilterModuloRange, Field: "score", Lower: intPtr(1), Upper: intPtr(2)}}}, scalars},
		{"modulo reversed", JobSpec{SampleRate: 100, SampleFilters: []SamplingFilter{{Type: FilterModuloRange, Field: "signed_id", Lower: intPtr(65), Upper: intPtr(45)}}}, scalars},
		{"modulo outside range", JobSpec{SampleRate: 100, SampleFilters: []SamplingFilter{{Type: FilterModuloRange, Field: "signed_id", Lower: intPtr(0), Upper: intPtr(100)}}}, scalars},
		{"compare on integer", JobSpec{SampleRate: 100, SampleFilters: []SamplingFilter{{Type: FilterFloatCompare, Field: "signed_id", Operator: ">", Value: floatPtr(1)}}}, scalars},
		{"compare missing value", JobSpec{SampleRate: 100, SampleFilters: []SamplingFilter{{Type: FilterFloatCompare, Field: "score", Operator: ">"}}}, scalars},
		{"compare bad operator", JobSpec{SampleRate: 100, SampleFilters: []SamplingFilter{{Type: FilterFloatCompare, Field: "score", Operator: "~", Value: floatPtr(1)}}}, scalars},
		{"legacy and composite conflict", JobSpec{SampleMode: ModeFieldValue, SampleRate: 100, SampleField: "label", SampleValues: []string{"x"}, SampleFilters: []SamplingFilter{{Type: FilterExact, Field: "label", Values: []string{"x"}}}}, scalars},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newGate(tc.spec, tc.tmpl); err == nil {
				t.Fatalf("newGate(%+v) = nil error, want error", tc.spec)
			}
		})
	}
}

func TestSamplingFieldsMatchesGateCapabilities(t *testing.T) {
	fields := SamplingFields(scalarTemplate(t))
	got := make(map[string]SamplingField, len(fields))
	for _, field := range fields {
		got[field.Field] = field
	}
	for name, kind := range map[string]string{
		"signed_id": "integer", "unsigned_id": "integer", "is_enabled": "boolean",
		"service_type": "enum", "label": "string", "score": "float",
	} {
		if got[name].Kind != kind {
			t.Errorf("%s kind = %q, want %q", name, got[name].Kind, kind)
		}
	}
	for _, name := range []string{"payload", "tags"} {
		if _, ok := got[name]; ok {
			t.Errorf("unsupported field %s was exposed to the UI", name)
		}
	}
	if !got["signed_id"].SupportsRatio || got["is_enabled"].SupportsRatio {
		t.Error("ratio capability must be true only for integer fields")
	}
	if !got["signed_id"].SupportsModulo || !got["signed_id"].SupportsExact || got["signed_id"].SupportsCompare {
		t.Error("integer field capabilities are wrong")
	}
	if !got["score"].SupportsCompare || got["score"].SupportsExact || got["score"].SupportsModulo {
		t.Error("float field capabilities are wrong")
	}
	if options := got["service_type"].Options; len(options) != 3 || options[2].Value != "SECONDARY" {
		t.Fatalf("enum options = %+v, want NONE/PRIMARY/SECONDARY", options)
	}
}

// A job persisted before sampling modes existed defaults to percentage mode.
func TestNewGateDefaultsToRate(t *testing.T) {
	g := mustGate(t, JobSpec{SampleRate: 10})
	if g.rate != 10 || !g.preDecode() {
		t.Fatalf("legacy rule = rate %d preDecode %v, want 10/true", g.rate, g.preDecode())
	}
}

func TestGateSamplingSummary(t *testing.T) {
	template := scalarTemplate(t)
	tests := []struct {
		name string
		spec JobSpec
		want string
	}{
		{name: "percentage only", spec: JobSpec{SampleRate: 10}, want: "10%"},
		{name: "one filter at full traffic", spec: JobSpec{SampleRate: 100, SampleFilters: []SamplingFilter{{Type: FilterExact, Field: "is_enabled", Values: []string{"false"}}}}, want: "1 filter"},
		{name: "three filters at full traffic", spec: JobSpec{SampleRate: 100, SampleFilters: []SamplingFilter{{Type: FilterExact, Field: "is_enabled", Values: []string{"false"}}, {Type: FilterModuloRange, Field: "signed_id", Lower: intPtr(45), Upper: intPtr(65)}, {Type: FilterFloatCompare, Field: "score", Operator: ">", Value: floatPtr(0.8)}}}, want: "3 filters"},
		{name: "percentage and three filters", spec: JobSpec{SampleRate: 10, SampleFilters: []SamplingFilter{{Type: FilterExact, Field: "is_enabled", Values: []string{"false"}}, {Type: FilterModuloRange, Field: "signed_id", Lower: intPtr(45), Upper: intPtr(65)}, {Type: FilterFloatCompare, Field: "score", Operator: ">", Value: floatPtr(0.8)}}}, want: "10% · 3 filters"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := newGate(tt.spec, template)
			if err != nil {
				t.Fatal(err)
			}
			if got := g.summary(); got != tt.want {
				t.Fatalf("summary = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizeSamplingPreservesLegacyRules(t *testing.T) {
	ratio, err := NormalizeSampling(Sampling{Mode: ModeFieldRatio, Rate: 21, Field: "entity_id"})
	if err != nil {
		t.Fatal(err)
	}
	if ratio.Mode != ModeComposite || ratio.Rate != 100 || len(ratio.Filters) != 1 ||
		ratio.Filters[0].Type != FilterModuloRange || *ratio.Filters[0].Lower != 0 || *ratio.Filters[0].Upper != 20 {
		t.Fatalf("legacy ratio normalized incorrectly: %+v", ratio)
	}
	value, err := NormalizeSampling(Sampling{Mode: ModeFieldValue, Field: "is_enabled", Values: []string{"false"}})
	if err != nil {
		t.Fatal(err)
	}
	if value.Rate != 100 || len(value.Filters) != 1 || value.Filters[0].Type != FilterExact || value.Filters[0].Values[0] != "false" {
		t.Fatalf("legacy value rule normalized incorrectly: %+v", value)
	}
}

func TestNewResolvedDoesNotReenterResolver(t *testing.T) {
	called := false
	resolve := func(string) (proto.Message, error) {
		called = true
		return nil, fmt.Errorf("resolver must not be called")
	}
	rec := NewResolved(JobSpec{SampleMode: ModeRate, SampleRate: 10}, resolve, scalarTemplate(t))
	if called {
		t.Fatal("NewResolved called the resolver while the caller may hold its lock")
	}
	if got := rec.SampleDesc(); got != "10%" {
		t.Fatalf("sampling description = %q, want 10%%", got)
	}
}

func TestConcurrentStartGateRefreshCannotOverwriteLiveSampling(t *testing.T) {
	template := scalarTemplate(t)
	spec := JobSpec{SampleRate: 100, SampleFilters: []SamplingFilter{{Type: FilterExact, Field: "is_enabled", Values: []string{"false"}}}}
	rec := NewResolved(spec, nil, template)

	start := make(chan struct{})
	errs := make(chan error, 1000)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := range 500 {
			value := "false"
			if i%2 == 0 {
				value = "true"
			}
			if err := rec.SetSampling(Sampling{Rate: 100, Filters: []SamplingFilter{{Type: FilterExact, Field: "is_enabled", Values: []string{value}}}}, template); err != nil {
				errs <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for range 500 {
			if err := rec.installCurrentGate(template); err != nil {
				errs <- err
			}
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	final := rec.Spec().SampleFilters[0].Values[0] == "true"
	g := rec.gate.Load()
	if got := g.keepField(event(t, template, map[string]any{"is_enabled": final})); !got {
		t.Fatalf("installed gate does not match final spec value %v", final)
	}
	if got := g.keepField(event(t, template, map[string]any{"is_enabled": !final})); got {
		t.Fatalf("installed gate kept the value opposite final spec %v", final)
	}
}

func TestKeepCount(t *testing.T) {
	g := mustGate(t, JobSpec{SampleMode: ModeRate, SampleRate: 10})
	kept := 0
	for n := uint64(1); n <= 1000; n++ {
		if g.keepCount(n) {
			kept++
		}
	}
	if kept != 100 || !g.keepCount(1) || g.keepCount(11) {
		t.Fatalf("10%% count gate kept %d/1000 with unexpected window boundaries", kept)
	}
}

func TestKeepFieldRatio(t *testing.T) {
	template := scalarTemplate(t)
	g := mustGate(t, JobSpec{SampleMode: ModeFieldRatio, SampleField: "unsigned_id", SampleRate: 10}, template)
	for _, tc := range []struct {
		entityID uint64
		want     bool
	}{{1001, true}, {1009, true}, {1010, false}, {1234, false}, {1100, true}} {
		if got := g.keepField(event(t, template, map[string]any{"unsigned_id": tc.entityID})); got != tc.want {
			t.Errorf("keepField(entity_id=%d) = %v, want %v", tc.entityID, got, tc.want)
		}
	}
}

func TestKeepFieldRatioIsStablePerID(t *testing.T) {
	template := scalarTemplate(t)
	g := mustGate(t, JobSpec{SampleMode: ModeFieldRatio, SampleField: "unsigned_id", SampleRate: 10}, template)
	for i := range 50 {
		msg := event(t, template, map[string]any{"unsigned_id": uint64(1007), "score": float64(i)})
		if !g.keepField(msg) {
			t.Fatalf("event %d of entity 1007 dropped", i)
		}
	}
}

func TestModuloRangeIsInclusiveAndNormalizesNegativeIntegers(t *testing.T) {
	template := scalarTemplate(t)
	g := mustGate(t, JobSpec{SampleRate: 100, SampleFilters: []SamplingFilter{{
		Type: FilterModuloRange, Field: "signed_id", Lower: intPtr(45), Upper: intPtr(65),
	}}}, template)
	for _, tc := range []struct {
		value int64
		want  bool
	}{{45, true}, {65, true}, {44, false}, {66, false}, {-55, true}, {-35, true}, {-34, false}} {
		if got := g.keepField(event(t, template, map[string]any{"signed_id": tc.value})); got != tc.want {
			t.Errorf("signed_id=%d match = %v, want %v", tc.value, got, tc.want)
		}
	}
}

func TestFloatComparisonOperators(t *testing.T) {
	template := scalarTemplate(t)
	for _, tc := range []struct {
		op       string
		actual   float64
		expected float64
		want     bool
	}{{">", .9, .8, true}, {">=", .8, .8, true}, {"<", .7, .8, true}, {"<=", .8, .8, true}, {"=", .8, .8, true}, {"!=", .9, .8, true}, {">", .8, .8, false}} {
		g := mustGate(t, JobSpec{SampleRate: 100, SampleFilters: []SamplingFilter{{
			Type: FilterFloatCompare, Field: "score", Operator: tc.op, Value: floatPtr(tc.expected),
		}}}, template)
		if got := g.keepField(event(t, template, map[string]any{"score": tc.actual})); got != tc.want {
			t.Errorf("%v %s %v = %v, want %v", tc.actual, tc.op, tc.expected, got, tc.want)
		}
	}
}

func TestCompositeSamplingANDsPercentageAndEveryFilter(t *testing.T) {
	template := scalarTemplate(t)
	g := mustGate(t, JobSpec{SampleRate: 10, SampleFilters: []SamplingFilter{
		{Type: FilterExact, Field: "is_enabled", Values: []string{"false"}},
		{Type: FilterModuloRange, Field: "signed_id", Lower: intPtr(45), Upper: intPtr(65)},
		{Type: FilterFloatCompare, Field: "score", Operator: ">", Value: floatPtr(.8)},
	}}, template)
	if !g.keepCount(1) || g.keepCount(11) {
		t.Fatal("percentage gate was not composed into the rule")
	}
	if !g.keepField(event(t, template, map[string]any{"is_enabled": false, "signed_id": int64(55), "score": .9})) {
		t.Fatal("message matching every payload filter was dropped")
	}
	for _, fields := range []map[string]any{
		{"is_enabled": true, "signed_id": int64(55), "score": .9},
		{"is_enabled": false, "signed_id": int64(70), "score": .9},
		{"is_enabled": false, "signed_id": int64(55), "score": .8},
	} {
		if g.keepField(event(t, template, fields)) {
			t.Fatalf("message with a failing filter was kept: %+v", fields)
		}
	}
}

func TestKeepFieldValueEverySupportedType(t *testing.T) {
	template := scalarTemplate(t)
	tests := []struct {
		name        string
		field       string
		values      []string
		matching    any
		nonmatching any
	}{
		{"signed integer", "signed_id", []string{"-2", "7"}, int64(-2), int64(8)},
		{"full uint64", "unsigned_id", []string{fmt.Sprint(uint64(math.MaxUint64))}, uint64(math.MaxUint64), uint64(8)},
		{"boolean", "is_enabled", []string{"true"}, true, false},
		{"string", "label", []string{"vip"}, "vip", "VIP"},
		{"enum by name", "service_type", []string{"SECONDARY"}, protoreflect.EnumNumber(5), protoreflect.EnumNumber(1)},
		{"enum by number", "service_type", []string{"1"}, protoreflect.EnumNumber(1), protoreflect.EnumNumber(5)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := mustGate(t, JobSpec{SampleMode: ModeFieldValue, SampleField: tc.field, SampleValues: tc.values}, template)
			if !g.keepField(event(t, template, map[string]any{tc.field: tc.matching})) {
				t.Error("matching event was dropped")
			}
			if g.keepField(event(t, template, map[string]any{tc.field: tc.nonmatching})) {
				t.Error("non-matching event was kept")
			}
		})
	}
}

func TestFieldValueUsesProto3Defaults(t *testing.T) {
	template := scalarTemplate(t)
	for _, spec := range []JobSpec{
		{SampleMode: ModeFieldValue, SampleField: "signed_id", SampleValues: []string{"0"}},
		{SampleMode: ModeFieldValue, SampleField: "is_enabled", SampleValues: []string{"false"}},
		{SampleMode: ModeFieldValue, SampleField: "label", SampleValues: []string{""}},
		{SampleMode: ModeFieldValue, SampleField: "service_type", SampleValues: []string{"NONE"}},
	} {
		if !mustGate(t, spec, template).keepField(event(t, template, nil)) {
			t.Errorf("unset proto3 %s did not match its default", spec.SampleField)
		}
	}
}

func TestFieldModesRequireDecode(t *testing.T) {
	for _, spec := range []JobSpec{
		{SampleMode: ModeFieldRatio, SampleField: "unsigned_id", SampleRate: 10},
		{SampleMode: ModeFieldValue, SampleField: "unsigned_id", SampleValues: []string{"1"}},
	} {
		if mustGate(t, spec).preDecode() {
			t.Errorf("%s unexpectedly runs before decode", spec.SampleMode)
		}
	}
}

// This exercises the complete data-plane path for every supported value kind:
// protobuf bytes -> decode -> sampling gate -> row hand-off.
func TestHandlerFiltersEverySupportedValueKindEndToEnd(t *testing.T) {
	template := scalarTemplate(t)
	tests := []struct {
		name        string
		field       string
		values      []string
		matching    any
		nonmatching any
	}{
		{"integer", "signed_id", []string{"2"}, int64(2), int64(3)},
		{"boolean", "is_enabled", []string{"true"}, true, false},
		{"string", "label", []string{"priority"}, "priority", "ordinary"},
		{"enum", "service_type", []string{"SECONDARY"}, protoreflect.EnumNumber(5), protoreflect.EnumNumber(1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := JobSpec{Topic: "events", SampleMode: ModeFieldValue, SampleField: tc.field, SampleValues: tc.values}
			resolve := func(string) (proto.Message, error) { return template.ProtoReflect().New().Interface(), nil }
			rec := New(spec, resolve)
			rowCh := make(chan pg.Row, 2)
			handle := rec.handler(template, schema.Introspect(template), rowCh)
			for _, fields := range []map[string]any{{tc.field: tc.matching}, {tc.field: tc.nonmatching}} {
				data, err := proto.Marshal(event(t, template, fields))
				if err != nil {
					t.Fatal(err)
				}
				handle(&nats.Msg{Subject: "events", Data: data})
			}
			if got := len(rowCh); got != 1 {
				t.Fatalf("row hand-off count = %d, want one matching event", got)
			}
			if got := rec.Snapshot().Received; got != 2 {
				t.Fatalf("received count = %d, want 2", got)
			}
		})
	}
}

func TestHandlerAppliesPercentageAndMultipleFiltersEndToEnd(t *testing.T) {
	template := scalarTemplate(t)
	spec := JobSpec{Topic: "events", SampleRate: 10, SampleFilters: []SamplingFilter{
		{Type: FilterExact, Field: "is_enabled", Values: []string{"false"}},
		{Type: FilterModuloRange, Field: "signed_id", Lower: intPtr(45), Upper: intPtr(65)},
		{Type: FilterFloatCompare, Field: "score", Operator: ">", Value: floatPtr(.8)},
	}}
	resolve := func(string) (proto.Message, error) { return template.ProtoReflect().New().Interface(), nil }
	rec := New(spec, resolve)
	rowCh := make(chan pg.Row, 11)
	handle := rec.handler(template, schema.Introspect(template), rowCh)

	messages := []map[string]any{
		{"is_enabled": false, "signed_id": int64(55), "score": .9}, // all pass
		{"is_enabled": true, "signed_id": int64(55), "score": .9},  // exact fails
		{"is_enabled": false, "signed_id": int64(70), "score": .9}, // modulo fails
		{"is_enabled": false, "signed_id": int64(55), "score": .8}, // float fails
	}
	for len(messages) < 10 {
		messages = append(messages, map[string]any{"is_enabled": true, "signed_id": int64(0), "score": 0.0})
	}
	// This matches every field condition but is arrival 11, outside the first
	// ten positions kept by a 10% percentage window.
	messages = append(messages, map[string]any{"is_enabled": false, "signed_id": int64(55), "score": .9})

	for _, fields := range messages {
		data, err := proto.Marshal(event(t, template, fields))
		if err != nil {
			t.Fatal(err)
		}
		handle(&nats.Msg{Subject: "events", Data: data})
	}
	if got := len(rowCh); got != 1 {
		t.Fatalf("row hand-off count = %d, want only the message passing percentage and every filter", got)
	}
	if got := rec.Snapshot().Received; got != 11 {
		t.Fatalf("received count = %d, want 11", got)
	}
}

func TestDescribe(t *testing.T) {
	template := scalarTemplate(t)
	tests := []struct {
		spec JobSpec
		tmpl proto.Message
		want string
	}{
		{JobSpec{SampleMode: ModeRate, SampleRate: 10}, template, "10%"},
		{JobSpec{SampleMode: ModeFieldRatio, SampleField: "signed_id", SampleRate: 25}, template, "signed_id%100 0–24"},
		{JobSpec{SampleMode: ModeFieldValue, SampleField: "is_enabled", SampleValues: []string{"true"}}, template, "is_enabled = true"},
		{JobSpec{SampleMode: ModeFieldValue, SampleField: "service_type", SampleValues: []string{"PRIMARY", "SECONDARY"}}, template, "service_type in 2 values"},
		{JobSpec{SampleRate: 10, SampleFilters: []SamplingFilter{
			{Type: FilterExact, Field: "is_enabled", Values: []string{"false"}},
			{Type: FilterModuloRange, Field: "signed_id", Lower: intPtr(45), Upper: intPtr(65)},
			{Type: FilterFloatCompare, Field: "score", Operator: ">", Value: floatPtr(.8)},
		}}, template, "10% · is_enabled = false · signed_id%100 45–65 · score > 0.8"},
	}
	for _, tc := range tests {
		if got := mustGate(t, tc.spec, tc.tmpl).describe(); got != tc.want {
			t.Errorf("describe() = %q, want %q", got, tc.want)
		}
	}
}

func floatPtr(value float64) *float64 { return &value }
