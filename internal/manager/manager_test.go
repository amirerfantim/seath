package manager

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"seath/internal/config"
	"seath/internal/natspreset"
	"seath/internal/prototypes"
	"seath/internal/recorder"
)

const testMaxRetentionHours = 24
const testTopic = "test.events"
const testProto = "test.Event"
const otherTestProto = "test.OtherEvent"

// testManager builds a Manager with no database, its mappings preloaded the way
// Reconcile would after seeding. NATS environments are DB-backed in production,
// so tests insert the environment they need directly into the live map.
func testManager(t *testing.T) *Manager {
	t.Helper()
	m := New(context.Background(), nil, "", nil, testMaxRetentionHours, config.DefaultQueryTimeoutSeconds, config.DefaultQueryMaxRows)
	m.presets["prod"] = natspreset.Preset{Name: "prod", URL: "nats://example:4222"}
	m.catalogue = testCatalogue(t)
	m.custom = map[string]string{testTopic: testProto}
	return m
}

func testCatalogue(t *testing.T) *prototypes.Registry {
	t.Helper()
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name: proto.String("test.proto"), Package: proto.String("test"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Event"), Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("first_value"), Number: proto.Int32(1),
				Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			}}},
			{Name: proto.String("OtherEvent"), Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("other_value"), Number: proto.Int32(1),
				Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			}}},
		},
	}}}
	data, err := proto.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := prototypes.Build([]prototypes.Module{{ID: 1, Path: "test/proto", Version: "v1", DescriptorSet: data}})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestManagerStartsWithoutCompiledInPresets(t *testing.T) {
	m := New(context.Background(), nil, "", nil, testMaxRetentionHours, config.DefaultQueryTimeoutSeconds, config.DefaultQueryMaxRows)
	if got := m.Presets(); len(got) != 0 {
		t.Fatalf("new manager has compiled-in environments: %+v", got)
	}
}

func TestActivationSnapshotGuardAllowsPinnedRecordings(t *testing.T) {
	m := testManager(t)
	spec := validSpec(t)
	spec.ID = "pinned"
	m.mu.RLock()
	tmpl, protoName, moduleIDs, err := m.currentProtoSnapshotLocked(spec.Topic)
	m.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	spec.ProtoName, spec.ProtoModuleIDs = protoName, moduleIDs
	m.recorders[spec.ID] = recorder.NewResolved(spec, fixedProtoResolver(tmpl), tmpl)

	if err := activationSnapshotError(m.recorders); err != nil {
		t.Fatalf("pinned recording blocked activation: %v", err)
	}
}

func TestActivationSnapshotGuardRejectsUnmigratedRecording(t *testing.T) {
	m := testManager(t)
	spec := validSpec(t)
	spec.ID = "legacy"
	m.recorders[spec.ID] = recorder.New(spec, m.resolveTopic)

	err := activationSnapshotError(m.recorders)
	if !errors.Is(err, ErrInUse) || !strings.Contains(err.Error(), spec.ID) {
		t.Fatalf("activationSnapshotError = %v, want ErrInUse naming %q", err, spec.ID)
	}
}

// anyTopic returns a stored topic name.
func anyTopic(t *testing.T) string {
	t.Helper()
	return testTopic
}

func validSpec(t *testing.T) recorder.JobSpec {
	t.Helper()
	return recorder.JobSpec{
		Topic:          anyTopic(t),
		Table:          "some_table",
		SampleRate:     10,
		NATSEnv:        "prod",
		RetentionHours: 1,
	}
}

func TestValidateRetentionHours(t *testing.T) {
	m := testManager(t)
	tests := []struct {
		name    string
		hours   int
		wantErr bool
	}{
		{"unset is refused — retention is mandatory", 0, true},
		{"one hour", 1, false},
		{"mid range", 6, false},
		{"the configured ceiling", testMaxRetentionHours, false},
		{"one over the ceiling", testMaxRetentionHours + 1, true},
		{"a week in hours", 168, true},
		{"negative", -1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := validSpec(t)
			spec.RetentionHours = tc.hours
			err := m.validate(spec)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("retention %d: expected rejection, got none", tc.hours)
				}
				if !errors.Is(err, ErrValidate) {
					t.Errorf("retention %d: want ErrValidate, got %v", tc.hours, err)
				}
				if !strings.Contains(err.Error(), "retention_hours") {
					t.Errorf("retention %d: message should name the field, got %q", tc.hours, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("retention %d: unexpected error %v", tc.hours, err)
			}
		})
	}
}

// The ceiling is configurable, so validation must follow the configured value
// rather than a hardcoded day.
func TestValidateFollowsConfiguredCeiling(t *testing.T) {
	m := New(context.Background(), nil, "", nil, 6, config.DefaultQueryTimeoutSeconds, config.DefaultQueryMaxRows)
	m.presets["prod"] = natspreset.Preset{Name: "prod", URL: "nats://example:4222"}
	m.catalogue = testCatalogue(t)
	m.custom = map[string]string{testTopic: testProto}

	spec := validSpec(t)
	spec.RetentionHours = 6
	if err := m.validate(spec); err != nil {
		t.Errorf("6h under a 6h ceiling should be allowed, got %v", err)
	}
	spec.RetentionHours = 7
	if err := m.validate(spec); err == nil {
		t.Error("7h under a 6h ceiling must be rejected")
	}
	if m.MaxRetentionHours() != 6 {
		t.Errorf("MaxRetentionHours() = %d, want 6", m.MaxRetentionHours())
	}
}

// A missing or nonsensical configured ceiling falls back to the default rather
// than leaving retention unbounded.
func TestManagerFallsBackToDefaultCeiling(t *testing.T) {
	for _, given := range []int{0, -5} {
		m := New(context.Background(), nil, "", nil, given, config.DefaultQueryTimeoutSeconds, config.DefaultQueryMaxRows)
		if m.MaxRetentionHours() != config.DefaultMaxRetentionHours {
			t.Errorf("ceiling %d: got %d, want the %d default", given, m.MaxRetentionHours(), config.DefaultMaxRetentionHours)
		}
	}
}

func TestRetentionJobsSkipsUnsetAndKeepsHours(t *testing.T) {
	m := testManager(t)
	specs := []recorder.JobSpec{
		{ID: "a", Table: "no_retention_set", RetentionHours: 0},
		{ID: "b", Table: "one_hour", RetentionHours: 1},
		{ID: "c", Table: "one_day", RetentionHours: 24},
	}
	for _, s := range specs {
		m.recorders[s.ID] = recorder.New(s, m.resolveTopic)
	}

	got := map[string]int{}
	for _, j := range m.retentionJobs() {
		got[j.table] = j.hours
	}
	if len(got) != 2 {
		t.Fatalf("want 2 retention jobs, got %d (%v)", len(got), got)
	}
	if _, ok := got["no_retention_set"]; ok {
		t.Error("a recording with no retention must not be scheduled for cleanup")
	}
	if got["one_hour"] != 1 || got["one_day"] != 24 {
		t.Errorf("hours passed through wrong: %v", got)
	}
}

// The cleanup tick has to be well under the smallest retention window, or a
// 1-hour retention would hold data for close to two hours.
func TestCleanupIntervalIsWellUnderAnHour(t *testing.T) {
	if config.RetentionSweepInterval.Minutes() > 15 {
		t.Errorf("RetentionSweepInterval = %v, too coarse for hour-granularity retention", config.RetentionSweepInterval)
	}
}

func TestValidateSampleRateStillBounded(t *testing.T) {
	m := testManager(t)
	for _, rate := range []int{0, -1, 101} {
		spec := validSpec(t)
		spec.SampleRate = rate
		if err := m.validate(spec); err == nil {
			t.Errorf("sample rate %d: expected rejection", rate)
		}
	}
}

func TestValidateRejectsUnknownTopicAndEnv(t *testing.T) {
	m := testManager(t)

	spec := validSpec(t)
	spec.Topic = "definitely.not.a.registered.topic"
	if err := m.validate(spec); err == nil {
		t.Error("unknown topic should be rejected")
	}

	spec = validSpec(t)
	spec.NATSEnv = "nowhere"
	if err := m.validate(spec); err == nil {
		t.Error("unknown nats env should be rejected")
	}

	spec = validSpec(t)
	spec.Table = ""
	if err := m.validate(spec); err == nil {
		t.Error("empty table name should be rejected")
	}
}
