package manager

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"seath/internal/recorder"
	"seath/internal/schema"
)

// classifyMappingUnderLock honours classifyMapping's m.mu contract.
func (m *Manager) classifyMappingUnderLock(t, protoName string) (Mapping, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.classifyMapping(t, protoName)
}

// protoName is the fully-qualified name of a resolved template.
func protoName(m proto.Message) string {
	return string(m.ProtoReflect().Descriptor().FullName())
}

// anotherProtoType returns a catalogue type name other than exclude.
func anotherProtoType(t *testing.T, m *Manager, exclude string) string {
	t.Helper()
	for _, ti := range m.catalogue.List() {
		if ti.Name != exclude {
			return ti.Name
		}
	}
	t.Fatal("fewer than two proto types are linked in")
	return ""
}

// findMapping returns the entry for a topic, failing if it is absent.
func findMapping(t *testing.T, ms []Mapping, name string) Mapping {
	t.Helper()
	for _, m := range ms {
		if m.Topic == name {
			return m
		}
	}
	t.Fatalf("topic %q missing from mappings", name)
	return Mapping{}
}

// The database is the only source: a topic with no stored row must not resolve.
func TestResolveTopicUsesStoredMappingsOnly(t *testing.T) {
	m := testManager(t)
	name := anyTopic(t)

	if _, err := m.resolveTopic(name); err != nil {
		t.Fatalf("seeded topic did not resolve: %v", err)
	}

	delete(m.custom, name)
	if _, err := m.resolveTopic(name); err == nil {
		t.Fatal("a deleted topic still resolved")
	}
}

// Editing a mapping has to change what the topic decodes as.
func TestResolveTopicFollowsAnEdit(t *testing.T) {
	m := testManager(t)
	name := anyTopic(t)
	before := m.custom[name]
	other := anotherProtoType(t, m, before)

	m.custom[name] = other
	got, err := m.resolveTopic(name)
	if err != nil {
		t.Fatalf("resolve after edit: %v", err)
	}
	if fq := protoName(got); fq != other {
		t.Fatalf("resolved to %q, want %q", fq, other)
	}
}

func TestResolveTopicRejectsUnknown(t *testing.T) {
	m := testManager(t)
	if _, err := m.resolveTopic("no.such.topic.anywhere"); err == nil {
		t.Fatal("expected an error for an unmapped topic")
	}
}

func TestRecordingResolverKeepsPinnedTypeAfterMappingChange(t *testing.T) {
	m := testManager(t)
	spec := validSpec(t)

	m.mu.RLock()
	tmpl, name, moduleIDs, err := m.currentProtoSnapshotLocked(spec.Topic)
	m.mu.RUnlock()
	if err != nil {
		t.Fatalf("capture snapshot: %v", err)
	}
	spec.ProtoName = name
	spec.ProtoModuleIDs = moduleIDs
	rec := recorder.NewResolved(spec, fixedProtoResolver(tmpl), tmpl)

	m.custom[spec.Topic] = otherTestProto
	got, err := rec.NewMessage()
	if err != nil {
		t.Fatalf("resolve pinned recording: %v", err)
	}
	if gotName := protoName(got); gotName != testProto {
		t.Fatalf("recording switched to %q after mapping change, want pinned %q", gotName, testProto)
	}
	if !hasProtoSnapshot(rec.Spec()) {
		t.Fatal("recording lost its proto snapshot metadata")
	}
}

func TestCurrentProtoSnapshotCapturesPersistentModuleIDs(t *testing.T) {
	m := testManager(t)
	m.mu.RLock()
	msg, name, ids, err := m.currentProtoSnapshotLocked(testTopic)
	m.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	if got := protoName(msg); got != testProto || name != testProto {
		t.Fatalf("snapshot type = (%q, %q), want %q", got, name, testProto)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("snapshot module IDs = %v, want [1]", ids)
	}
}

// Every mapping is a row, so the list is exactly what is stored.
func TestTopicMappingsListsExactlyWhatIsStored(t *testing.T) {
	m := testManager(t)
	m.custom = map[string]string{
		"b.topic": "example.Lock",
		"a.topic": "example.ProbeResult",
	}

	got := m.TopicMappings()
	if len(got) != 2 {
		t.Fatalf("got %d mappings, want 2", len(got))
	}
	if got[0].Topic != "a.topic" || got[1].Topic != "b.topic" {
		t.Fatalf("not sorted by topic: %+v", got)
	}
	if got[0].ProtoName != "example.ProbeResult" {
		t.Fatalf("proto name = %q", got[0].ProtoName)
	}
}

// Topics() drives the recording builder; it must agree with the mapping table.
func TestTopicsMatchesTheMappingList(t *testing.T) {
	m := testManager(t)
	names := m.Topics()
	maps := m.TopicMappings()
	if len(names) != len(maps) {
		t.Fatalf("Topics() has %d, TopicMappings() has %d", len(names), len(maps))
	}
	if !sort.StringsAreSorted(names) {
		t.Fatal("Topics() is not sorted")
	}
	for i := range names {
		if names[i] != maps[i].Topic {
			t.Fatalf("entry %d: Topics()=%q, TopicMappings()=%q", i, names[i], maps[i].Topic)
		}
	}
}

func TestClassifyMappingRejectsEmptyInput(t *testing.T) {
	m := testManager(t)
	valid := anotherProtoType(t, m, "")
	for _, tc := range []struct{ topic, proto string }{
		{"", valid},
		{"some.topic", ""},
		{"", ""},
	} {
		if _, err := m.classifyMappingUnderLock(tc.topic, tc.proto); !errors.Is(err, ErrMapping) {
			t.Fatalf("classify(%q, %q) = %v, want ErrMapping", tc.topic, tc.proto, err)
		}
	}
}

func TestClassifyMappingRejectsUnlinkedType(t *testing.T) {
	m := testManager(t)
	_, err := m.classifyMappingUnderLock("some.topic", "not.a.real.ProtoType")
	if !errors.Is(err, ErrMapping) {
		t.Fatalf("got %v, want ErrMapping", err)
	}
}

// A seeded topic is an ordinary row: re-pointing it is allowed, exactly like a
// topic someone added by hand.
func TestClassifyMappingTreatsSeededTopicsLikeAnyOther(t *testing.T) {
	m := testManager(t)
	name := anyTopic(t)
	other := anotherProtoType(t, m, m.custom[name])

	mp, err := m.classifyMappingUnderLock(name, other)
	if err != nil {
		t.Fatalf("re-pointing a seeded topic: %v", err)
	}
	if mp.Topic != name || mp.ProtoName != other {
		t.Fatalf("got %+v", mp)
	}
}

// Existing recordings own pinned templates, so a mapping change only affects
// recordings created later.
func TestClassifyMappingAllowsWhileRecording(t *testing.T) {
	m := testManager(t)
	spec := validSpec(t)
	spec.ID = "rec-1"
	m.recorders["rec-1"] = recorder.New(spec, m.resolveTopic)

	if _, err := m.classifyMappingUnderLock(spec.Topic, anotherProtoType(t, m, "")); err != nil {
		t.Fatalf("mapping change was blocked by a pinned recording: %v", err)
	}
}

func TestSetTopicMappingWhileRecordingChangesOnlyFutureResolution(t *testing.T) {
	m := testManager(t)
	m.topics = &blockingStore{}
	spec := validSpec(t)
	spec.ID = "rec-1"
	tmpl, err := m.resolveTopic(spec.Topic)
	if err != nil {
		t.Fatal(err)
	}
	m.recorders["rec-1"] = recorder.NewResolved(spec, fixedProtoResolver(tmpl), tmpl)

	next := anotherProtoType(t, m, m.custom[spec.Topic])
	if _, err := m.SetTopicMapping(spec.Topic, next); err != nil {
		t.Fatalf("set mapping: %v", err)
	}
	if m.custom[spec.Topic] != next {
		t.Fatalf("mapping = %q, want %q", m.custom[spec.Topic], next)
	}
	old, err := m.recorders["rec-1"].NewMessage()
	if err != nil {
		t.Fatal(err)
	}
	if got := protoName(old); got != testProto {
		t.Fatalf("existing recording switched to %q, want %q", got, testProto)
	}
}

// The add form has no confirmation step, so it must not silently replace.
func TestAddTopicMappingRefusesToReplace(t *testing.T) {
	m := testManager(t)
	m.custom["already.mapped"] = anotherProtoType(t, m, "")

	_, err := m.AddTopicMapping("already.mapped", anotherProtoType(t, m, ""))
	if !errors.Is(err, ErrMappingExists) {
		t.Fatalf("got %v, want ErrMappingExists", err)
	}
}

func TestDeleteTopicMappingUnknownIsNotFound(t *testing.T) {
	m := testManager(t)
	if err := m.DeleteTopicMapping("never.mapped"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestDeleteTopicMappingLeavesPinnedRecordingUsable(t *testing.T) {
	m := testManager(t)
	m.topics = &blockingStore{}
	spec := validSpec(t)
	spec.ID = "rec-1"
	tmpl, err := m.resolveTopic(spec.Topic)
	if err != nil {
		t.Fatal(err)
	}
	m.recorders["rec-1"] = recorder.NewResolved(spec, fixedProtoResolver(tmpl), tmpl)

	if err := m.DeleteTopicMapping(spec.Topic); err != nil {
		t.Fatalf("delete mapping: %v", err)
	}
	if _, ok := m.custom[spec.Topic]; ok {
		t.Fatal("mapping still present")
	}
	if _, err := m.recorders["rec-1"].NewMessage(); err != nil {
		t.Fatalf("pinned recording stopped resolving after mapping deletion: %v", err)
	}
}

// Refuse injection-shaped topics at the door rather than escaping on the way out.
func TestClassifyMappingRejectsMalformedTopics(t *testing.T) {
	m := testManager(t)
	valid := anotherProtoType(t, m, "")
	for _, bad := range []string{
		`x" onmouseover="alert(1)`, // attribute-context injection
		`<script>alert(1)</script>`,
		"has space",
		"trailing.space ",
		" leading.space",
		"wild.*",       // NATS token wildcard
		"wild.>",       // NATS full wildcard
		"double..dot",  // empty token
		".leading.dot", // empty token
		"trailing.dot.",
		"tab\there",
		"new\nline",
	} {
		if _, err := m.classifyMappingUnderLock(bad, valid); !errors.Is(err, ErrMapping) {
			t.Errorf("classify(%q) = %v, want ErrMapping", bad, err)
		}
	}
}

// The charset must admit ordinary stored NATS subjects.
func TestStoredTopicsPassTheCharset(t *testing.T) {
	m := testManager(t)
	valid := anotherProtoType(t, m, "")
	for name := range m.custom {
		if _, err := m.classifyMappingUnderLock(name, valid); errors.Is(err, ErrMapping) {
			t.Errorf("seeded topic %q is rejected by the charset: %v", name, err)
		}
	}
}

// Stale columns from a since-changed mapping would write NULL forever.
func TestValidateRejectsColumnsFromAnotherProtoType(t *testing.T) {
	m := testManager(t)
	spec := validSpec(t)
	spec.Columns = []schema.ColumnDef{{Field: "no_such_field_anywhere", Column: "c", Include: true}}

	err := m.validate(spec)
	if !errors.Is(err, ErrValidate) {
		t.Fatalf("got %v, want ErrValidate", err)
	}
	if !strings.Contains(err.Error(), "no_such_field_anywhere") {
		t.Fatalf("error does not name the offending field: %v", err)
	}
}

func TestValidateAcceptsColumnsFromTheResolvedType(t *testing.T) {
	m := testManager(t)
	spec := validSpec(t)
	msg, err := m.resolveTopic(spec.Topic)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	spec.Columns = schema.Introspect(msg)
	if len(spec.Columns) == 0 {
		t.Skip("test topic has no fields to check against")
	}
	if err := m.validate(spec); err != nil {
		t.Fatalf("columns from the resolved type were rejected: %v", err)
	}
}

// Empty columns means "introspect at start", always consistent by construction.
func TestValidateAllowsEmptyColumns(t *testing.T) {
	m := testManager(t)
	spec := validSpec(t)
	spec.Columns = nil
	if err := m.validate(spec); err != nil {
		t.Fatalf("empty columns rejected: %v", err)
	}
}

// After re-pointing a topic, the previous type's columns must stop validating.
func TestValidateFollowsAnOverride(t *testing.T) {
	m := testManager(t)
	spec := validSpec(t)
	msg, err := m.resolveTopic(spec.Topic)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	spec.Columns = schema.Introspect(msg)
	if len(spec.Columns) == 0 {
		t.Skip("test topic has no fields to check against")
	}
	if err := m.validate(spec); err != nil {
		t.Fatalf("baseline rejected: %v", err)
	}

	// Re-point the topic at a type that shares no field names with the old one.
	m.custom[spec.Topic] = otherTestProto
	if err := m.validate(spec); !errors.Is(err, ErrValidate) {
		t.Fatalf("stale columns accepted after the mapping changed: %v", err)
	}
}

// blockingStore holds AddCustomTopic open until released, so a test can observe
// whether a second mapping mutation is genuinely excluded.
type blockingStore struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	writes  []string
}

// Only the first write blocks; later ones would have no reader for entered.
func (b *blockingStore) AddCustomTopic(topic, protoName string) error {
	b.mu.Lock()
	b.writes = append(b.writes, topic)
	b.mu.Unlock()
	if b.entered != nil {
		b.once.Do(func() {
			b.entered <- struct{}{}
			<-b.release
		})
	}
	return nil
}

func (b *blockingStore) DeleteCustomTopic(topic string) error { return nil }

// F1: the guard, the row and the map are one decision. While one mapping write
// is in flight, every other mapping mutation and every reader of m.custom must
// be excluded — before the fix each took its own short lock and could interleave.
func TestMappingMutationExcludesEverythingElse(t *testing.T) {
	m := testManager(t)
	bs := &blockingStore{entered: make(chan struct{}), release: make(chan struct{})}
	m.topics = bs

	go func() { _, _ = m.AddTopicMapping("first.topic", testProto) }()
	<-bs.entered // the write is now in flight, holding m.mu

	// A different topic, so nothing but the lock itself can serialize these.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = m.AddTopicMapping("second.topic", testProto)
	}()
	// And a plain reader.
	read := make(chan struct{})
	go func() {
		defer close(read)
		m.TopicMappings()
	}()

	select {
	case <-done:
		t.Fatal("a second mapping mutation ran while one was mid-write")
	case <-read:
		t.Fatal("a reader observed m.custom while a mapping was mid-write")
	case <-time.After(150 * time.Millisecond):
	}

	close(bs.release)
	for _, c := range []chan struct{}{done, read} {
		select {
		case <-c:
		case <-time.After(5 * time.Second):
			t.Fatal("a caller stayed blocked after the write was released")
		}
	}

	bs.mu.Lock()
	defer bs.mu.Unlock()
	if len(bs.writes) != 2 {
		t.Fatalf("writes = %v, want both topics persisted once", bs.writes)
	}
}

// The existence check and the write are one decision too: only one of several
// concurrent Adds for the same absent topic may persist it.
func TestConcurrentAddOfTheSameTopicPersistsOnce(t *testing.T) {
	m := testManager(t)
	bs := &blockingStore{}
	m.topics = bs

	var wg sync.WaitGroup
	created := make(chan struct{}, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.AddTopicMapping("contested.topic", testProto); err == nil {
				created <- struct{}{}
			}
		}()
	}
	wg.Wait()
	close(created)

	if n := len(created); n != 1 {
		t.Fatalf("%d callers were told they created the mapping, want exactly 1", n)
	}
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if len(bs.writes) != 1 {
		t.Fatalf("persisted %d times, want 1: %v", len(bs.writes), bs.writes)
	}
}

// A field that kept its name across a re-point but changed type would take the
// old column type: ExampleText.id is TEXT where ExampleNumber.id is BIGINT.
func TestValidateRejectsSameNameFieldWithADifferentType(t *testing.T) {
	m := testManager(t)
	spec := validSpec(t)
	msg, err := m.resolveTopic(spec.Topic)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	cols := schema.Introspect(msg)
	if len(cols) == 0 {
		t.Skip("test topic has no fields")
	}
	// Same field name, wrong SQL type.
	cols[0].Type = "BOOLEAN"
	if cols[0].Type == schema.Introspect(msg)[0].Type {
		t.Skip("field is already BOOLEAN")
	}
	spec.Columns = cols

	err = m.validate(spec)
	if !errors.Is(err, ErrValidate) {
		t.Fatalf("got %v, want ErrValidate", err)
	}
	if !strings.Contains(err.Error(), cols[0].Field) {
		t.Fatalf("error does not name the field: %v", err)
	}
}

// A client that leaves Type empty is asking the server to derive it, which is
// always consistent.
func TestValidateAllowsColumnsWithoutAType(t *testing.T) {
	m := testManager(t)
	spec := validSpec(t)
	msg, err := m.resolveTopic(spec.Topic)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	cols := schema.Introspect(msg)
	if len(cols) == 0 {
		t.Skip("test topic has no fields")
	}
	for i := range cols {
		cols[i].Type = ""
	}
	spec.Columns = cols
	if err := m.validate(spec); err != nil {
		t.Fatalf("columns without a type were rejected: %v", err)
	}
}

// The charset must keep out what can break HTML or NATS, while still admitting
// the punctuation NATS itself allows in a subject token.
func TestTopicPatternAdmitsLegitimateSubjects(t *testing.T) {
	for _, ok := range []string{"a", "example.finished", "example.events.raw.v2", "example.state-validation", "svc:v1.topic", "a/b.c", "x@y.z", "a_b.c2"} {
		if !topicPattern.MatchString(ok) {
			t.Errorf("rejected legitimate subject %q", ok)
		}
	}
}
