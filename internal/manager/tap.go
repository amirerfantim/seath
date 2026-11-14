package manager

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protowire"

	"seath/internal/natspreset"
)

// maxSamples is how many recent messages a tap keeps for preview.
const maxSamples = 10

// sampleInterval throttles how often a tap captures a sample, so a high-rate
// subject doesn't churn allocations (the count/bytes still update per message).
const sampleInterval = 150 * time.Millisecond

// previewCap bounds how many payload bytes are retained per sample.
const previewCap = 512

// Tap is an ephemeral subscription used to check whether anything is publishing
// on a subject. It does no schema-aware decoding or persistence — just counts
// messages and keeps a small ring buffer of recent payloads (previewed at view
// time). Taps live in memory only and disappear on restart.
type Tap struct {
	id      string
	subject string
	env     string

	count atomic.Uint64
	bytes atomic.Uint64

	mu           sync.Mutex
	status       string
	lastError    string
	startedAt    time.Time
	lastAt       time.Time
	lastSampleAt time.Time
	samples      []rawSample // most-recent-last ring buffer

	cancel context.CancelFunc
	done   chan struct{}
}

// rawSample holds a captured payload (capped) for later preview rendering.
type rawSample struct {
	at      time.Time
	subject string
	size    int
	data    []byte
}

// TapSample is a preview of one observed message. Preview is a one-line summary
// for the list; Detail is the full (within the capture cap) pretty decode and
// Hex is the raw hex dump, both shown in the message detail view.
type TapSample struct {
	At      time.Time `json:"at"`
	Subject string    `json:"subject"`
	Size    int       `json:"size"`
	Capped  bool      `json:"capped"`
	Preview string    `json:"preview"`
	Detail  string    `json:"detail"`
	Hex     string    `json:"hex"`
}

// TapView is the API representation of a tap.
type TapView struct {
	ID        string      `json:"id"`
	Subject   string      `json:"subject"`
	Env       string      `json:"env"`
	Status    string      `json:"status"`
	Count     uint64      `json:"count"`
	Bytes     uint64      `json:"bytes"`
	StartedAt time.Time   `json:"started_at"`
	LastAt    *time.Time  `json:"last_at"`
	LastError string      `json:"last_error"`
	Samples   []TapSample `json:"samples"`
}

// StartTap opens a subscription on the given subject and env. The subject may
// contain NATS wildcards (* and >).
func (m *Manager) StartTap(subject, env string) (TapView, error) {
	subject = strings.TrimSpace(subject)
	if subject == "" || strings.ContainsAny(subject, " \t\n") {
		return TapView{}, fmt.Errorf("%w: subject is required and must not contain spaces", ErrValidate)
	}
	preset, ok := m.resolvePreset(env)
	if !ok {
		return TapView{}, fmt.Errorf("%w: unknown nats env %q", ErrValidate, env)
	}

	t := &Tap{
		id:        newID(),
		subject:   subject,
		env:       env,
		status:    "running",
		startedAt: time.Now(),
	}

	opts := []nats.Option{
		nats.Name("seath-tap/" + subject),
		nats.MaxReconnects(-1),
	}
	opts = append(opts, natspreset.BuildOptions(preset)...)

	nc, err := nats.Connect(preset.URL, opts...)
	if err != nil {
		return TapView{}, fmt.Errorf("nats connect to %s: %v", preset.URL, err)
	}

	sub, err := nc.Subscribe(subject, t.handle)
	if err != nil {
		nc.Close()
		return TapView{}, fmt.Errorf("subscribe to %q: %v", subject, err)
	}

	ctx, cancel := context.WithCancel(m.rootCtx)
	done := make(chan struct{})
	t.cancel = cancel
	t.done = done

	go func() {
		defer close(done)
		<-ctx.Done()
		_ = sub.Unsubscribe()
		_ = nc.Drain()
	}()

	m.tapMu.Lock()
	m.taps[t.id] = t
	m.tapMu.Unlock()

	slog.Info("tap subscribed", "id", t.id, "subject", subject, "env", preset.Name)
	return m.tapView(t), nil
}

func (t *Tap) handle(msg *nats.Msg) {
	t.count.Add(1)
	t.bytes.Add(uint64(len(msg.Data)))
	now := time.Now()
	t.mu.Lock()
	t.lastAt = now
	// Throttle sample capture so a high-rate subject doesn't churn allocations.
	if now.Sub(t.lastSampleAt) >= sampleInterval {
		t.lastSampleAt = now
		d := msg.Data
		if len(d) > previewCap {
			d = d[:previewCap]
		}
		cp := make([]byte, len(d))
		copy(cp, d)
		t.samples = append(t.samples, rawSample{at: now, subject: msg.Subject, size: len(msg.Data), data: cp})
		if len(t.samples) > maxSamples {
			t.samples = t.samples[len(t.samples)-maxSamples:]
		}
	}
	t.mu.Unlock()
}

// ListTaps returns all taps, newest first.
func (m *Manager) ListTaps() []TapView {
	m.tapMu.Lock()
	views := make([]TapView, 0, len(m.taps))
	for _, t := range m.taps {
		views = append(views, m.tapView(t))
	}
	m.tapMu.Unlock()
	sort.Slice(views, func(i, j int) bool { return views[i].StartedAt.After(views[j].StartedAt) })
	return views
}

// StopTap tears down a tap and removes it.
func (m *Manager) StopTap(id string) error {
	m.tapMu.Lock()
	t, ok := m.taps[id]
	if ok {
		delete(m.taps, id)
	}
	m.tapMu.Unlock()
	if !ok {
		return ErrNotFound
	}
	t.stop()
	slog.Info("tap stopped", "id", id, "observed", t.count.Load())
	return nil
}

func (t *Tap) stop() {
	t.mu.Lock()
	cancel := t.cancel
	t.status = "stopped"
	t.mu.Unlock()
	if cancel != nil {
		cancel()
		<-t.done
	}
}

// stopAllTaps tears down every tap (used on shutdown).
func (m *Manager) stopAllTaps() {
	m.tapMu.Lock()
	taps := make([]*Tap, 0, len(m.taps))
	for id, t := range m.taps {
		taps = append(taps, t)
		delete(m.taps, id)
	}
	m.tapMu.Unlock()
	for _, t := range taps {
		t.stop()
	}
}

func (m *Manager) tapView(t *Tap) TapView {
	t.mu.Lock()
	defer t.mu.Unlock()
	v := TapView{
		ID:        t.id,
		Subject:   t.subject,
		Env:       t.env,
		Status:    t.status,
		Count:     t.count.Load(),
		Bytes:     t.bytes.Load(),
		StartedAt: t.startedAt,
		LastError: t.lastError,
	}
	if !t.lastAt.IsZero() {
		la := t.lastAt
		v.LastAt = &la
	}
	v.Samples = make([]TapSample, len(t.samples))
	for i, s := range t.samples {
		v.Samples[i] = TapSample{
			At:      s.at,
			Subject: s.subject,
			Size:    s.size,
			Capped:  s.size > len(s.data),
			Preview: preview(s.data),
			Detail:  detail(s.data),
			Hex:     hexDump(s.data),
		}
	}
	return v
}

// preview renders a short, human-readable view of a payload: a truncated string
// if it's mostly printable text (e.g. JSON), a schema-less protobuf wire decode
// (field-number:value) if it parses as protobuf, otherwise a hex dump.
func preview(data []byte) string {
	if len(data) == 0 {
		return "(empty)"
	}
	if utf8.Valid(data) && mostlyPrintable(data) {
		const max = 120
		s := strings.TrimSpace(string(data))
		if len(s) > max {
			return s[:max] + "…"
		}
		return s
	}
	if s, ok := decodeProto(data, 0); ok && s != "" {
		const max = 200
		if len(s) > max {
			return s[:max] + "…"
		}
		return s
	}
	const max = 32
	n := len(data)
	suffix := ""
	if n > max {
		n = max
		suffix = " …"
	}
	return hex.EncodeToString(data[:n]) + suffix
}

// decodeProto walks protobuf wire bytes and renders "field:value" pairs without
// a schema. Value types are inferred from the wire type (varints as integers,
// fixed32/64 as floats, length-delimited as nested messages / strings / hex).
// Best-effort: returns false if the bytes don't parse cleanly as protobuf.
func decodeProto(data []byte, depth int) (string, bool) {
	if depth > 4 {
		return "", false
	}
	var parts []string
	b := data
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", false
		}
		b = b[n:]
		var val string
		switch typ {
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return "", false
			}
			b = b[n:]
			val = strconv.FormatInt(int64(v), 10)
		case protowire.Fixed32Type:
			v, n := protowire.ConsumeFixed32(b)
			if n < 0 {
				return "", false
			}
			b = b[n:]
			val = strconv.FormatFloat(float64(math.Float32frombits(v)), 'g', 6, 32)
		case protowire.Fixed64Type:
			v, n := protowire.ConsumeFixed64(b)
			if n < 0 {
				return "", false
			}
			b = b[n:]
			val = strconv.FormatFloat(math.Float64frombits(v), 'g', 8, 64)
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return "", false
			}
			b = b[n:]
			val = formatBytesField(v, depth)
		default:
			// Groups and unknown wire types: bail to hex fallback.
			return "", false
		}
		parts = append(parts, strconv.Itoa(int(num))+":"+val)
		if len(parts) >= 40 {
			break
		}
	}
	if len(parts) == 0 {
		return "", false
	}
	return strings.Join(parts, " "), true
}

// formatBytesField renders a length-delimited field, preferring a printable
// string, then a nested message, then hex.
func formatBytesField(v []byte, depth int) string {
	if len(v) == 0 {
		return `""`
	}
	if utf8.Valid(v) && mostlyPrintable(v) {
		return strconv.Quote(string(v))
	}
	if s, ok := decodeProto(v, depth+1); ok {
		return "{" + s + "}"
	}
	h := hex.EncodeToString(v)
	if len(h) > 24 {
		h = h[:24] + "…"
	}
	return "0x" + h
}

// detail renders the full payload: text as-is, protobuf as an indented,
// type-annotated field tree, otherwise a hex dump.
func detail(data []byte) string {
	if len(data) == 0 {
		return "(empty)"
	}
	if utf8.Valid(data) && mostlyPrintable(data) {
		return strings.TrimSpace(string(data))
	}
	if s, ok := decodeProtoPretty(data, 0); ok && s != "" {
		return s
	}
	return hexDump(data)
}

// decodeProtoPretty renders protobuf wire bytes as indented "field  type  value"
// lines, recursing into nested messages. Best-effort; false if it doesn't parse.
func decodeProtoPretty(data []byte, depth int) (string, bool) {
	if depth > 6 {
		return "", false
	}
	indent := strings.Repeat("    ", depth)
	var sb strings.Builder
	b := data
	count := 0
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", false
		}
		b = b[n:]
		switch typ {
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return "", false
			}
			b = b[n:]
			fmt.Fprintf(&sb, "%s%-3d varint   %d\n", indent, num, int64(v))
		case protowire.Fixed32Type:
			v, n := protowire.ConsumeFixed32(b)
			if n < 0 {
				return "", false
			}
			b = b[n:]
			fmt.Fprintf(&sb, "%s%-3d f32      %s\n", indent, num, strconv.FormatFloat(float64(math.Float32frombits(v)), 'g', 7, 32))
		case protowire.Fixed64Type:
			v, n := protowire.ConsumeFixed64(b)
			if n < 0 {
				return "", false
			}
			b = b[n:]
			fmt.Fprintf(&sb, "%s%-3d f64      %s\n", indent, num, strconv.FormatFloat(math.Float64frombits(v), 'g', 10, 64))
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return "", false
			}
			b = b[n:]
			if len(v) > 0 && utf8.Valid(v) && mostlyPrintable(v) {
				fmt.Fprintf(&sb, "%s%-3d string   %q\n", indent, num, string(v))
			} else if nested, ok := decodeProtoPretty(v, depth+1); ok && len(v) > 0 {
				fmt.Fprintf(&sb, "%s%-3d message  (%d bytes)\n%s", indent, num, len(v), nested)
			} else {
				h := hex.EncodeToString(v)
				if len(h) > 48 {
					h = h[:48] + "…"
				}
				fmt.Fprintf(&sb, "%s%-3d bytes    %d bytes  0x%s\n", indent, num, len(v), h)
			}
		default:
			return "", false
		}
		count++
		if count >= 200 {
			break
		}
	}
	if count == 0 {
		return "", false
	}
	return sb.String(), true
}

// hexDump renders a classic offset / hex / ASCII dump.
func hexDump(data []byte) string {
	var sb strings.Builder
	for i := 0; i < len(data); i += 16 {
		end := i + 16
		if end > len(data) {
			end = len(data)
		}
		chunk := data[i:end]
		fmt.Fprintf(&sb, "%04x  ", i)
		for j := 0; j < 16; j++ {
			if j < len(chunk) {
				fmt.Fprintf(&sb, "%02x ", chunk[j])
			} else {
				sb.WriteString("   ")
			}
		}
		sb.WriteString(" ")
		for _, c := range chunk {
			if c >= 0x20 && c < 0x7f {
				sb.WriteByte(c)
			} else {
				sb.WriteByte('.')
			}
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}

func mostlyPrintable(data []byte) bool {
	n := len(data)
	if n > 256 {
		n = 256
	}
	printable := 0
	for _, b := range data[:n] {
		if b >= 0x20 && b < 0x7f || b == '\t' || b == '\n' || b == '\r' {
			printable++
		}
	}
	return printable*10 >= n*9 // >= 90% printable
}
