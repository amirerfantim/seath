package schema

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Recordings decode through dynamicpb, so a Timestamp field arrives as a
// *dynamicpb.Message rather than *timestamppb.Timestamp.
func dynamicEventType(t *testing.T) *dynamicpb.Message {
	t.Helper()
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:       proto.String("event.proto"),
		Package:    proto.String("test"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"google/protobuf/timestamp.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Event"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:     proto.String("at"),
				Number:   proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".google.protobuf.Timestamp"),
				JsonName: proto.String("at"),
			}},
		}},
	}, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	return dynamicpb.NewMessage(file.Messages().ByName("Event"))
}

func TestExtractValuesDynamicTimestamp(t *testing.T) {
	want := time.Date(2026, 9, 26, 10, 30, 0, 123000000, time.UTC)
	wire, err := proto.Marshal(timestamppb.New(want))
	if err != nil {
		t.Fatal(err)
	}
	// Field 1, wire type 2 (length-delimited) wrapping the Timestamp bytes.
	payload := append([]byte{0x0a, byte(len(wire))}, wire...)

	msg := dynamicEventType(t)
	if err := proto.Unmarshal(payload, msg); err != nil {
		t.Fatal(err)
	}
	cols := Introspect(msg)
	if cols[0].Type != "TIMESTAMPTZ" {
		t.Fatalf("type = %q, want TIMESTAMPTZ", cols[0].Type)
	}
	got, ok := ExtractValues(msg, cols)[0].(time.Time)
	if !ok || !got.Equal(want) {
		t.Fatalf("value = %#v, want %v", ExtractValues(msg, cols)[0], want)
	}
}

func TestExtractValuesUnsetTimestampIsNull(t *testing.T) {
	msg := dynamicEventType(t)
	if v := ExtractValues(msg, Introspect(msg))[0]; v != nil {
		t.Fatalf("value = %#v, want nil", v)
	}
}
