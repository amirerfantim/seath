package prototypes

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func descriptorBundle(t *testing.T, file, pkg, message string) []byte {
	t.Helper()
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:    proto.String(file),
		Package: proto.String(pkg),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String(message),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("value"), Number: proto.Int32(1),
				Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			}},
		}},
	}}}
	data, err := proto.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestBuildCreatesDynamicMessages(t *testing.T) {
	module := Module{Path: "example/proto", Version: "v1.0.0", Active: true,
		DescriptorSet: descriptorBundle(t, "event.proto", "example.events", "Event")}
	registry, err := Build([]Module{module})
	if err != nil {
		t.Fatal(err)
	}
	if !registry.Exists("example.events.Event") {
		t.Fatal("dynamic message type is missing")
	}
	message, err := registry.New("example.events.Event")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(message.ProtoReflect().Descriptor().FullName()); got != "example.events.Event" {
		t.Fatalf("message name = %q", got)
	}
	if got := registry.List(); len(got) != 1 || got[0].Fields != 1 {
		t.Fatalf("types = %+v", got)
	}
}

func TestBuildRejectsMalformedAndConflictingBundles(t *testing.T) {
	if _, err := Build([]Module{{Path: "bad", Version: "v1", DescriptorSet: []byte("not protobuf")}}); err == nil {
		t.Fatal("malformed descriptor set was accepted")
	}
	one := Module{Path: "one", Version: "v1", DescriptorSet: descriptorBundle(t, "same.proto", "one", "Event")}
	two := Module{Path: "two", Version: "v1", DescriptorSet: descriptorBundle(t, "same.proto", "two", "Event")}
	if _, err := Build([]Module{one, two}); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("conflicting file error = %v", err)
	}
}

func TestBuildDeduplicatesIdenticalSharedImports(t *testing.T) {
	bundle := descriptorBundle(t, "shared.proto", "shared", "Event")
	registry, err := Build([]Module{
		{Path: "one", Version: "v1", DescriptorSet: bundle},
		{Path: "two", Version: "v2", DescriptorSet: bundle},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(registry.List()) != 1 {
		t.Fatalf("duplicate descriptor produced %d types", len(registry.List()))
	}
}

func TestDigestIsStable(t *testing.T) {
	if got := Digest([]byte("abc")); got != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("digest = %s", got)
	}
}
