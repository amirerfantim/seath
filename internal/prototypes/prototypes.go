// Package prototypes owns Seath's runtime protobuf catalogue.
//
// A catalogue is assembled from versioned FileDescriptorSet bundles stored in
// Postgres. No generated application proto package is linked into Seath.
package prototypes

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const MaxDescriptorSetBytes = 32 << 20

type TypeInfo struct {
	Name   string `json:"name"`
	Fields int    `json:"fields"`
}

// Module is one stored descriptor bundle. DescriptorSet is never returned to
// the browser; it is retained here so a complete candidate registry can be
// validated before activation.
type Module struct {
	ID            int64     `json:"id"`
	Path          string    `json:"path"`
	Version       string    `json:"version"`
	SHA256        string    `json:"sha256"`
	Active        bool      `json:"active"`
	CreatedBy     string    `json:"created_by"`
	CreatedAt     time.Time `json:"created_at"`
	DescriptorSet []byte    `json:"-"`
}

// Registry is immutable. The manager builds a complete candidate before
// swapping it into service, so readers cannot observe a partial catalogue.
type Registry struct {
	types   map[protoreflect.FullName]protoreflect.MessageType
	list    []TypeInfo
	modules []Module
}

func Empty() *Registry {
	return &Registry{types: make(map[protoreflect.FullName]protoreflect.MessageType)}
}

// Build validates and merges active descriptor bundles. Sets must include
// imports. Identical shared imports are deduplicated; conflicts are rejected.
func Build(modules []Module) (*Registry, error) {
	if len(modules) == 0 {
		return Empty(), nil
	}
	filesByPath := make(map[string]*descriptorpb.FileDescriptorProto)
	for _, module := range modules {
		if len(module.DescriptorSet) == 0 {
			return nil, fmt.Errorf("module %s@%s has an empty descriptor set", module.Path, module.Version)
		}
		if len(module.DescriptorSet) > MaxDescriptorSetBytes {
			return nil, fmt.Errorf("module %s@%s descriptor set exceeds %d bytes", module.Path, module.Version, MaxDescriptorSetBytes)
		}
		var set descriptorpb.FileDescriptorSet
		if err := proto.Unmarshal(module.DescriptorSet, &set); err != nil {
			return nil, fmt.Errorf("decode %s@%s descriptor set: %w", module.Path, module.Version, err)
		}
		if len(set.File) == 0 {
			return nil, fmt.Errorf("module %s@%s descriptor set contains no files", module.Path, module.Version)
		}
		for _, file := range set.File {
			if file.GetName() == "" {
				return nil, fmt.Errorf("module %s@%s contains a descriptor with no file name", module.Path, module.Version)
			}
			if previous, ok := filesByPath[file.GetName()]; ok {
				if !proto.Equal(previous, file) {
					return nil, fmt.Errorf("descriptor file %q conflicts between active modules", file.GetName())
				}
				continue
			}
			filesByPath[file.GetName()] = file
		}
	}

	paths := make([]string, 0, len(filesByPath))
	for path := range filesByPath {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	merged := &descriptorpb.FileDescriptorSet{File: make([]*descriptorpb.FileDescriptorProto, 0, len(paths))}
	for _, path := range paths {
		merged.File = append(merged.File, filesByPath[path])
	}
	files, err := protodesc.NewFiles(merged)
	if err != nil {
		return nil, fmt.Errorf("link descriptor imports: %w", err)
	}

	registry := Empty()
	registry.modules = publicModules(modules)
	files.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		collectMessages(registry.types, file.Messages())
		return true
	})
	registry.list = typeList(registry.types)
	if len(registry.list) == 0 {
		return nil, fmt.Errorf("descriptor bundles contain no message types")
	}
	return registry, nil
}

// Synthetic map-entry messages are implementation details, not recordable
// payload types. Real nested messages remain selectable for parity with Go's
// generated protobuf registry.
func collectMessages(dst map[protoreflect.FullName]protoreflect.MessageType, messages protoreflect.MessageDescriptors) {
	for i := 0; i < messages.Len(); i++ {
		descriptor := messages.Get(i)
		if !descriptor.IsMapEntry() {
			dst[descriptor.FullName()] = dynamicpb.NewMessageType(descriptor)
		}
		collectMessages(dst, descriptor.Messages())
	}
}

func typeList(types map[protoreflect.FullName]protoreflect.MessageType) []TypeInfo {
	out := make([]TypeInfo, 0, len(types))
	for name, messageType := range types {
		if strings.HasPrefix(string(name), "google.protobuf.") {
			continue
		}
		out = append(out, TypeInfo{Name: string(name), Fields: messageType.Descriptor().Fields().Len()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func publicModules(modules []Module) []Module {
	out := make([]Module, len(modules))
	copy(out, modules)
	for i := range out {
		out[i].DescriptorSet = nil
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path == out[j].Path {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].Path < out[j].Path
	})
	return out
}

func (r *Registry) List() []TypeInfo {
	if r == nil {
		return nil
	}
	return append([]TypeInfo(nil), r.list...)
}

func (r *Registry) Modules() []Module {
	if r == nil {
		return nil
	}
	return publicModules(r.modules)
}

func (r *Registry) New(fullName string) (proto.Message, error) {
	if r == nil {
		return nil, fmt.Errorf("proto catalogue is not loaded")
	}
	messageType, ok := r.types[protoreflect.FullName(fullName)]
	if !ok {
		return nil, fmt.Errorf("proto type %q is not in the active catalogue", fullName)
	}
	return messageType.New().Interface(), nil
}

func (r *Registry) Exists(fullName string) bool {
	if r == nil {
		return false
	}
	_, ok := r.types[protoreflect.FullName(fullName)]
	return ok
}

func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
