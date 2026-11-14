// Package natspreset defines the named NATS environments the UI can record from.
// Credentials never leave the server: the browser only ever sends a preset name.
package natspreset

import "github.com/nats-io/nats.go"

// Preset is a named NATS environment. Username/Password/Token are tagged
// json:"-" so they are never serialized to API responses.
type Preset struct {
	Name     string `json:"name"`
	URL      string `json:"url"`
	Username string `json:"-"`
	Password string `json:"-"`
	Token    string `json:"-"`
}

// BuildOptions returns the nats.Option list implied by a preset's credentials.
// Mirrors the auth handling previously inlined in recorder.Run.
func BuildOptions(p Preset) []nats.Option {
	var opts []nats.Option
	if p.Token != "" {
		opts = append(opts, nats.Token(p.Token))
	}
	if p.Username != "" {
		opts = append(opts, nats.UserInfo(p.Username, p.Password))
	}
	return opts
}
