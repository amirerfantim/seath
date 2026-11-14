//go:build !linux

package sysstat

// diskUsage is only implemented on Linux (the deploy target). On other
// platforms it reports "unsupported" so the rest of the sample still renders.
func diskUsage(string) (total, free int64, ok bool) {
	return 0, 0, false
}
