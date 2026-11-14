//go:build linux

package sysstat

import "syscall"

// diskUsage returns total and free bytes of the filesystem backing path.
func diskUsage(path string) (total, free int64, ok bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, false
	}
	bsize := int64(st.Bsize)
	return int64(st.Blocks) * bsize, int64(st.Bavail) * bsize, true
}
