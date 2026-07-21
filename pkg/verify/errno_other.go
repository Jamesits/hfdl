//go:build !unix

package verify

// On Windows the sparse path is fcio's designed default and its
// fallocate stand-in never reports these POSIX errnos, so the tier-B walk
// trigger never fires here.
func fallocateUnsupported(err error) bool { return false }
