//go:build linux

package fcio

import "sync"

// Engine opens files on the requested IO tier and owns per-volume capability
// decisions (in-memory over the injected CapsCache). The Linux build adds the
// O_DIRECT volume-caps cache and the fallocate test seam.
type Engine struct {
	engineCommon

	// vols memoizes per-volume direct-IO capability probes in memory, keyed
	// "volcaps:<dev>" -> *volCaps, in front of the injected CapsCache.
	vols sync.Map

	// fallocateFn overrides the fallocate syscall used by Open; nil uses the
	// OS call. Test seam for the ENOSYS degradation to the no-preallocation
	// tier.
	fallocateFn func(fd int, size int64) error
}
