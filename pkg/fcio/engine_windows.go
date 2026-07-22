//go:build windows

package fcio

import "sync"

// Engine opens files on the requested IO tier and owns per-volume capability
// decisions (in-memory over the injected CapsCache). The Windows build adds the
// FILE_FLAG_NO_BUFFERING volume-caps cache.
type Engine struct {
	engineCommon

	// vols memoizes per-volume direct-IO capability probes in memory, keyed
	// "volcaps:<id>" -> *winVolCaps, in front of the injected CapsCache.
	vols sync.Map
}
