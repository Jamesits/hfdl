//go:build !linux && !windows

package fcio

// Engine opens files on the requested IO tier and owns per-volume capability
// decisions (in-memory over the injected CapsCache). macOS (F_NOCACHE) and the
// plain fallback carry no per-volume state, so this build adds no fields beyond
// the common ones.
type Engine struct {
	engineCommon
}
