// Package cache is the content-addressed blob store (<root>/blobs plus
// <root>/.hfdl/incomplete staging) and the installer that materializes
// verified blobs into either the huggingface_hub cache layout
// (<type>s--org--name/{refs,snapshots/<sha>} with relative symlinks) or a
// local directory (reflink else copy, temp file + atomic rename, plus
// .cache/huggingface/download metadata stamps). Blobs are never moved out
// of the cache; namespace durability is file fsync → link/rename → parent
// directory fsync at every step.
package cache
