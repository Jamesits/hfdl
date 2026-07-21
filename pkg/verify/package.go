// Package verify guarantees blob integrity and density:
// DeSparse makes the cache file physically dense (fallocate, else a
// SEEK_DATA/SEEK_HOLE zero-fill walk) and Hash/Verify check the final
// physical layout — plain sha256 for LFS files, git blob sha1
// ("blob {size}\x00"+content) for regular files. All IO goes through
// pkg/fcio under the disk DutyLimiter.
package verify
