package fcio

import (
	"context"
	"sync"
)

// VolumeID identifies a whole physical device (spindle). On Linux it is the
// whole disk's dev "major:minor" resolved from st_dev via /sys/dev/block, so
// two partitions of one disk share an id.
type VolumeID string

// VolumeSet is the FastCopy driveMng.OccupancyDrives analog:
// read vs mixed-RW work must not share an HDD spindle; SSDs overlap at
// reduced depth. This is the congestion rule, not a hint.
type VolumeSet struct {
	mu   sync.Mutex
	vols map[VolumeID]*volOccupancy
}

// volOccupancy tracks holders of one volume. wake is closed and replaced on
// every state change, giving a ctx-cancelable condition variable.
type volOccupancy struct {
	readers int
	rw      bool
	wake    chan struct{}
}

// Occupancy rule, per volume:
//   - HDD / NetFS / Unknown (conservative): readers overlap freely; an RW
//     job is exclusive against readers and other RW jobs.
//   - SSD: at most two holders, at most one of them RW — i.e. two readers,
//     or one RW plus one reader.
func grantRead(fs FsType, o *volOccupancy) bool {
	if fs == FsSSD {
		total := o.readers
		if o.rw {
			total++
		}
		return total < 2
	}
	return !o.rw
}

func grantWrite(fs FsType, o *volOccupancy) bool {
	if fs == FsSSD {
		return !o.rw && o.readers < 2
	}
	return !o.rw && o.readers == 0
}

// NewVolumeSet builds an empty lock table.
func NewVolumeSet() *VolumeSet {
	return &VolumeSet{vols: map[VolumeID]*volOccupancy{}}
}

// VolumeOf maps a path (which need not exist yet) to its volume.
func (v *VolumeSet) VolumeOf(ctx context.Context, path string) (VolumeID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return statVolumeID(path)
}

// AcquireRead blocks until a read slot on id is granted or ctx is done. The
// returned func releases the slot and is idempotent.
func (v *VolumeSet) AcquireRead(ctx context.Context, id VolumeID, fs FsType) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		v.mu.Lock()
		o := v.occupancy(id)
		if grantRead(fs, o) {
			o.readers++
			v.mu.Unlock()
			var once sync.Once
			return func() { once.Do(func() { v.release(id, false) }) }, nil
		}
		wake := o.wake
		v.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// AcquireRW locks a mixed read+write job: an RW slot on every volume the
// job touches — the lock covers both the read and the write volume,
// taken atomically so there are no lock-ordering hazards. On HDD
// that serializes the job against every other holder on either spindle;
// on SSD each volume still admits one extra reader beside the RW holder.
// read == write takes a single RW slot. Blocks until granted or ctx is
// done; the returned func releases all slots and is idempotent.
func (v *VolumeSet) AcquireRW(ctx context.Context, read, write VolumeID, fs FsType) (func(), error) {
	same := read == write
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		v.mu.Lock()
		ro := v.occupancy(read)
		wo := ro
		if !same {
			wo = v.occupancy(write)
		}
		if grantWrite(fs, wo) && (same || grantWrite(fs, ro)) {
			ro.rw = true
			if !same {
				wo.rw = true
			}
			v.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					v.release(read, true)
					if !same {
						v.release(write, true)
					}
				})
			}, nil
		}
		rwake, wwake := ro.wake, wo.wake
		v.mu.Unlock()
		select {
		case <-rwake:
		case <-wwake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (v *VolumeSet) occupancy(id VolumeID) *volOccupancy {
	o, ok := v.vols[id]
	if !ok {
		o = &volOccupancy{wake: make(chan struct{})}
		v.vols[id] = o
	}
	return o
}

func (v *VolumeSet) release(id VolumeID, rw bool) {
	v.mu.Lock()
	if o, ok := v.vols[id]; ok {
		if rw {
			o.rw = false
		} else {
			o.readers--
		}
		close(o.wake)
		o.wake = make(chan struct{})
		if o.readers == 0 && !o.rw {
			delete(v.vols, id)
		}
	}
	v.mu.Unlock()
}
