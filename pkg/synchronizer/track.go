package synchronizer

import (
	"io"
	"math"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3"

	"github.com/livekit/mediatransportutil"
)

const (
	maxSNDropout         = 3000 // max sequence number skip
	uint32Overflow int64 = 4294967296
)

type TrackRemote interface {
	ID() string
	Codec() webrtc.RTPCodecParameters
	Kind() webrtc.RTPCodecType
	SSRC() webrtc.SSRC
}

type TrackSynchronizer struct {
	sync.Mutex
	sync *Synchronizer

	// track info
	trackID              string
	rtpDuration          float64 // duration in ns per unit RTP time
	frameDuration        int64   // frame duration in RTP time
	defaultFrameDuration int64   // used if no value has been recorded

	// timing info
	startedAt int64         // starting time in unix nanoseconds
	firstTS   int64         // first RTP timestamp received
	maxPTS    time.Duration // maximum valid PTS (set after EOS)

	// previous packet info
	lastSN    uint16        // previous sequence number
	lastTS    int64         // previous RTP timestamp
	lastPTS   time.Duration // previous presentation timestamp
	lastValid bool          // previous packet did not cause a reset
	inserted  int64         // number of frames inserted

	// offsets
	snOffset  uint16 // sequence number offset (increases with each blank frame inserted
	ptsOffset int64  // presentation timestamp offset (used for a/v sync)

	lastPTSDrift time.Duration // track massive PTS drift, in case it's correct
}

func newTrackSynchronizer(s *Synchronizer, track TrackRemote) *TrackSynchronizer {
	clockRate := float64(track.Codec().ClockRate)

	t := &TrackSynchronizer{
		trackID:     track.ID(),
		sync:        s,
		rtpDuration: float64(1000000000) / clockRate,
	}

	switch track.Kind() {
	case webrtc.RTPCodecTypeAudio:
		// opus default frame size is 20ms
		t.defaultFrameDuration = int64(clockRate / 50)
	default:
		// use 24 fps for video
		t.defaultFrameDuration = int64(clockRate / 24)
	}

	return t
}

// Initialize should be called as soon as the first packet is received
func (t *TrackSynchronizer) Initialize(pkt *rtp.Packet) {
	now := time.Now().UnixNano()
	startedAt := t.sync.getOrSetStartedAt(now)

	t.Lock()
	t.startedAt = now
	t.firstTS = int64(pkt.Timestamp)
	t.ptsOffset = now - startedAt
	t.Unlock()
}

// GetPTS will reset sequence numbers and/or offsets if necessary
// Packets are expected to be in order
func (t *TrackSynchronizer) GetPTS(pkt *rtp.Packet) (time.Duration, error) {
	t.Lock()
	defer t.Unlock()

	ts, pts, valid := t.adjust(pkt)
	t.inserted = 0

	// update frame duration if this is a new frame and both packets are valid
	if valid && t.lastValid && pkt.SequenceNumber == t.lastSN+1 {
		t.frameDuration = ts - t.lastTS
	}

	// if past end time, return EOF
	if t.maxPTS > 0 && (pts > t.maxPTS || !valid) {
		return 0, io.EOF
	}

	// update previous values
	t.lastTS = ts
	t.lastSN = pkt.SequenceNumber
	t.lastPTS = pts
	t.lastValid = valid

	return pts, nil
}

// adjust accounts for uint32 overflow, and will reset sequence numbers or rtp time if necessary
func (t *TrackSynchronizer) adjust(pkt *rtp.Packet) (int64, time.Duration, bool) {
	// adjust sequence number and reset if needed
	pkt.SequenceNumber += t.snOffset
	if t.lastTS != 0 &&
		pkt.SequenceNumber-t.lastSN > maxSNDropout &&
		t.lastSN-pkt.SequenceNumber > maxSNDropout {

		// reset sequence numbers
		t.snOffset += t.lastSN + 1 - pkt.SequenceNumber
		pkt.SequenceNumber = t.lastSN + 1

		// reset RTP timestamps
		frameDuration := t.getFrameDurationRTP()
		tsOffset := (t.inserted + 1) * frameDuration
		ts := t.lastTS + tsOffset
		t.firstTS += int64(pkt.Timestamp) - ts

		pts := t.lastPTS + time.Duration(math.Round(float64(tsOffset)*t.rtpDuration))
		return ts, pts, false
	}

	// adjust timestamp for uint32 wrap
	ts := int64(pkt.Timestamp)
	for ts < t.lastTS {
		ts += uint32Overflow
	}

	// use the previous pts if this packet has the same timestamp
	if ts == t.lastTS {
		return ts, t.lastPTS, t.lastValid
	}

	return ts, time.Duration(t.getElapsed(ts) + t.ptsOffset), true
}

func (t *TrackSynchronizer) getElapsed(ts int64) int64 {
	return int64(math.Round(float64(ts-t.firstTS) * t.rtpDuration))
}

// InsertFrame is used to inject frames (usually blank) into the stream
// It updates the timestamp and sequence number of the packet, as well as offsets for all future packets
func (t *TrackSynchronizer) InsertFrame(pkt *rtp.Packet) time.Duration {
	t.Lock()
	defer t.Unlock()

	pts, _ := t.insertFrameBefore(pkt, nil)
	return pts
}

// InsertFrameBefore updates the packet and offsets only if it is at least one frame duration before next
func (t *TrackSynchronizer) InsertFrameBefore(pkt *rtp.Packet, next *rtp.Packet) (time.Duration, bool) {
	t.Lock()
	defer t.Unlock()

	return t.insertFrameBefore(pkt, next)
}

func (t *TrackSynchronizer) insertFrameBefore(pkt *rtp.Packet, next *rtp.Packet) (time.Duration, bool) {
	t.inserted++
	t.snOffset++
	t.lastValid = false

	frameDurationRTP := t.getFrameDurationRTP()

	ts := t.lastTS + (t.inserted * frameDurationRTP)
	if next != nil {
		nextTS, _, _ := t.adjust(next)
		if ts+frameDurationRTP > nextTS {
			// too long, drop
			return 0, false
		}
	}

	// update packet
	pkt.SequenceNumber = t.lastSN + uint16(t.inserted)
	pkt.Timestamp = uint32(ts)

	pts := t.lastPTS + time.Duration(math.Round(float64(frameDurationRTP)*t.rtpDuration*float64(t.inserted)))
	return pts, true
}

// GetFrameDuration returns frame duration in seconds
func (t *TrackSynchronizer) GetFrameDuration() time.Duration {
	t.Lock()
	defer t.Unlock()

	frameDurationRTP := t.getFrameDurationRTP()
	return time.Duration(math.Round(float64(frameDurationRTP) * t.rtpDuration))
}

// getFrameDurationRTP returns frame duration in RTP time
func (t *TrackSynchronizer) getFrameDurationRTP() int64 {
	if t.frameDuration != 0 {
		return t.frameDuration
	}

	return t.defaultFrameDuration
}

func (t *TrackSynchronizer) getSenderReportPTS(pkt *rtcp.SenderReport) (time.Duration, bool) {
	t.Lock()
	defer t.Unlock()

	ts := int64(pkt.RTPTime)
	for ts < t.lastTS-(uint32Overflow/2) {
		ts += uint32Overflow
	}

	elapsed := t.getElapsed(ts)
	expected := time.Now().UnixNano() - t.startedAt

	return time.Duration(elapsed + t.ptsOffset), inDelta(elapsed, expected, 1e9)
}

// onSenderReport handles pts adjustments for a track
func (t *TrackSynchronizer) onSenderReport(pkt *rtcp.SenderReport, pts time.Duration, ntpStart time.Time) {
	t.Lock()
	defer t.Unlock()

	expected := mediatransportutil.NtpTime(pkt.NTPTime).Time().Sub(ntpStart)
	if pts != expected {
		drift := expected - pts
		// if absGreater(drift, largePTSDrift) {
		// 	logger.Warnw("high pts drift", nil, "trackID", t.trackID, "pts", pts, "drift", drift)
		// 	if absGreater(drift, massivePTSDrift) {
		// 		if t.lastPTSDrift == 0 || absGreater(drift-t.lastPTSDrift, largePTSDrift) {
		// 			t.lastPTSDrift = drift
		// 			return
		// 		}
		// 	}
		// }

		t.ptsOffset += int64(drift)
	}
}

func absGreater(a, b time.Duration) bool {
	return a > b || a < -b
}

func inDelta(a, b, delta int64) bool {
	if a > b {
		return a-b <= delta
	}
	return b-a <= delta
}
