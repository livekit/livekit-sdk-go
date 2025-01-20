// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jitter

import (
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/pion/rtp"

	"github.com/livekit/protocol/logger"
)

type Buffer struct {
	depacketizer    rtp.Depacketizer
	maxLate         uint32
	clockRate       uint32
	onPacketDropped func()
	packetsDropped  int
	packetsTotal    int
	logger          logger.Logger

	mu          sync.Mutex
	pool        *packet
	size        int
	initialized bool
	prevSN      uint16
	head        *packet
	tail        *packet

	maxSampleSize uint32
	minTS         uint32
}

type packet struct {
	prev, next     *packet
	start, end     bool
	reset, padding bool
	packet         *rtp.Packet
}

func NewBuffer(depacketizer rtp.Depacketizer, clockRate uint32, maxLatency time.Duration, opts ...Option) *Buffer {
	b := &Buffer{
		depacketizer: depacketizer,
		maxLate:      uint32(float64(maxLatency) / float64(time.Second) * float64(clockRate)),
		clockRate:    clockRate,
		logger:       logger.LogRLogger(logr.Discard()),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *Buffer) UpdateMaxLatency(maxLatency time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()

	maxLate := uint32(float64(maxLatency) / float64(time.Second) * float64(b.clockRate))
	b.minTS += b.maxLate - maxLate
	b.maxLate = maxLate
}

func (b *Buffer) Push(pkt *rtp.Packet) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.packetsTotal++
	var start, end, padding bool
	if len(pkt.Payload) == 0 {
		// drop padding packets from the beginning of the stream
		if !b.initialized {
			return
		}
		start = true
		end = true
		padding = true
	} else {
		start = b.depacketizer.IsPartitionHead(pkt.Payload)
		end = b.depacketizer.IsPartitionTail(pkt.Marker, pkt.Payload)
	}

	p := b.newPacket(start, end, padding, pkt)

	beforePrev := before16(pkt.SequenceNumber, b.prevSN)
	outsidePrevRange := outsideRange(pkt.SequenceNumber, b.prevSN)

	if !b.initialized {
		if p.start {
			for {
				if b.head != nil && before16(b.head.packet.SequenceNumber, pkt.SequenceNumber) {
					// drop packet
					b.head = b.head.next
					if b.head == nil {
						b.tail = nil
					}
				} else {
					break
				}
			}

			// initialize on the first start packet
			b.initialized = true
			b.prevSN = pkt.SequenceNumber - 1
			b.minTS = pkt.Timestamp - b.maxLate
			p.reset = true
		} else {
			// drop the buffer
			return
		}
	} else if beforePrev && !outsidePrevRange {
		// drop if packet comes before previously pushed packet
		if !p.padding {
			b.packetsDropped++
			if b.onPacketDropped != nil {
				b.onPacketDropped()
			}
		}
		return
	}

	if b.tail == nil {
		if !p.reset {
			p.reset = p.start && outsidePrevRange
		}
		b.minTS = pkt.Timestamp - b.maxLate
		b.head = p
		b.tail = p
		return
	}

	beforeHead := before16(pkt.SequenceNumber, b.head.packet.SequenceNumber)
	beforeTail := before16(pkt.SequenceNumber, b.tail.packet.SequenceNumber)
	outsideHeadRange := outsideRange(pkt.SequenceNumber, b.head.packet.SequenceNumber)
	outsideTailRange := outsideRange(pkt.SequenceNumber, b.tail.packet.SequenceNumber)

	switch {
	case !beforeTail && !outsideTailRange:
		// append (within range)
		b.minTS += pkt.Timestamp - b.tail.packet.Timestamp
		if pkt.SequenceNumber == b.tail.packet.SequenceNumber+1 {
			if f := pkt.Timestamp - b.tail.packet.Timestamp; f > b.maxSampleSize {
				b.maxSampleSize = f
			}
		}
		p.prev = b.tail
		b.tail.next = p
		b.tail = p

	case outsideHeadRange && outsideTailRange:
		// append (reset)
		p.reset = p.start
		b.minTS += b.maxSampleSize
		p.prev = b.tail
		b.tail.next = p
		b.tail = p

	case beforeHead && !outsideHeadRange:
		// prepend (within range)
		p.reset = p.start && outsidePrevRange
		b.head.prev = p
		p.next = b.head
		b.head = p

	case outsideTailRange:
		// insert (within head range)
		for c := b.tail.prev; c != nil; c = c.prev {
			if before16(pkt.SequenceNumber, c.packet.SequenceNumber) || outsideRange(pkt.SequenceNumber, c.packet.SequenceNumber) {
				continue
			}

			// insert after c
			if pkt.SequenceNumber == c.packet.SequenceNumber+1 {
				if f := pkt.Timestamp - c.packet.Timestamp; f > b.maxSampleSize {
					b.maxSampleSize = f
				}
			}
			c.next.prev = p
			p.next = c.next
			p.prev = c
			c.next = p
			break
		}

	default:
		// insert (within tail range)
		for c := b.tail.prev; c != nil; c = c.prev {
			outsideCRange := outsideRange(pkt.SequenceNumber, c.packet.SequenceNumber)
			if before16(pkt.SequenceNumber, c.packet.SequenceNumber) && !outsideCRange {
				continue
			}

			// insert after c
			if p.start && outsideCRange {
				p.reset = true
			} else if pkt.SequenceNumber == c.packet.SequenceNumber+1 {
				if f := pkt.Timestamp - c.packet.Timestamp; f > b.maxSampleSize {
					b.maxSampleSize = f
				}
			}
			c.next.prev = p
			p.next = c.next
			p.prev = c
			c.next = p
			break
		}
	}
}

func (b *Buffer) Pop(force bool) []*rtp.Packet {
	b.mu.Lock()
	defer b.mu.Unlock()

	if force {
		return b.forcePop()
	} else {
		return b.pop()
	}
}

func (b *Buffer) PopSamples(force bool) [][]*rtp.Packet {
	if force {
		return b.forcePopSamples()
	} else {
		return b.popSamples()
	}
}

func (b *Buffer) PacketLoss() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	return float64(b.packetsDropped) / float64(b.packetsTotal)
}

func (b *Buffer) forcePop() []*rtp.Packet {
	packets := make([]*rtp.Packet, 0, b.size)

	var next *packet
	for c := b.head; c != nil; c = next {
		next = c.next
		packets = append(packets, c.packet)
		b.free(c)
	}

	b.head = nil
	b.tail = nil
	return packets
}

func (b *Buffer) forcePopSamples() [][]*rtp.Packet {
	packets := make([][]*rtp.Packet, 0)
	sample := make([]*rtp.Packet, 0)

	var next *packet
	for c := b.head; c != nil; c = next {
		next = c.next
		if c.start && len(sample) > 0 {
			packets = append(packets, sample)
			sample = make([]*rtp.Packet, 0)
		}
		sample = append(sample, c.packet)
		if c.end {
			packets = append(packets, sample)
			sample = make([]*rtp.Packet, 0)
		}
		b.free(c)
	}

	b.head = nil
	b.tail = nil
	return packets
}

func (b *Buffer) pop() []*rtp.Packet {
	if !b.initialized {
		return nil
	}

	b.drop()
	if b.head == nil || !b.head.start {
		return nil
	}

	end := b.getEnd()
	if end == nil {
		return nil
	}

	packets := make([]*rtp.Packet, 0, b.size)
	var next *packet
	for c := b.head; ; c = next {
		next = c.next
		if !c.padding {
			packets = append(packets, c.packet)
		}
		if next != nil {
			if outsideRange(next.packet.SequenceNumber, c.packet.SequenceNumber) {
				// adjust minTS to account for sequence number reset
				b.minTS += next.packet.Timestamp - c.packet.Timestamp - b.maxSampleSize
			}
			next.prev = nil
		}
		if c == end {
			b.prevSN = c.packet.SequenceNumber
			b.head = next
			if next == nil {
				b.tail = nil
			}
			b.free(c)
			return packets
		}
		b.free(c)
	}
}

func (b *Buffer) popSamples() [][]*rtp.Packet {
	if !b.initialized {
		return nil
	}

	b.drop()
	if b.head == nil || !b.head.start {
		return nil
	}

	end := b.getEnd()
	if end == nil {
		return nil
	}

	packets := make([][]*rtp.Packet, 0)
	sample := make([]*rtp.Packet, 0)
	var next *packet
	for c := b.head; ; c = next {
		next = c.next
		if !c.padding {
			sample = append(sample, c.packet)
		}
		if next != nil {
			if outsideRange(next.packet.SequenceNumber, c.packet.SequenceNumber) {
				// adjust minTS to account for sequence number reset
				b.minTS += next.packet.Timestamp - c.packet.Timestamp - b.maxSampleSize
			}
			next.prev = nil
		}
		if c.end {
			packets = append(packets, sample)
			sample = make([]*rtp.Packet, 0)
		}
		if c == end {
			b.prevSN = c.packet.SequenceNumber
			b.head = next
			if next == nil {
				b.tail = nil
			}
			b.free(c)
			return packets
		}
		b.free(c)
	}
}

func (b *Buffer) getEnd() *packet {
	prevSN := b.prevSN
	prevComplete := true
	var end *packet
	for c := b.head; c != nil; c = c.next {
		// sequence number must be next
		if c.packet.SequenceNumber != prevSN+1 &&
			// or a reset which is about to reach its time limit
			(!prevComplete || !c.reset || !before32(c.packet.Timestamp-b.maxSampleSize, b.minTS)) {
			break
		}
		if prevComplete {
			prevComplete = false
		}
		if c.end {
			end = c
			prevComplete = true
		}
		prevSN = c.packet.SequenceNumber
	}

	return end
}

func (b *Buffer) drop() {
	if b.head == nil {
		return
	}

	dropped := false

	if b.head.packet.SequenceNumber != b.prevSN+1 &&
		(b.head.start && before32(b.head.packet.Timestamp-b.maxSampleSize, b.minTS) ||
			!b.head.start && before32(b.head.packet.Timestamp, b.minTS)) {
		// lost packets will now be too old even if we receive them
		// on sequence number reset, skip callback because we don't know whether we lost any
		if !b.head.reset {
			b.packetsDropped++
			dropped = true
		}

		for b.head != nil && !b.head.start && before32(b.head.packet.Timestamp-b.maxSampleSize, b.minTS) {
			dropped = true
			b.packetsDropped++
			b.prevSN = b.head.packet.SequenceNumber - 1
			b.dropHead()
		}

		if b.head != nil {
			b.prevSN = b.head.packet.SequenceNumber - 1
		}
	}

	for c := b.head; c != nil; {
		// check if timestamp >= than minimum
		if (c.start && before32(b.minTS, c.packet.Timestamp)) || (!c.start && !before32(c.packet.Timestamp, b.minTS)) {
			break
		}

		// drop all packets within this sample
		dropped = true
		count := 0
		ts := c.packet.Timestamp
		for {
			b.packetsDropped++
			count++
			b.dropHead()
			c = b.head

			// break if head is part of a new sample
			if b.head == nil || b.head.packet.Timestamp != ts {
				break
			}
		}
	}

	if dropped && b.onPacketDropped != nil {
		b.onPacketDropped()
	}
}

func (b *Buffer) dropHead() {
	c := b.head
	b.prevSN = c.packet.SequenceNumber

	b.head = c.next
	if b.head == nil {
		b.tail = nil
	} else {
		b.head.prev = nil
		if outsideRange(b.head.packet.SequenceNumber, c.packet.SequenceNumber) {
			// adjust minTS to account for sequence number reset
			b.minTS += b.head.packet.Timestamp - c.packet.Timestamp - b.maxSampleSize
		}
	}
	b.free(c)
}

func (b *Buffer) newPacket(start, end, padding bool, pkt *rtp.Packet) *packet {
	b.size++
	if b.pool == nil {
		return &packet{
			start:   start,
			end:     end,
			padding: padding,
			packet:  pkt,
		}
	}

	p := b.pool
	b.pool = p.next
	p.next = nil
	p.start = start
	p.end = end
	p.padding = padding
	p.packet = pkt
	return p
}

func (b *Buffer) free(pkt *packet) {
	b.size--
	pkt.prev = nil
	pkt.packet = nil
	pkt.next = b.pool
	b.pool = pkt
}

func before16(a, b uint16) bool {
	return (b-a)&0x8000 == 0
}

func before32(a, b uint32) bool {
	return (b-a)&0x80000000 == 0
}

func outsideRange(a, b uint16) bool {
	return a-b > 3000 && b-a > 3000
}
