package event

import (
	"fmt"
	"time"

	"github.com/FrameworkOSS/feature_wires/wire"
	"github.com/JoshuaDoes/crunchio"
)

func NewEvent() *Event {
	e := new(Event)
	e.SetEpochMilliNow()
	return e
}

/*	---
	--- INTERNAL EVENTS ---
	---
*/

func NewEventSuccess(feature string) *Event {
	return NewEvent().
		SetID("success").
		SetProducer(feature)
}

func NewEventResponse(feature string, data []byte) *Event {
	return NewEvent().
		SetID("resp").
		SetProducer(feature).
		SetData(data)
}

func NewEventBatch(feature string, events ...*Event) (e *Event) {
	e = NewEvent().
		SetID("batch").
		SetProducer(feature)

	offsets := make([]uint64, len(events))
	offsets[0] = 0

	data := make([]byte, 0)
	for i := 0; i < len(events); i++ {
		if i > 0 {
			offsets[i] = uint64(len(data))
		}
		data = append(data, e.Bytes()...)
	}

	e.SetData(data)
	e.SetOffsets(offsets...)

	return
}

func NewEventReady(feature string, ready bool) (e *Event) {
	e = NewEvent().
		SetID("ready").
		SetProducer(feature)

	if ready {
		e.SetData([]byte{1})
	} else {
		e.SetData([]byte{0})
	}
	e.SetOffsets(0)

	return
}

func NewEventError(feature string, err error) *Event {
	return NewEvent().
		SetID("error").
		SetProducer(feature).
		SetData([]byte(err.Error()))
}

func NewEventBytes(p []byte) *Event {
	if len(p) > 0 {
		w := wire.NewWire(p)
		defer w.Close()

		portal := w.GetValues(0)
		id := w.GetValues(1)
		channel := w.GetValues(2)
		producer := w.GetValues(3)
		participants := w.GetValues(4)
		offsets := w.GetValues(5)
		data := w.GetValues(6)
		epochMilli := w.GetValues(7)

		e := NewEvent()
		e.SetPortal(string(portal[0]))
		fmt.Println("Portal:", e.GetPortal())
		e.SetID(string(id[0]))
		fmt.Println("ID:", e.GetID())
		e.SetChannel(string(channel[0]))
		fmt.Println("Channel:", e.GetChannel())
		e.SetProducer(string(producer[0]))
		fmt.Println("Producer:", e.GetProducer())
		e.SetData(data[0])

		for i := 0; i < len(participants); i++ {
			e.AddParticipants(string(participants[i]))
			fmt.Println("Participants:", e.GetParticipants())
		}

		offs := make([]uint64, len(offsets))
		for i := 0; i < len(offsets); i++ {
			offset := crunchio.NewBuffer("", offsets[i])
			offs = append(offs, offset.Buffer().ReadU64LE(0, 1)...)
			offset.Close()
		}
		e.SetOffsets(offs...)
		fmt.Println("Offsets:", e.GetOffsets())

		if len(epochMilli) > 0 {
			epoch := crunchio.NewBuffer("", epochMilli[0])
			e.SetEpochMilli(epoch.Buffer().ReadU64LE(0, 1)[0])
			fmt.Println("Time:", e.GetEpochMilli())
			epoch.Close()
		}

		return e
	}
	return nil
}

/*	---
	--- GETTERS ---
	---
*/

func (e *Event) Bytes() []byte {
	w := wire.NewWire()
	defer w.Close()

	w.AddField(0, []byte(e.GetPortal()))
	w.AddField(1, []byte(e.GetID()))
	w.AddField(2, []byte(e.GetChannel()))
	w.AddField(3, []byte(e.GetProducer()))

	participants := e.GetParticipants()
	for i := 0; i < len(participants); i++ {
		w.AddField(4, []byte(participants[i]))
	}

	offsets := e.GetOffsets()
	for i := 0; i < len(offsets); i++ {
		offset := crunchio.NewBuffer("", make([]byte, 8))
		offset.Buffer().WriteU64LE(0, []uint64{offsets[i]})
		w.AddField(5, offset.Bytes())
		offset.Close()
	}

	data := e.GetData()
	w.AddField(6, data)

	if epochMilli := e.GetEpochMilli(); epochMilli > 0 {
		epoch := crunchio.NewBuffer("", make([]byte, 8))
		epoch.Buffer().WriteU64LE(0, []uint64{epochMilli})
		w.AddField(7, epoch.Bytes())
		epoch.Close()
	}

	return w.Bytes()
}

func (e *Event) GetPortal() string {
	return e.portal
}

func (e *Event) GetID() string {
	return e.id
}

func (e *Event) GetProducer() string {
	return e.producer
}

func (e *Event) GetChannel() string {
	return e.channel
}

func (e *Event) GetParticipants(exclude ...string) []string {
	participants := make([]string, 0)
	for i := 0; i < len(e.participants); i++ {
		excluded := false
		for j := 0; j < len(exclude); j++ {
			if e.participants[i] == exclude[j] {
				excluded = true
				break
			}
		}
		if !excluded {
			participants = append(participants, e.participants[i])
		}
	}
	return participants
}

func (e *Event) IsParticipant(participant string) bool {
	for i := 0; i < len(e.participants); i++ {
		if e.participants[i] == participant {
			return true
		}
	}
	return false
}

func (e *Event) GetOffsets() []uint64 {
	return e.offsets
}

func (e *Event) GetData() []byte {
	return e.data
}

func (e *Event) GetDataSize() int {
	return len(e.data)
}

func (e *Event) GetArgument(i int) []byte {
	if len(e.offsets) == 0 || i >= len(e.offsets) || len(e.data) == 0 {
		return nil
	}
	if i == len(e.offsets)-1 {
		d := e.data[e.offsets[i]:]
		//fmt.Printf("D: %X\n", d)
		//fmt.Printf("B: %X\n", e.data)
		return d
	}
	return e.data[e.offsets[i]:e.offsets[i+1]]
}

func (e *Event) GetEpochMilli() uint64 {
	return e.epochMilli
}

/*	---
	--- SETTERS ---
	---
*/

func (e *Event) SetPortal(portal string) *Event {
	e.portal = portal
	return e
}

func (e *Event) SetID(id string) *Event {
	e.id = id
	return e
}

func (e *Event) SetProducer(producer string) *Event {
	e.producer = producer
	return e
}

func (e *Event) SetChannel(channel string) *Event {
	e.channel = channel
	return e
}

func (e *Event) AddParticipants(participants ...string) *Event {
	for i := 0; i < len(participants); i++ {
		if !e.IsParticipant(participants[i]) {
			e.participants = append(e.participants, participants[i])
		}
	}
	return e
}

func (e *Event) SetParticipants(participants ...string) *Event {
	e.participants = participants
	return e
}

func (e *Event) SetOffsets(offsets ...uint64) *Event {
	e.offsets = offsets
	return e
}

// AddOffsetNext creates a new offset entry using the current length of data.
func (e *Event) AddOffsetNext() *Event {
	offsets := e.GetOffsets()
	size := e.GetDataSize()

	if len(offsets) == 0 {
		e.SetOffsets(uint64(size))
		return e
	}

	if offsets[len(offsets)-1] == uint64(size) {
		return e
	}

	offsets = append(offsets, uint64(size))
	return e.SetOffsets(offsets...)
}

func (e *Event) SetData(data []byte) *Event {
	e.data = data
	return e
}

func (e *Event) AddDataNext(data []byte) *Event {
	if e.data == nil {
		e.data = data
		return e
	}

	e.data = append(e.data, data...)
	return e
}

func (e *Event) AddStringNext(str string) *Event {
	return e.AddDataNext([]byte(str))
}

func (e *Event) SetEpochMilli(epochMilli uint64) *Event {
	e.epochMilli = epochMilli
	return e
}

func (e *Event) SetEpochMilliNow() *Event {
	e.epochMilli = uint64(time.Now().UnixMilli())
	return e
}
