package stream

import (
	"go.temporal.io/api/serviceerror"
	streampb "go.temporal.io/api/stream/v1"
	streamlib "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// AtOffset starts at an absolute offset.
func AtOffset(offset int64) *streampb.StreamStartPosition {
	return &streampb.StreamStartPosition{
		Position: &streampb.StreamStartPosition_Offset{Offset: offset},
	}
}

// Earliest starts at the oldest record the stream still holds.
func Earliest() *streampb.StreamStartPosition {
	return &streampb.StreamStartPosition{
		Position: &streampb.StreamStartPosition_Earliest{Earliest: true},
	}
}

// Tail starts at the head, so only records appended afterwards are read.
func Tail() *streampb.StreamStartPosition {
	return &streampb.StreamStartPosition{
		Position: &streampb.StreamStartPosition_Tail{Tail: true},
	}
}

// LastN starts n records before the head, or at the floor when fewer are held.
func LastN(n int64) *streampb.StreamStartPosition {
	return &streampb.StreamStartPosition{
		Position: &streampb.StreamStartPosition_LastN{LastN: n},
	}
}

// RequestedStart reads where a caller asked to start from the two fields a
// request can say it in. Setting both is refused rather than ranked, because
// the two could disagree and neither is obviously the one the caller meant.
//
// A negative offset once meant the head. It is refused rather than
// reinterpreted, so a caller still sending it finds out instead of reading
// from somewhere it did not ask for.
func RequestedStart(
	pos *streampb.StreamStartPosition, offsetField string, offset int64,
) (*streampb.StreamStartPosition, error) {
	if pos != nil {
		if offset != 0 {
			return nil, serviceerror.NewInvalidArgumentf(
				"set either start_position or %s, not both", offsetField)
		}
		return pos, CheckStart(pos)
	}
	if offset < 0 {
		return nil, serviceerror.NewInvalidArgumentf(
			"%s cannot be negative, got %d: ask for the head with start_position.tail",
			offsetField, offset)
	}
	return AtOffset(offset), nil
}

// CheckStart refuses a position that names nothing the server can resolve.
func CheckStart(pos *streampb.StreamStartPosition) error {
	switch p := pos.GetPosition().(type) {
	case *streampb.StreamStartPosition_Offset:
		if p.Offset < 0 {
			return serviceerror.NewInvalidArgumentf(
				"start_position.offset cannot be negative, got %d", p.Offset)
		}
	case *streampb.StreamStartPosition_LastN:
		if p.LastN <= 0 {
			return serviceerror.NewInvalidArgumentf(
				"start_position.last_n must be positive, got %d", p.LastN)
		}
	case *streampb.StreamStartPosition_Earliest:
		if !p.Earliest {
			return serviceerror.NewInvalidArgument("start_position.earliest must be true when set")
		}
	case *streampb.StreamStartPosition_Tail:
		if !p.Tail {
			return serviceerror.NewInvalidArgument("start_position.tail must be true when set")
		}
	default:
		return serviceerror.NewInvalidArgument("start_position names no position")
	}
	return nil
}

func (s *Stream) resolveStart(pos *streampb.StreamStartPosition) (int64, error) {
	return ResolveStart(pos, s.State)
}

// ResolveStart turns a position into the absolute offset it names against a
// stream's frontier. The caller checks the result against the floor and head
// in whatever terms its request uses.
func ResolveStart(pos *streampb.StreamStartPosition, state *streamlib.StreamState) (int64, error) {
	if err := CheckStart(pos); err != nil {
		return 0, err
	}
	base, head := state.GetBaseOffset(), state.GetHeadOffset()
	switch p := pos.GetPosition().(type) {
	case *streampb.StreamStartPosition_Offset:
		return p.Offset, nil
	case *streampb.StreamStartPosition_LastN:
		// N counts records of every kind, because the server does not decode
		// them to tell data from a producer's finish.
		return max(base, head-p.LastN), nil
	case *streampb.StreamStartPosition_Earliest:
		return base, nil
	default:
		return head, nil
	}
}
