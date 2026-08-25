package matching

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Each started-task response exists twice, once carrying History and once
// carrying raw bytes, so matching can forward a task without deserializing it.
// The two are only interchangeable while every field number means the same
// thing on both sides. That invariant is stated in a proto comment, which is
// easy to satisfy on one copy and forget on the other, and a mismatch stays
// invisible until history.sendRawHistoryBetweenInternalServices is enabled.
func TestStartedResponsePairsStayWireCompatible(t *testing.T) {
	t.Run("history service", func(t *testing.T) {
		// raw_history is a History on one side and repeated bytes on the other,
		// which is the whole point of keeping two messages.
		require.Empty(t, wireCompatDiff(
			&historyservice.RecordWorkflowTaskStartedResponse{},
			&historyservice.RecordWorkflowTaskStartedResponseWithRawHistory{},
			20,
		))
	})

	t.Run("matching service", func(t *testing.T) {
		require.Empty(t, wireCompatDiff(
			&matchingservice.PollWorkflowTaskQueueResponse{},
			&matchingservice.PollWorkflowTaskQueueResponseWithRawHistory{},
			22,
		))
	})
}

// Without a negative control the check above passes just as happily when it has
// stopped inspecting anything. Drop the exemption and the one field the pair is
// allowed to disagree on has to be reported.
func TestWireCompatDiffDetectsAMismatch(t *testing.T) {
	diff := wireCompatDiff(
		&historyservice.RecordWorkflowTaskStartedResponse{},
		&historyservice.RecordWorkflowTaskStartedResponseWithRawHistory{},
	)
	require.Len(t, diff, 1)
	require.Contains(t, diff[0], "field 20")
	require.Contains(t, diff[0], "raw_history")
}

// wireCompatDiff reports every way the two messages would disagree on the wire,
// ignoring the field numbers named in typeMayDiffer.
func wireCompatDiff(
	a proto.Message,
	b proto.Message,
	typeMayDiffer ...protoreflect.FieldNumber,
) []string {
	exempt := make(map[protoreflect.FieldNumber]struct{}, len(typeMayDiffer))
	for _, number := range typeMayDiffer {
		exempt[number] = struct{}{}
	}

	aName := a.ProtoReflect().Descriptor().Name()
	bName := b.ProtoReflect().Descriptor().Name()
	aFields := fieldsByNumber(a)
	bFields := fieldsByNumber(b)

	var problems []string
	report := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	for _, number := range sortedFieldNumbers(aFields) {
		aField := aFields[number]
		bField, ok := bFields[number]
		if !ok {
			report("field %d (%s) is on %s but missing from %s", number, aField.Name(), aName, bName)
			continue
		}
		if aField.Name() != bField.Name() {
			report("field %d is %s on %s and %s on %s", number, aField.Name(), aName, bField.Name(), bName)
			continue
		}
		if _, ok := exempt[number]; ok {
			continue
		}

		// A map field's value type is a synthesized entry message nested in its
		// parent, so its full name always differs across the pair. Compare what
		// actually goes on the wire instead.
		if aField.IsMap() {
			if !bField.IsMap() {
				report("field %d (%s) is a map on %s only", number, aField.Name(), aName)
				continue
			}
			if aField.MapKey().Kind() != bField.MapKey().Kind() {
				report("field %d (%s) has a different map key type on each side", number, aField.Name())
			}
			if problem := valueTypeDiff(aField.MapValue(), bField.MapValue(), number, aField.Name()); problem != "" {
				report("%s", problem)
			}
			continue
		}

		if aField.Cardinality() != bField.Cardinality() {
			report("field %d (%s) is %s on %s and %s on %s",
				number, aField.Name(), aField.Cardinality(), aName, bField.Cardinality(), bName)
			continue
		}
		if problem := valueTypeDiff(aField, bField, number, aField.Name()); problem != "" {
			report("%s", problem)
		}
	}

	for _, number := range sortedFieldNumbers(bFields) {
		if _, ok := aFields[number]; !ok {
			report("field %d (%s) is on %s but missing from %s", number, bFields[number].Name(), bName, aName)
		}
	}

	return problems
}

func valueTypeDiff(
	a protoreflect.FieldDescriptor,
	b protoreflect.FieldDescriptor,
	number protoreflect.FieldNumber,
	name protoreflect.Name,
) string {
	if a.Kind() != b.Kind() {
		return fmt.Sprintf("field %d (%s) is %s on one side and %s on the other", number, name, a.Kind(), b.Kind())
	}

	switch a.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if a.Message().FullName() != b.Message().FullName() {
			return fmt.Sprintf("field %d (%s) refers to %s on one side and %s on the other",
				number, name, a.Message().FullName(), b.Message().FullName())
		}
	case protoreflect.EnumKind:
		if a.Enum().FullName() != b.Enum().FullName() {
			return fmt.Sprintf("field %d (%s) refers to %s on one side and %s on the other",
				number, name, a.Enum().FullName(), b.Enum().FullName())
		}
	}
	return ""
}

func fieldsByNumber(m proto.Message) map[protoreflect.FieldNumber]protoreflect.FieldDescriptor {
	fields := m.ProtoReflect().Descriptor().Fields()
	byNumber := make(map[protoreflect.FieldNumber]protoreflect.FieldDescriptor, fields.Len())
	for i := range fields.Len() {
		field := fields.Get(i)
		byNumber[field.Number()] = field
	}
	return byNumber
}

// Field order drives the order of reported problems, which keeps a failure
// message stable between runs.
func sortedFieldNumbers(fields map[protoreflect.FieldNumber]protoreflect.FieldDescriptor) []protoreflect.FieldNumber {
	numbers := make([]protoreflect.FieldNumber, 0, len(fields))
	for number := range fields {
		numbers = append(numbers, number)
	}
	slices.Sort(numbers)
	return numbers
}
