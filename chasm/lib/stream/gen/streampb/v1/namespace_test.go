package streampb

import (
	"testing"

	"go.temporal.io/server/common/rpc/interceptor"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Every request carrying a frontend_request must expose its namespace to the
// interceptors. Driven off the descriptor rather than a hand-written list so a
// new RPC fails here instead of silently opting out of namespace rate limits,
// validation, authorization and redirection.
func TestEveryRoutedRequestExposesNamespace(t *testing.T) {
	fd := File_temporal_server_chasm_lib_stream_proto_v1_request_response_proto
	messages := fd.Messages()

	checked := 0
	for i := 0; i < messages.Len(); i++ {
		md := messages.Get(i)
		if md.Fields().ByName("frontend_request") == nil {
			continue
		}
		mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName())
		if err != nil {
			t.Fatalf("%s is not registered: %v", md.FullName(), err)
		}
		msg := mt.New().Interface().(proto.Message)
		if _, ok := msg.(interceptor.NamespaceNameGetter); !ok {
			t.Errorf("%s has a frontend_request but no GetNamespace; add it in namespace.go", md.FullName())
		}
		checked++
	}

	if checked == 0 {
		t.Fatal("found no routed requests, so this test is not checking anything")
	}
}
