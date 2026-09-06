// Command streamdemo shows a native stream end to end: a producer standing in
// for an LLM-calling activity appends tokens, and a browser watches them arrive
// over SSE. The bridge in the middle is what an application's backend would be.
//
// It needs no SDK and no workflow. That is the point of the path it exercises:
// tokens come from an activity, not from workflow code, so nothing here has to
// wait on the workflow-facing API.
//
//	go run ./develop/streamdemo -frontend 127.0.0.1:7233 -namespace default
//	open http://127.0.0.1:8088
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var sentence = strings.Fields(
	"Durable streaming means the tokens you are reading right now survive a " +
		"server restart, because every one of them was committed before it was " +
		"shown to you.")

func main() {
	frontend := flag.String("frontend", "127.0.0.1:7233", "frontend gRPC address")
	ns := flag.String("namespace", "default", "namespace")
	listen := flag.String("listen", "127.0.0.1:8088", "address to serve the demo on")
	flag.Parse()

	conn, err := grpc.NewClient(*frontend, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial frontend: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client := streampb.NewStreamServiceClient(conn)

	d := &demo{client: client, namespace: *ns}
	http.HandleFunc("/", d.page)
	http.HandleFunc("/start", d.start)
	http.HandleFunc("/events", d.events)

	log.Printf("streamdemo listening on http://%s", *listen)
	server := &http.Server{Addr: *listen, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}

type demo struct {
	client    streampb.StreamServiceClient
	namespace string
}

// start creates a stream and produces into it, batching on an interval the way
// an activity consuming a model's token stream would.
func (d *demo) start(w http.ResponseWriter, r *http.Request) {
	streamID := r.URL.Query().Get("stream")
	if streamID == "" {
		http.Error(w, "stream is required", http.StatusBadRequest)
		return
	}

	created, err := d.client.CreateStream(r.Context(), &streampb.CreateStreamRequest{
		FrontendRequest: &streampb.CreateStreamInput{
			Namespace: d.namespace, StreamId: streamID,
		},
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	runID := created.GetFrontendResponse().GetRunId()

	go d.produce(streamID, runID)
	w.WriteHeader(http.StatusAccepted)
}

func (d *demo) produce(streamID, runID string) {
	ctx := context.Background()
	for i, word := range sentence {
		time.Sleep(120 * time.Millisecond)
		_, err := d.client.AddMessages(ctx, &streampb.AddMessagesRequest{
			FrontendRequest: &streampb.AddMessagesInput{
				Namespace: d.namespace, StreamId: streamID, RunId: runID,
				ProducerId: "demo", Sequence: int64(i + 1),
				Messages: []*streampb.StreamMessage{{
					Body: &commonpb.Payload{Data: []byte(word + " ")},
					Kind: streampb.STREAM_MESSAGE_KIND_DATA,
				}},
			},
		})
		if err != nil {
			log.Printf("append failed: %v", err)
			return
		}
	}
	if _, err := d.client.CloseStream(ctx, &streampb.CloseStreamRequest{
		FrontendRequest: &streampb.CloseStreamInput{
			Namespace: d.namespace, StreamId: streamID,
		},
	}); err != nil {
		log.Printf("close failed: %v", err)
	}
}

// events bridges the stream to the browser. The reader owns its offset, so a
// reconnecting browser resumes exactly where it stopped by passing the offset
// back rather than by the server remembering anything about it.
func (d *demo) events(w http.ResponseWriter, r *http.Request) {
	streamID := r.URL.Query().Get("stream")
	from, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	for r.Context().Err() == nil {
		resp, err := d.client.PollMessages(r.Context(), &streampb.PollMessagesRequest{
			FrontendRequest: &streampb.PollMessagesInput{
				Namespace: d.namespace, StreamId: streamID,
				FromOffset: from, WaitNewMessages: true,
			},
		})
		if err != nil {
			return
		}
		out := resp.GetFrontendResponse()
		for _, m := range out.GetMessages() {
			if _, err := fmt.Fprintf(w, "data: %s\n\n", m.GetBody().GetData()); err != nil {
				return
			}
		}
		from = out.GetNextOffset()
		flusher.Flush()

		if out.GetClosed() && len(out.GetMessages()) == 0 {
			_, _ = fmt.Fprint(w, "event: done\ndata: \n\n")
			flusher.Flush()
			return
		}
	}
}

func (d *demo) page(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	_, _ = fmt.Fprint(w, `<!doctype html>
<title>Temporal native streams</title>
<body style="font:16px/1.6 system-ui;max-width:40em;margin:4em auto">
<button id="go">Start a stream</button>
<p id="out"></p>
<script>
document.getElementById('go').onclick = async () => {
  const id = 'demo-' + Date.now();
  document.getElementById('out').textContent = '';
  await fetch('/start?stream=' + id, {method: 'POST'});
  const es = new EventSource('/events?stream=' + id + '&from=0');
  es.onmessage = e => document.getElementById('out').textContent += e.data;
  es.addEventListener('done', () => es.close());
};
</script>
</body>`)
}
