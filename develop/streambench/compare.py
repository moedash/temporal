import json

a = json.load(open("/tmp/bench/option5.json"))
b = json.load(open("/tmp/bench/option7.json"))
n5, n7 = a["tokens_published"], b["tokens_published"]

def per(v, n): return f"{v/n:.2f}" if n else "n/a"

rows = [
    ("tokens published",        f"{n5}",                       f"{n7}"),
    ("latency p50 ms",          f"{a['latency_p50_ms']:.0f}",  f"{b['latency_p50_ms']:.0f}"),
    ("latency p90 ms",          f"{a['latency_p90_ms']:.0f}",  f"{b['latency_p90_ms']:.0f}"),
    ("latency p99 ms",          f"{a['latency_p99_ms']:.0f}",  f"{b['latency_p99_ms']:.0f}"),
    ("latency max ms",          f"{a['latency_max_ms']:.0f}",  f"{b['latency_max_ms']:.0f}"),
    ("Temporal ops total",      f"{a['temporal_ops_total']}",  f"{b['temporal_ops_total']}"),
    ("Temporal ops / token",    per(a['temporal_ops_total'], n5), per(b['temporal_ops_total'], n7)),
    ("Redis ops total",         f"{a['redis_ops_total']}",     f"{b['redis_ops_total']}"),
    ("Redis ops / token",       per(a['redis_ops_total'], n5), per(b['redis_ops_total'], n7)),
    ("TOTAL io ops / token",    per(a['temporal_ops_total']+a['redis_ops_total'], n5),
                                per(b['temporal_ops_total']+b['redis_ops_total'], n7)),
    ("history events",          f"{a['history_events']}",      f"{b['history_events']}"),
    ("history bytes",           f"{a['history_bytes']}",       f"{b['history_bytes']}"),
    ("history bytes / token",   per(a['history_bytes'], n5),   per(b['history_bytes'], n7)),
]

w = max(len(r[0]) for r in rows) + 2
print(f"{'':<{w}}{'Option 5 (Temporal log)':>26}{'Option 7 (Redis)':>20}")
print("-" * (w + 46))
for name, x, y in rows:
    print(f"{name:<{w}}{x:>26}{y:>20}")
print()
print("Option 5 top ops:", sorted(a["temporal_ops"].items(), key=lambda k: -k[1])[:5])
print("Option 7 top ops:", sorted(b["redis_ops"].items(), key=lambda k: -k[1])[:5])
