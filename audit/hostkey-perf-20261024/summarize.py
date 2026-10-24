# // ai-generated: summarize measured payload successes and failures without dropping timeouts.
from collections import defaultdict
from pathlib import Path
import json
import statistics

rows = [json.loads(line) for line in Path(__file__).with_name('results.jsonl').read_text().splitlines()]
groups = defaultdict(list)
for row in rows:
    if row['phase'] not in ['handshake', 'steady-window']:
        groups[row['label'], row['phase']].append(row)
out = []
for (label, phase), values in groups.items():
    good = [r for r in values if r.get('ok')]
    item = {'label':label, 'phase':phase, 'attempts':len(values), 'success':len(good), 'failures':len(values)-len(good)}
    for metric in ['ttfb_s', 'total_s', 'mbps']:
        measured = [r[metric] for r in good if metric in r]
        if measured:
            item[metric] = {'median':statistics.median(measured), 'min':min(measured), 'max':max(measured)}
    if phase in ['load-bulk', 'steady-bulk'] and good and all('started_epoch' in r for r in good):
        span = max(r['started_epoch']+r['total_s'] for r in good)-min(r['started_epoch'] for r in good)
        item['aggregate_mbps'] = sum(r['actual_bytes'] for r in good)*8/span/1e6
        item['aggregate_span_s'] = span
    out.append(item)
print(json.dumps(out, indent=2))
