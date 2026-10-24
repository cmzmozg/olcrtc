# // ai-generated: summarize useful bytes, latency, failures and aggregate goodput per isolated phase.
from collections import defaultdict
from pathlib import Path
import json
import statistics

root = Path(__file__).parent
groups = defaultdict(list)
for line in (root/'results.jsonl').read_text().splitlines():
    row = json.loads(line)
    if 'http' in row:
        groups[row['label'],row['phase']].append(row)
result = []
for (label,phase),rows in groups.items():
    good = [r for r in rows if r['ok']]
    entry = {'label':label,'phase':phase,'attempts':len(rows),'success':len(good),'failures':len(rows)-len(good)}
    for field in ['ttfb_s','total_s','mbps']:
        values = [r[field] for r in good]
        if values:
            entry[field] = {'median':statistics.median(values),'min':min(values),'max':max(values)}
    if phase.endswith('-bulk') and good:
        elapsed = max(r['started_epoch']+r['total_s'] for r in good)-min(r['started_epoch'] for r in good)
        entry['aggregate_span_s'] = elapsed
        entry['aggregate_mbps'] = sum(r['actual_bytes'] for r in good)*8/elapsed/1e6
    result.append(entry)
(root/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
for row in result:
    if row['phase'] == 'ready':
        continue
    print(row['label'],row['phase'],str(row['success'])+'/'+str(row['attempts']),
          'TTFB='+str(round(row.get('ttfb_s',{}).get('median',0),3)),
          'mbps='+str(round(row.get('mbps',{}).get('median',0),2)),
          'aggregate='+str(round(row.get('aggregate_mbps',0),2)))
