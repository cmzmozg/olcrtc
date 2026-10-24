# // ai-generated: compose standalone diagnostic fragments into existing tests without editing repository sources.
from pathlib import Path
import json

here = Path(__file__).resolve().parent
root = here.parents[1]
out = root / 'build/hostkey-opt2-diagnostics'
out.mkdir(parents=True, exist_ok=True)
mapping = {}
for name, fragment, target in [
    ('internal/protect/resolver_test.go', 'dns-diagnostic.go.txt', 'resolver-test.go.txt'),
    ('internal/server/egress_test.go', 'dial-diagnostic.go.txt', 'egress-test.go.txt'),
]:
    source = root / name
    destination = out / target
    destination.write_text(source.read_text() + '\n' + (here / fragment).read_text())
    mapping[str(source)] = str(destination)
(out / 'overlay.json').write_text(json.dumps({'Replace': mapping}, indent=2) + '\n')
print(out / 'overlay.json')
