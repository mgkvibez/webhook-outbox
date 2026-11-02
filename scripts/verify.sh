#!/usr/bin/env bash
# The gate. Local runs of this and CI run the same script, so a green CI is not the only proof.
set -u
cd "$(dirname "$0")/.."
fail=0
say() { printf '%-46s %s\n' "$1" "$2"; }

# 1. the package imports
if python3 -c "import webhook_outbox" 2>/dev/null; then say "1/5 package imports" "ok"
else say "1/5 package imports" "FAIL"; fail=1; fi

# 2. pyproject version == module __version__ (3.9 has no tomllib, so fall back to a regex)
py_ver=$(python3 - <<'PY'
import re, pathlib
text = pathlib.Path("pyproject.toml").read_text()
try:
    import tomllib
    print(tomllib.loads(text)["project"]["version"])
except ModuleNotFoundError:
    print(re.search(r'^version = "([^"]+)"', text, re.M).group(1))
PY
)
mod_ver=$(python3 -c "import webhook_outbox; print(webhook_outbox.__version__)" 2>/dev/null)
if [ "$py_ver" = "$mod_ver" ] && [ -n "$py_ver" ]; then say "2/5 version $py_ver consistent" "ok"
else say "2/5 version drift ($py_ver vs $mod_ver)" "FAIL"; fail=1; fi

# 3. the tests
if python3 -m pytest -q >.gate-pytest.log 2>&1; then
  say "3/5 tests ($(tail -1 .gate-pytest.log | tr -d '\n'))" "ok"
else say "3/5 tests" "FAIL"; tail -5 .gate-pytest.log; fail=1; fi

# 4. CLI smoke, including the documented exit contract (nothing waiting exits 0)
db=$(mktemp /tmp/wo-gate-XXXXXX.db)
out=$(python3 -m webhook_outbox.cli --version 2>&1)
if [ "$out" = "outbox $mod_ver" ]; then say "4/5 cli --version" "ok"
else say "4/5 cli --version ($out)" "FAIL"; fail=1; fi
python3 -m webhook_outbox.cli --db "$db" enqueue --endpoint http://127.0.0.1:9/h --event e >/dev/null 2>&1
python3 -m webhook_outbox.cli --db "$db" stats >/dev/null 2>&1
stats_code=$?
if [ "$stats_code" -eq 0 ]; then say "4/5 cli exit contract (stats=0)" "ok"
else say "4/5 cli exit contract (stats=$stats_code)" "FAIL"; fail=1; fi
rm -f "$db" "$db"-wal "$db"-shm

# 5. zero runtime dependencies in the built artifact
rm -r .gate-dist 2>/dev/null
build_log=.gate-build.log
if python3 -m build --wheel --outdir .gate-dist >"$build_log" 2>&1; then
  wheel=$(ls .gate-dist/*.whl 2>/dev/null | head -1)
  reqs=$(unzip -p "$wheel" '*/METADATA' 2>/dev/null | grep '^Requires-Dist' | grep -v 'extra ==' || true)
  if [ -z "$reqs" ]; then say "5/5 no runtime dependencies" "ok"
  else say "5/5 runtime deps found: $reqs" "FAIL"; fail=1; fi
else say "5/5 wheel build" "FAIL"; tail -3 "$build_log"; fail=1; fi
rm -r .gate-dist 2>/dev/null

echo
[ "$fail" -eq 0 ] && echo "GATE PASS" || echo "GATE FAIL"
exit "$fail"
