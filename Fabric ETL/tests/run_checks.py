"""Run every static check: the four Fabric lints, and the generated key checks against the declared keys.
The key-check comparison builds the warehouse from Silver, so run run_demo.py once first.

    python tests/run_checks.py
"""
import os, subprocess, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHECKS = ["tests/lint_batch_columns.py", "tests/lint_star_views.py", "tests/lint_temp_widths.py",
          "tests/lint_view_grants.py", "tools/key_checks.py"]
failed = []
for c in CHECKS:
    r = subprocess.run([sys.executable, os.path.join(ROOT, c)], capture_output=True, text=True)
    print(f"{'ok  ' if r.returncode == 0 else 'FAIL'} {c}: {(r.stdout.strip().splitlines() or [''])[-1]}")
    if r.returncode:
        failed.append(c)
        print(r.stdout[-2000:], r.stderr[-2000:])
sys.exit(1 if failed else 0)
