name: CVD Scan

on:
  schedule:
    - cron: "*/5 * * * *"
  workflow_dispatch:

permissions:
  contents: read

concurrency:
  group: cvd-scan
  cancel-in-progress: false

jobs:
  scan:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip

      - run: pip install -r requirements.txt

      - name: Restore state
        uses: actions/cache/restore@v4
        with:
          path: cvd_zero_touch_state.json
          key: cvd-state-${{ github.run_id }}
          restore-keys: cvd-state-

      - name: Run scanner
        run: python cvd_zero_touch.py --once
        env:
          NTFY_TOPIC: ${{ secrets.NTFY_TOPIC }}

      - name: Save state
        if: always()
        uses: actions/cache/save@v4
        with:
          path: cvd_zero_touch_state.json
          key: cvd-state-${{ github.run_id }}
