name: Whale Activity + Order Book Scanner

on:
  schedule:
    - cron: "*/5 * * * *"
  workflow_dispatch:

permissions:
  contents: read

jobs:
  scan:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: python -m pip install --upgrade pip requests

      - name: Restore scanner state
        uses: actions/cache/restore@v4
        with:
          path: whale_scanner_state.json
          key: whale-state-${{ github.ref_name }}-${{ github.run_id }}
          restore-keys: |
            whale-state-${{ github.ref_name }}-

      - name: Run Whale Scanner
        env:
          NFTY_URL: ${{ secrets.NFTY_URL }}
          TOP_N: "100"
          MIN_TRADE_USD: "100000"
          WALL_MIN_USD: "500000"
          WALL_DISTANCE_PCT: "1.0"
          ALERT_COOLDOWN_MIN: "30"

        run: python whale_activity_scanner.py

      - name: Save scanner state
        if: always()
        uses: actions/cache/save@v4
        with:
          path: whale_scanner_state.json
          key: whale-state-${{ github.ref_name }}-${{ github.run_id }}
