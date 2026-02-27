# Meralco Rates CLI

A standalone, generic CLI utility to fetch and parse Meralco’s monthly "Summary Schedule of Rates" (Residential) PDFs into structured JSON or CSV formats.

**Note:** This tool explicitly parses the **Residential** rates block and its associative subsidies/charges. Industrial-only headers (such as Power Factor) and other customer classes are intentionally ignored.

## Installation

Ensure you have Python 3.9+ installed.

```bash
git clone https://github.com/WEBDEV-Intern/meralco-rates-cli.git
cd meralco-rates-cli
pip install -e .
```

This will expose the `meralco-rates` command globally in your Python environment.

## Quickstart

### Fetch the latest rates
By default, the tool outputs pretty-printed JSON to stdout.

```bash
meralco-rates latest --pretty
```

### Save output to a CSV file

```bash
meralco-rates --output csv --out meralco_latest.csv latest
```

### Backfill historical rates
You can crawl the Meralco archive for a specific range:

```bash
meralco-rates --output json --out historical_rates.json backfill --start 2024-01 --end 2024-06
```

## CLI Reference

```
usage: meralco-rates [-h] [--timeout TIMEOUT] [--retries RETRIES] [--user-agent USER_AGENT] [--output {json,csv}] [--pretty] [--out OUT] {latest,backfill} ...

Fetch and parse Meralco Residential Schedule of Rates

positional arguments:
  {latest,backfill}
    latest              Fetch the latest published rates
    backfill            Fetch historical rates for a given range (requires --start and --end)

optional arguments:
  -h, --help            show this help message and exit
  --timeout TIMEOUT     HTTP timeout in seconds (default: 15)
  --retries RETRIES     Number of exponential backoff retries for 429/5xx errors (default: 3)
  --user-agent USER_AGENT
                        Custom User-Agent header (default: meralco-rates-cli/0.1.0)
  --output {json,csv}   Output format (default: json)
  --pretty              Pretty-print the JSON output
  --out OUT             File path to save the output (prints to stdout if omitted)
```

## Data Fields Explanation

The parser maps PDF columns to standard field names:

* `consumption_bracket`: The localized text block (e.g. `0 TO 20 KWH`, `21 TO 50 KWH`).
* `min_kwh` / `max_kwh`: Parsed integer equivalents of the bracket bounds.
* `generation_charge`, `transmission_charge`, `system_loss_charge`, `distribution_charge`, `supply_charge`, `metering_charge`: Core fundamental supply metrics.
* `awat_charge`, `regulatory_reset_fees_adjustment`, `one_time_reset_fee_adjustment`, `current_rpt_charge`: Adjustment factors (which may occasionally be negative/refunds).
* `lifeline`: A nested object specifying `rate_subsidy_per_kwh` and `applicable_discount_percent`.
* `senior_citizen_subsidy`: Subsidies.
* `uc_me_npc_spug`, `uc_me_red_ci`, `uc_ec`, `uc_sd`, `fit_all`, `gea_all`: Various Universal and Feed-in Tariff allowances.

**Provenance Metadata**
Every JSON/CSV payload includes a metadata root documenting the `source_pdf_url`, `pdf_sha256`, and a `table_layout_signature` indicating precisely how columns mapped during extraction.

## Known Layout Variations
Meralco periodically updates their PDF layout. 
* Most months contain **23 columns** across the page.
* Some months (especially during true-ups or new charge introductions like GEA) expand to **25 columns**.

The parser utilizes a dynamic heuristic mapping index scanning the first 4 rows to detect headers (e.g. `'generation'`, `'transmission'`, `'system loss'`, `'fit all'`). This enables `pdfplumber` to accurately index variable PDFs.

## License

MIT License. See `LICENSE` for details.
