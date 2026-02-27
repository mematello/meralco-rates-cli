import argparse
import sys
import json
import csv
import logging
from datetime import datetime
import os
from .scraper import MeralcoRateScraper

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("meralco_rates")

def setup_arg_parser():
    common_parser = argparse.ArgumentParser(add_help=False)
    
    # Global HTTP arguments
    common_parser.add_argument(
        "--timeout",
        type=int,
        default=15,
        help="HTTP timeout in seconds (default: 15)",
    )
    common_parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of exponential backoff retries for 429/5xx errors (default: 3)",
    )
    common_parser.add_argument(
        "--user-agent",
        type=str,
        default="meralco-rates-cli/0.1.0",
        help="Custom User-Agent header (default: meralco-rates-cli/0.1.0)",
    )

    # Output Controls
    common_parser.add_argument(
        "--output",
        choices=["json", "csv"],
        default="json",
        help="Output format (default: json)",
    )
    common_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print the JSON output",
    )
    common_parser.add_argument(
        "--out",
        type=str,
        help="File path to save the output (prints to stdout if omitted)",
    )

    parser = argparse.ArgumentParser(
        description="Fetch and parse Meralco Residential Schedule of Rates",
        epilog="Note: This tool specifically parses the Residential rates block. Power factor/industrial headers are intentionally ignored.",
        conflict_handler='resolve'
    )
    
    # Add arguments to main parser so `meralco-rates --help` shows them
    # But effectively they are evaluated as part of the subcommand.
    for action in common_parser._actions:
        parser._add_action(action)

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: latest
    latest_parser = subparsers.add_parser("latest", help="Fetch the latest published rates", parents=[common_parser], conflict_handler='resolve')

    # Subcommand: backfill
    backfill_parser = subparsers.add_parser("backfill", help="Fetch historical rates for a given range", parents=[common_parser], conflict_handler='resolve')
    backfill_parser.add_argument("--start", required=True, help="Start month (YYYY-MM)")
    backfill_parser.add_argument("--end", required=True, help="End month (YYYY-MM)")

    return parser

def output_results(data_docs, args):
    if args.output == "json":
        text_out = json.dumps(data_docs, indent=2 if args.pretty else None)
    elif args.output == "csv":
        import io
        import copy
        output = io.StringIO()
        if not data_docs:
            return ""
            
        # Flatten structure for CSV
        flat_records = []
        for doc in data_docs:
            base_meta = {
                "month_key": doc["month_key"],
                "source_rss_item_url": doc["provenance"]["source_rss_item_url"],
                "source_pdf_url": doc["provenance"]["source_pdf_url"],
                "pdf_sha256": doc["provenance"]["pdf_sha256"],
                "parser_version": doc["provenance"]["parser_version"]
            }
            for row in doc["residential_rates"]:
                flat_row = copy.deepcopy(base_meta)
                flat_row.update(row)
                # Unpack midline nested object
                if "lifeline" in flat_row:
                    flat_row["lifeline_rate_subsidy_per_kwh"] = flat_row["lifeline"].get("rate_subsidy_per_kwh")
                    flat_row["lifeline_applicable_discount_percent"] = flat_row["lifeline"].get("applicable_discount_percent")
                    del flat_row["lifeline"]
                flat_records.append(flat_row)
                
        if flat_records:
            writer = csv.DictWriter(output, fieldnames=flat_records[0].keys())
            writer.writeheader()
            writer.writerows(flat_records)
        text_out = output.getvalue()

    if args.out:
        with open(args.out, "w") as f:
            f.write(text_out)
        logger.info(f"\n✅ Output successfully written to {args.out}")
    else:
        # If outputting to stdout, we don't want to pollute with logger messages on stdout
        # However, logger by default writes to stderr if configured correctly, but our print statements might conflict.
        # Actually our `text_out` is the payload.
        print(text_out)

def process_items(scraper, items_to_process, args):
    results = []
    
    for item in items_to_process:
        month_key = item['month_key_str']
        logger.info(f"📥 Processing {month_key} ({item['title']})...")
        
        os.makedirs(".meralco_tmp", exist_ok=True)
        pdf_path = f".meralco_tmp/meralco_rates_{month_key}.pdf"
        
        try:
            pdf_sha256 = scraper.download_pdf(item['pdf_url'], pdf_path)
            logger.info(f"   Extracting Residential Rates...")
            rates = scraper.extract_residential_rates(pdf_path)
            
            if not rates:
                logger.error(f"❌ Failed to extract any residential rates from {month_key}.")
                continue
                
            doc = {
                "month_key": month_key,
                "residential_rates": rates,
                "provenance": {
                    "source_rss_item_url": item['rss_item_url'],
                    "source_pdf_url": item['pdf_url'],
                    "pdf_sha256": pdf_sha256,
                    "fetched_at": datetime.now().isoformat(),
                    "parser_version": "v3_generic",
                    "table_layout_signature": getattr(scraper, "last_col_map", {})
                }
            }
            results.append(doc)
            
        except Exception as e:
            logger.error(f"❌ Unhandled error for {month_key}: {e}")
        finally:
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
                
    return results

def main():
    parser = setup_arg_parser()
    
    # Parse known args first to capture global flags passed before the subcommand
    # because argparse sub-parsers overwrite them with default values if using `parents`.
    global_args, rest = parser.parse_known_args()
    args = parser.parse_args()
    
    # Preserve global flags like --out and --output if specified before subcommand
    for key, val in vars(global_args).items():
        if val is not None and val is not False and val != getattr(parser, 'default_values', {}).get(key):
            setattr(args, key, val)
    
    # Update default tracking for output (since we hardcoded default="json" in setup)
    if not hasattr(args, 'output') or (hasattr(global_args, 'output') and global_args.output != "json"):
         args.output = global_args.output

    scraper = MeralcoRateScraper(
        user_agent=args.user_agent,
        retries=args.retries,
        timeout=args.timeout
    )
    
    try:
        if args.command == "latest":
            latest_item = scraper.fetch_latest_rss_item()
            if not latest_item:
                logger.error("No valid Meralco summary found in RSS feed.")
                sys.exit(1)
            results = process_items(scraper, [latest_item], args)
            output_results(results, args)
            
        elif args.command == "backfill":
            logger.info(f"Fetching historical Archive items from {args.start} to {args.end}...")
            items_to_process = scraper.fetch_historical_archive_items(args.start, args.end)
            if not items_to_process:
                logger.warning(f"No valid Meralco summary found in range {args.start} - {args.end}.")
                sys.exit(1)
            # Sort chronologically
            items_to_process.sort(key=lambda x: x['month_key_str'])
            results = process_items(scraper, items_to_process, args)
            output_results(results, args)
            
    except Exception as e:
        logger.error(f"Critical error during execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
