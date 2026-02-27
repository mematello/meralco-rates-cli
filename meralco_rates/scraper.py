import xml.etree.ElementTree as ET
import re
import urllib.request
import urllib.error
from datetime import datetime
import json
import os
import hashlib
import pdfplumber
import ssl
import certifi
import logging

ssl_context = ssl.create_default_context(cafile=certifi.where())
logger = logging.getLogger("meralco_rates")


def parse_negative(val_str: str) -> float:
    """Parse string to float, handling accounting negatives like '(0.123)' as '-0.123'."""
    if not val_str:
        return 0.0
    val_str = val_str.replace("P", "").replace(",", "").strip()
    if val_str.startswith("(") and val_str.endswith(")"):
        num_str = val_str.strip("()")
        return -float(num_str) if num_str else 0.0
    try:
        return float(val_str)
    except ValueError:
        return 0.0


class MeralcoRateScraper:
    def __init__(
        self,
        rss_url="https://company.meralco.com.ph/taxonomy/term/86/feed",
        user_agent="meralco-rates-cli/0.1.0",
        retries=3,
        timeout=15,
    ):
        self.rss_url = rss_url
        self.user_agent = user_agent
        self.retries = retries
        self.timeout = timeout

    def _fetch_with_retry(self, url: str) -> str:
        """Fetch URL with exponential backoff for 429 and 5xx errors."""
        import time

        for attempt in range(self.retries):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
                with urllib.request.urlopen(
                    req, context=ssl_context, timeout=self.timeout
                ) as response:
                    return response.read().decode("utf-8")
            except urllib.error.HTTPError as e:
                if e.code == 429 or 500 <= e.code < 600:
                    wait_time = 2**attempt
                    logger.warning(
                        f"HTTP {e.code} for {url}. Retrying in {wait_time}s (Attempt {attempt+1}/{self.retries})..."
                    )
                    time.sleep(wait_time)
                else:
                    raise
            except urllib.error.URLError as e:
                wait_time = 2**attempt
                logger.warning(
                    f"URL Error for {url}: {e}. Retrying in {wait_time}s (Attempt {attempt+1}/{self.retries})..."
                )
                time.sleep(wait_time)
        raise Exception(f"Failed to fetch {url} after {self.retries} attempts.")

    def fetch_latest_rss_item(self):
        """Fetches the RSS feed and finds the most recent 'Summary of Schedule of Rates' PDF entry."""
        try:
            req = urllib.request.Request(self.rss_url, headers={"User-Agent": self.user_agent})
            with urllib.request.urlopen(
                req, context=ssl_context, timeout=self.timeout
            ) as response:
                xml_data = response.read()
        except urllib.error.URLError as e:
            raise Exception(f"Failed to fetch RSS feed: {e}")

        root = ET.fromstring(xml_data)

        for item in root.findall(".//item"):
            title_elem = item.find("title")
            title = title_elem.text if (title_elem is not None and title_elem.text) else ""

            if (
                "SUMMARY OF SCHEDULE OF RATES" in title.upper()
                or "SUMMARY SCHEDULE OF RATES" in title.upper()
            ):
                link_elem = item.find("link")
                rss_item_url = link_elem.text if (link_elem is not None and link_elem.text) else ""

                pub_elem = item.find("pubDate")
                pubDate = pub_elem.text if (pub_elem is not None and pub_elem.text) else ""

                try:
                    html_data = self._fetch_with_retry(rss_item_url)
                    pdf_urls = re.findall(
                        r"href=['\"]?([^'\" >]+\.pdf)['\"]?", html_data, re.IGNORECASE
                    )

                    target_pdf_url = None
                    for purl in pdf_urls:
                        if "rate" in purl.lower() and (
                            "schedule" in purl.lower() or "summary" in purl.lower()
                        ):
                            target_pdf_url = purl
                            break
                    if not target_pdf_url:
                        target_pdf_url = pdf_urls[-1] if pdf_urls else None

                    if not target_pdf_url:
                        logger.warning("No PDF URL found in the RSS item HTML.")
                        continue

                    pdf_url = target_pdf_url
                    if not pdf_url.startswith("http"):
                        pdf_url = "https://company.meralco.com.ph" + pdf_url
                except Exception as e:
                    logger.warning(f"Failed to fetch item HTML: {e}")
                    continue

                month_key_str = None
                if pubDate:
                    try:
                        month_match = re.search(
                            r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* (\d{4})",
                            pubDate,
                            re.IGNORECASE,
                        )
                        if month_match:
                            m_str, y_str = month_match.groups()
                            dt = datetime.strptime(f"{m_str[:3]} {y_str}", "%b %Y")
                            month_key_str = dt.strftime("%Y-%m")
                    except Exception:
                        pass

                if not month_key_str:
                    month_match = re.search(
                        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
                        title,
                        re.IGNORECASE,
                    )
                    if month_match:
                        month_name, year = month_match.groups()
                        dt = datetime.strptime(f"{month_name} {year}", "%B %Y")
                        month_key_str = dt.strftime("%Y-%m")

                return {
                    "pdf_url": pdf_url,
                    "rss_item_url": rss_item_url,
                    "month_key_str": month_key_str,
                    "title": title,
                }
        return None

    def fetch_historical_archive_items(self, start_month: str, end_month: str):
        """Fetches historical 'Summary of Schedule of Rates' PDF entries by crawling the paginated HTML archive."""
        import time

        items_in_range = []
        seen_pdf_urls = set()
        seen_node_urls = set()

        page_num = 0
        pages_crawled = 0
        stop_crawling = False

        while not stop_crawling:
            archive_url = f"https://company.meralco.com.ph/taxonomy/term/86?page={page_num}"
            logger.info(f"Crawling archive page {page_num}: {archive_url}")

            try:
                html_data = self._fetch_with_retry(archive_url)
                pages_crawled += 1
            except Exception as e:
                logger.error(f"Failed to fetch archive page {page_num}: {e}")
                break

            node_links = re.findall(
                r"<a[^>]+href=['\"](/node/\d+)['\"][^>]*>(.*?)</a>", html_data
            )

            if not node_links:
                logger.debug(f"No more nodes found on page {page_num}. Ending pagination.")
                break

            for href, link_text in node_links:
                node_url = "https://company.meralco.com.ph" + href

                if node_url in seen_node_urls:
                    continue
                seen_node_urls.add(node_url)

                time.sleep(1)
                try:
                    node_html = self._fetch_with_retry(node_url)
                    pdf_urls = re.findall(
                        r"href=['\"]?([^'\" >]+\.pdf)['\"]?", node_html, re.IGNORECASE
                    )

                    target_pdf_url = None
                    for purl in pdf_urls:
                        if "rate" in purl.lower() and (
                            "schedule" in purl.lower() or "summary" in purl.lower()
                        ):
                            target_pdf_url = purl
                            break
                    if not target_pdf_url:
                        target_pdf_url = pdf_urls[-1] if pdf_urls else None

                    if not target_pdf_url:
                        continue

                    pdf_url = target_pdf_url
                    if not pdf_url.startswith("http"):
                        pdf_url = "https://company.meralco.com.ph" + pdf_url

                    if pdf_url in seen_pdf_urls:
                        continue

                    month_match = re.search(r"/(202[0-9])-(\d{2})/", pdf_url)
                    if month_match:
                        y_str, m_str = month_match.groups()
                        month_key_str = f"{y_str}-{m_str}"
                    else:
                        month_match = re.search(r"/(\d{2})-(202[0-9])_", pdf_url)
                        if month_match:
                            m_str, y_str = month_match.groups()
                            month_key_str = f"{y_str}-{m_str}"
                        else:
                            title_match = re.search(
                                r"<title>(.*?)</title>", node_html, re.IGNORECASE
                            )
                            title = title_match.group(1) if title_match else ""
                            m_match = re.search(
                                r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
                                title,
                                re.IGNORECASE,
                            )
                            if m_match:
                                m_name, y_name = m_match.groups()
                                dt = datetime.strptime(f"{m_name} {y_name}", "%B %Y")
                                month_key_str = dt.strftime("%Y-%m")
                            else:
                                continue

                    if month_key_str > end_month:
                        seen_pdf_urls.add(pdf_url)
                        continue

                    if month_key_str < start_month:
                        logger.info(
                            f"Found {month_key_str} which is older than start_month {start_month}. Halting pagination."
                        )
                        stop_crawling = True
                        break

                    seen_pdf_urls.add(pdf_url)
                    title_match = re.search(r"<title>(.*?)</title>", node_html, re.IGNORECASE)

                    items_in_range.append(
                        {
                            "pdf_url": pdf_url,
                            "rss_item_url": node_url,
                            "month_key_str": month_key_str,
                            "title": title_match.group(1).replace(" | Meralco", "").strip()
                            if title_match
                            else "Historical Meralco Rates",
                        }
                    )
                except Exception as e:
                    logger.warning(f"Failed to fetch historical node {node_url}: {e}")
                    continue

            page_num += 1
            time.sleep(1)

        logger.info(
            f"Crawler finished. Crawled {pages_crawled} pages. Found {len(items_in_range)} valid entries matching range."
        )
        return items_in_range

    def download_pdf(self, pdf_url: str, output_path: str) -> str:
        """Download the PDF."""
        req = urllib.request.Request(pdf_url, headers={"User-Agent": self.user_agent})
        with urllib.request.urlopen(
            req, context=ssl_context, timeout=self.timeout
        ) as response, open(output_path, "wb") as out_file:
            data = response.read()
            out_file.write(data)
            pdf_sha256 = hashlib.sha256(data).hexdigest()
        return pdf_sha256

    def _build_column_index_map(self, table: list) -> dict:
        """Dynamically infer column offsets from the first 4 rows."""
        if not table or not table[0]:
            return {}

        num_cols = len(table[0])
        header_strings = [""] * num_cols
        for row in table[:4]:
            if not row:
                continue
            for i, cell in enumerate(row):
                if i < num_cols and cell:
                    header_strings[i] += " " + str(cell)

        normalized = []
        for h in header_strings:
            s = h.lower().replace("\n", " ")
            s = re.sub(r"[^a-z0-9\s]", "", s)
            s = re.sub(r"\s+", " ", s).strip()
            normalized.append(s)

        mapping = {}
        for i, text in enumerate(normalized):
            if not text:
                continue
            if "generation" in text:
                mapping["generation_charge"] = i
            elif "transmission" in text:
                mapping["transmission_charge"] = i
            elif "system loss" in text:
                mapping["system_loss_charge"] = i
            elif "distribution" in text:
                mapping["distribution_charge"] = i
            elif "supply" in text:
                mapping["supply_charge"] = i
            elif "metering" in text:
                mapping["metering_charge"] = i
            elif "awat" in text:
                mapping["awat_charge"] = i
            elif "reset" in text:
                if "onetime" in text or "one time" in text:
                    mapping["one_time_reset_fee_adjustment"] = i
                elif "regulatory" in text:
                    mapping["regulatory_reset_fees_adjustment"] = i
            elif "lifeline" in text and "subsidy" in text:
                mapping["lifeline_rate_subsidy"] = i
            elif "lifeline" in text and "discount" in text:
                mapping["applicable_discount_percent"] = i
            elif "senior citizen" in text:
                mapping["senior_citizen_subsidy"] = i
            elif "current rpt" in text:
                mapping["current_rpt_charge"] = i
            elif ("ucme" in text or "uc me" in text) and "npc" in text:
                mapping["uc_me_npc_spug"] = i
            elif ("ucme" in text or "uc me" in text) and "red" in text:
                mapping["uc_me_red_ci"] = i
            elif "uc ec" in text or "ucec" in text:
                mapping["uc_ec"] = i
            elif "uc sd" in text or "ucsd" in text:
                mapping["uc_sd"] = i
            elif "fitall" in text or "fit all" in text:
                mapping["fit_all"] = i
            elif "gea" in text:
                mapping["gea_all"] = i
            else:
                if (
                    text
                    not in [
                        "per kw",
                        "per custmo",
                        "penalty",
                        "disc",
                    ]
                    and "power factor" not in text
                    and "summary schedule" not in text
                ):
                    if "unmapped_headers" not in mapping:
                        mapping["unmapped_headers"] = []
                    mapping["unmapped_headers"].append(text)

        return mapping

    def extract_residential_rates(self, pdf_path: str) -> list:
        """Extract table from the PDF securely.
        Focuses on 'SUMMARY SCHEDULE OF RATES' Residential section.
        """
        rates = []
        with pdfplumber.open(pdf_path) as pdf:
            stop_extracting = False
            for page in pdf.pages:
                if stop_extracting:
                    break

                tables = page.extract_tables()
                if not tables:
                    continue

                for table in tables:
                    if stop_extracting:
                        break

                    col_map = self._build_column_index_map(table)
                    self.last_col_map = col_map

                    for row in table:
                        if not row or not any(row):
                            continue

                        clean_row = [
                            cell.replace("\n", " ").strip() if cell else "" for cell in row
                        ]

                        if clean_row[0].upper().startswith("GENERAL SERVICE"):
                            stop_extracting = True
                            break

                        if re.match(
                            r"(?:^\d+\s*TO\s*\d+\s*KWH)|(?:^OVER\s*\d+\s*KWH)",
                            clean_row[0],
                            re.IGNORECASE,
                        ):
                            try:
                                required_core = [
                                    "generation_charge",
                                    "transmission_charge",
                                    "system_loss_charge",
                                    "distribution_charge",
                                    "supply_charge",
                                    "metering_charge",
                                    "lifeline_rate_subsidy",
                                    "applicable_discount_percent",
                                ]
                                missing = [k for k in required_core if k not in col_map]
                                if missing:
                                    logger.error(
                                        f"Residential table is missing required header mappings: {missing}. Mapped: {col_map}"
                                    )
                                    continue

                                def _val(key: str, is_percent=False) -> float:
                                    idx_val = col_map.get(key, -1)

                                    if isinstance(idx_val, list):
                                        total = 0.0
                                        for ix in idx_val:
                                            if ix != -1 and ix < len(clean_row):
                                                v = clean_row[ix]
                                                if is_percent:
                                                    v = v.replace("%", "")
                                                total += parse_negative(v)
                                        return total

                                    if idx_val == -1 or idx_val >= len(clean_row):
                                        return 0.0
                                    val = clean_row[idx_val]
                                    if is_percent:
                                        val = val.replace("%", "")
                                    return parse_negative(val)

                                gen = _val("generation_charge")
                                dist = _val("distribution_charge")
                                sys_loss = _val("system_loss_charge")

                                if gen == 0.0 or dist == 0.0 or sys_loss == 0.0:
                                    logger.error(
                                        f"Extracted 0.0 for core field in {clean_row[0]}. Gen: {gen}, Dist: {dist}, SysLoss: {sys_loss}."
                                    )
                                    continue

                                bracket_upper = clean_row[0].upper()
                                min_kwh = 0
                                max_kwh = None

                                nums = [int(n) for n in re.findall(r"\d+", bracket_upper)]
                                if "OVER" in bracket_upper and nums:
                                    min_kwh = nums[0] + 1
                                    max_kwh = None
                                elif len(nums) >= 2:
                                    min_kwh = nums[0]
                                    max_kwh = nums[1]

                                rate = {
                                    "consumption_bracket": clean_row[0],
                                    "min_kwh": min_kwh,
                                    "max_kwh": max_kwh,
                                    "generation_charge": gen,
                                    "transmission_charge": _val("transmission_charge"),
                                    "system_loss_charge": sys_loss,
                                    "distribution_charge": dist,
                                    "supply_charge": _val("supply_charge"),
                                    "metering_charge": _val("metering_charge"),
                                    "awat_charge": _val("awat_charge"),
                                    "regulatory_reset_fees_adjustment": _val(
                                        "regulatory_reset_fees_adjustment"
                                    ),
                                    "one_time_reset_fee_adjustment": _val(
                                        "one_time_reset_fee_adjustment"
                                    ),
                                    "lifeline": {
                                        "rate_subsidy_per_kwh": _val("lifeline_rate_subsidy"),
                                        "applicable_discount_percent": _val(
                                            "applicable_discount_percent", is_percent=True
                                        ),
                                    },
                                    "senior_citizen_subsidy": _val("senior_citizen_subsidy"),
                                    "current_rpt_charge": _val("current_rpt_charge"),
                                    "uc_me_npc_spug": _val("uc_me_npc_spug"),
                                    "uc_me_red_ci": _val("uc_me_red_ci"),
                                    "uc_ec": _val("uc_ec"),
                                    "uc_sd": _val("uc_sd"),
                                    "fit_all": _val("fit_all"),
                                    "gea_all": _val("gea_all"),
                                }
                                rates.append(rate)
                            except Exception as e:
                                logger.error(f"Error parsing row: {clean_row}, Exception: {e}")

        return rates
