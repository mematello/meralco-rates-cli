import pytest
from meralco_rates.scraper import MeralcoRateScraper, parse_negative

def test_parse_negative():
    assert parse_negative("0.123") == 0.123
    assert parse_negative("(0.123)") == -0.123
    assert parse_negative("P(0.123)") == -0.123
    assert parse_negative("P 0.123") == 0.123
    assert parse_negative("1,234.56") == 1234.56
    assert parse_negative("(1,234.56)") == -1234.56
    assert parse_negative("") == 0.0
    assert parse_negative("Invalid") == 0.0

def test_scraper_initialization():
    scraper = MeralcoRateScraper(user_agent="test-agent", retries=5, timeout=10)
    assert scraper.user_agent == "test-agent"
    assert scraper.retries == 5
    assert scraper.timeout == 10

def test_column_index_map():
    scraper = MeralcoRateScraper()
    # Mocking a small subset of headers from Meralco PDFs.
    # pdfplumber extracts columns vertically aligned as lists.
    table = [
        ["SUMMARY SCHEDULE OF RATES EFFECTIVE FEBRUARY 2026 BILLING RESIDENTIAL", "", ""],
        ["", "", ""],
        ["GENERATION", "TRANSMISSION", "SYSTEM LOSS"],
        ["CHARGE", "CHARGE", "CHARGE"]
    ]
    
    col_map = scraper._build_column_index_map(table)
    assert col_map.get("generation_charge") == 0
    assert col_map.get("transmission_charge") == 1
    assert col_map.get("system_loss_charge") == 2
