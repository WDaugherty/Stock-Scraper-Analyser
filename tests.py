# test_scripts.py

import pytest
import os
import pandas as pd
from data_retrieval import get_stock_info
from analysis_pipeline import fetch_additional_metrics

@pytest.mark.parametrize("ticker_symbol", [ "AAPL", "MSFT", "GOOGL", "AMZN", 
    "TSLA", "META", "V", "JPM", 
    "DIS", "NFLX"])
def test_get_stock_info_dict_structure(ticker_symbol):
    """
    Ensure get_stock_info returns a dictionary with specific keys for each ticker.
    """
    result = get_stock_info(ticker_symbol)
    required_keys = [
        'Stock Symbol',
        'Company Name',
        'Next Earnings Report Date',
        'Dividend Offered',
        'Ex-Dividend Date',
        'Annual Dividend Yield'
    ]

    # Check result is a dictionary
    assert isinstance(result, dict), f"Expected a dict, got {type(result)}."

    # Check all required keys are present
    for key in required_keys:
        assert key in result, f"Missing key '{key}' in result."

@pytest.mark.parametrize("ticker_symbol", ["AAPL", "MSFT"])
def test_get_stock_info_data_values(ticker_symbol):
    """
    Test that certain fields have values of the correct type or format.
    """
    result = get_stock_info(ticker_symbol)

    # 'Stock Symbol' should be a string matching the ticker
    assert result['Stock Symbol'] == ticker_symbol, \
        f"Stock symbol mismatch. Expected {ticker_symbol}, found {result['Stock Symbol']}."

    # 'Dividend Offered' should be 'Yes' or 'No'
    assert result['Dividend Offered'] in ['Yes', 'No'], \
        f"Unexpected value for Dividend Offered: {result['Dividend Offered']}."

def test_fetch_additional_metrics():
    """
    Test fetch_additional_metrics to ensure it returns expected keys/values.
    """
    symbol = "AAPL"
    metrics = fetch_additional_metrics(symbol)
    expected_keys = [
        "current_price",
        "trailing_eps",
        "book_value",
        "calculated_pe_ratio",
        "calculated_pb_ratio"
    ]

    # Check that all expected keys are present
    for key in expected_keys:
        assert key in metrics, f"Missing '{key}' in fetched metrics data."

    # Example check: "current_price" should be float, int, or None
    assert isinstance(metrics["current_price"], (float, int, type(None))), \
        f"Expected current_price to be float, int, or None. Found {type(metrics['current_price'])}."

def test_analysis_output_exists():
    """
    Optional test that checks if stock_data.csv exists so that the analysis pipeline can run.
    Requires data_retrieval.py to be run first.
    """
    assert os.path.exists("stock_data.csv"), \
        "stock_data.csv not found. Please run data_retrieval.py to generate it."

def test_analysis_output_non_empty():
    """
    Optional test to ensure stock_data.csv is not empty, confirming some data was retrieved.
    """
    assert os.path.exists("stock_data.csv"), \
        "stock_data.csv not found. Please run data_retrieval.py to generate it."
    
    df = pd.read_csv("stock_data.csv")
    assert not df.empty,
