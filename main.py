import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
from typing import Dict, Any
import pytz

def get_stock_info(ticker_symbol: str) -> Dict[str, Any]:
    """
    Retrieves stock info such as company name, next earnings date, and dividend details.
    
    Args:
        ticker_symbol (str): The stock ticker symbol
        
    Returns:
        Dict[str, Any]: Dictionary containing stock information
    """
    try:
        time.sleep(1)
        ticker = yf.Ticker(ticker_symbol)
        
        # Initialize default values
        company_name = 'N/A'
        next_earnings_date = None
        dividend_offered = 'No'
        ex_dividend_date = None
        annual_dividend_yield = None

        # Get company info
        try:
            info = ticker.info
            company_name = info.get('shortName', 'N/A')
        except Exception as e:
            print(f"Error getting info for {ticker_symbol}: {str(e)}")
            info = {}

        # Get earnings date - try multiple methods
        try:
            # Method 1: Try calendar
            try:
                calendar = ticker.calendar
                if isinstance(calendar, pd.DataFrame) and 'Earnings Date' in calendar.index:
                    next_earnings_date = calendar.loc['Earnings Date'].iloc[0]
            except:
                pass

            # Method 2: Try earnings_dates if calendar failed
            if not next_earnings_date:
                earnings_dates = ticker.earnings_dates
                if isinstance(earnings_dates, pd.DataFrame) and not earnings_dates.empty:
                    ny_tz = pytz.timezone('America/New_York')
                    current_date = datetime.now(ny_tz)
                    
                    # Convert timestamps to UTC for comparison
                    if earnings_dates.index.tz is not None:
                        current_date = current_date.astimezone(pytz.UTC)
                        future_dates = earnings_dates[earnings_dates.index > current_date]
                    else:
                        # If dates are naive, localize them first
                        localized_dates = earnings_dates.index.tz_localize(ny_tz)
                        current_date = current_date.astimezone(ny_tz)
                        future_dates = earnings_dates[localized_dates > current_date]
                        
                    if not future_dates.empty:
                        next_earnings_date = future_dates.index[0]
        except Exception as e:
            print(f"Error getting earnings date for {ticker_symbol}: {str(e)}")

        # Get dividend information
        try:
            # Get recent dividends
            recent_dividends = ticker.dividends
            
            if not recent_dividends.empty:
                dividend_offered = 'Yes'
                
                # Calculate annual dividend yield from recent dividends
                annual_div_rate = recent_dividends.head(4).sum() if len(recent_dividends) >= 4 else recent_dividends.sum() * (4 / len(recent_dividends))
                latest_price = info.get('regularMarketPrice', info.get('currentPrice'))
                if latest_price:
                    annual_dividend_yield = (annual_div_rate / latest_price) * 100
                
                # Get most recent ex-dividend date
                if len(recent_dividends) > 0:
                    ex_dividend_date = recent_dividends.index[-1].strftime('%Y-%m-%d')

        except Exception as e:
            print(f"Error getting dividend info for {ticker_symbol}: {str(e)}")

        # Validate dates are not in the distant future
        ny_tz = pytz.timezone('America/New_York')
        current_date = datetime.now(ny_tz)
        max_future_date = current_date + timedelta(days=180)  # Maximum 6 months in future
        
        if next_earnings_date:
            if isinstance(next_earnings_date, pd.Timestamp):
                # Convert both dates to UTC for comparison
                if next_earnings_date.tz is not None:
                    next_earnings_date_utc = next_earnings_date.tz_convert('UTC')
                else:
                    next_earnings_date_utc = next_earnings_date.tz_localize(ny_tz).tz_convert('UTC')
                    
                max_future_date_utc = max_future_date.astimezone(pytz.UTC)
                if next_earnings_date_utc > max_future_date_utc:
                    next_earnings_date = None

        if ex_dividend_date:
            try:
                ex_div_dt = datetime.strptime(ex_dividend_date, '%Y-%m-%d')
                ex_div_dt = ny_tz.localize(ex_div_dt)
                if ex_div_dt > max_future_date:
                    ex_dividend_date = None
            except:
                pass

        return {
            'Stock Symbol': ticker_symbol,
            'Company Name': company_name,
            'Next Earnings Report Date': next_earnings_date.strftime('%Y-%m-%d') if isinstance(next_earnings_date, pd.Timestamp) else 'N/A',
            'Dividend Offered': dividend_offered,
            'Ex-Dividend Date': ex_dividend_date if ex_dividend_date else 'N/A',
            'Annual Dividend Yield': f"{annual_dividend_yield:.2f}%" if annual_dividend_yield else 'N/A'
        }

    except Exception as e:
        print(f"Failed to process {ticker_symbol}: {str(e)}")
        return {
            'Stock Symbol': ticker_symbol,
            'Company Name': 'N/A',
            'Next Earnings Report Date': 'N/A',
            'Dividend Offered': 'N/A',
            'Ex-Dividend Date': 'N/A',
            'Annual Dividend Yield': 'N/A'
        }

def main():
    stock_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", 
                     "TSLA", "META", "V", "JPM", 
                     "DIS", "NFLX"]

    results = []
    total_stocks = len(stock_symbols)
    
    for idx, symbol in enumerate(stock_symbols, 1):
        try:
            info = get_stock_info(symbol)
            results.append(info)
            print(f"Processed {symbol} ({idx}/{total_stocks})")
        except Exception as e:
            print(f"Failed to process {symbol}: {str(e)}")
            continue

    if results:
        df = pd.DataFrame(results)
        try:
            df.to_csv("stock_data.csv", index=False)
            print("Data saved to stock_data.csv")
        except Exception as e:
            print(f"Error saving to CSV: {str(e)}")
    else:
        print("No data was collected")

if __name__ == "__main__":
    main()