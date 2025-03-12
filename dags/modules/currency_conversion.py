# modules/currency_conversion.py
import freecurrencyapi
import os
import pandas as pd

def get_exchange_rates(base_currency='USD', currencies=None):
    """
    Retrieve the latest exchange rates from the freecurrencyapi.

    This function fetches the latest exchange rate data, allowing for customization
    of the base currency and target currencies.

    Parameters:
    -----------
    base_currency : str, optional
        The base currency for the exchange rates (default is 'USD').
    currencies : list of str, optional
        A list of currency codes to retrieve. If None, all available currencies will be returned.

    Returns:
    --------
    dict
        A dictionary containing the latest exchange rate data. The structure is:
        {
            'data': {
                'CUR1': rate1,
                'CUR2': rate2,
                ...
            }
        }

    Raises:
    -------
    ValueError
        If the API key is not found or if there's an issue with the input parameters.
    requests.exceptions.RequestException
        For any issues with the API request.

    Examples:
    ---------
    >>> result_all = get_exchange_rates()
    >>> result_specific = get_exchange_rates(currencies=['EUR', 'CAD', 'JPY'])

    Notes:
    ------
    - Requires a valid API key from freecurrencyapi set as an environment variable 'CURRENCY_API_KEY'.
    - The API has limitations of 5,000 requests per month, at basic subscription level.
    """
    CURRENCY_API_KEY = os.getenv('CURRENCY_API_KEY')
    client = freecurrencyapi.Client(CURRENCY_API_KEY)

    # Prepare the API request parameters
    params = {
        'base_currency': base_currency,        
    }
    
    # Add the 'currencies' parameter only if it is provided
    if currencies:
        params['currencies'] = currencies

    exchange_rates = client.latest(**params)
    
    return exchange_rates


def get_historical_exchange_rates(date, base_currency='USD', currencies=None):
    """
    Retrieve historical exchange rates from the freecurrencyapi.

    This function fetches historical exchange rate data for a specific date,
    allowing for customization of the base currency and target currencies.

    Parameters:
    -----------
    date : str
        The date for which to retrieve exchange rates, in the format 'YYYY-MM-DD'.
    base_currency : str, optional
        The base currency for the exchange rates (default is 'USD').
    currencies : list of str, optional
        A list of currency codes to retrieve. If None, all available currencies will be returned.

    Returns:
    --------
    dict
        A dictionary containing the historical exchange rate data. The structure is:
        {
            'data': {
                'YYYY-MM-DD': {
                    'CUR1': rate1,
                    'CUR2': rate2,
                    ...
                }
            }
        }

    
    Examples:
    ---------
    >>> result_all = get_historical_exchange_rates(date='2021-02-02')
    >>> result_specific = get_historical_exchange_rates(date='2021-02-02', currencies=['EUR', 'CAD', 'JPY'])

    Notes:
    ------
    - Historical exchange rates can be requsted going back up to 1999.
    - Requires a valid API key from freecurrencyapi set as an environment variable 'CURRENCY_API_KEY'.
    - The API has limitations of 5,000 requests per month, at basic subscription level.
    """
    # Retrieve the API key from environment variables
    CURRENCY_API_KEY = os.getenv('CURRENCY_API_KEY')
    client = freecurrencyapi.Client(CURRENCY_API_KEY)
    
    # Prepare the API request parameters
    params = {
        'base_currency': base_currency,
        'date': date,
    }
    
    # Add the 'currencies' parameter only if it is provided
    if currencies:
        params['currencies'] = currencies  

    # Make the API call   
    historical_exchange_rates = client.historical(**params)
    
    return historical_exchange_rates


def convert_to_usd(row, exchange_rates):
    try:
        if row['currency'] != 'USD':
            conversion_rate = exchange_rates['data'].get(row['currency'])
            if conversion_rate is None:
                raise ValueError(f"Exchange rate for {row['currency']} on row {row.name} not found in dictionary")
            row['price'] = row['price'] / conversion_rate
            row['currency'] = 'USD'
    except Exception as e:
        print(f"Error converting {row['currency']} to USD for row {row.name}: {e}")
    return row


# Using vectorize operation 
def process_dataframe(df, exchange_rates):
    unique_currencies = df['currency'].unique().tolist()
    if 'USD' in unique_currencies and len(unique_currencies) == 1:
        print("There are no foreign currency priced rows in the given dataframe")
    else:
        # Create a boolean mask for non-USD rows
        non_usd_mask = df['currency'] != 'USD'
        
        # Get the conversion rates for non-USD currencies
        non_usd_currencies = df.loc[non_usd_mask, 'currency'].unique()
        conversion_rates = {curr: exchange_rates['data'].get(curr) for curr in non_usd_currencies}
        
        # Convert prices to USD
        df.loc[non_usd_mask, 'price'] = (df.loc[non_usd_mask, 'price'] / df.loc[non_usd_mask, 'currency'].map(conversion_rates)).round(2)
        
        # Update the currency to USD
        df.loc[non_usd_mask, 'currency'] = 'USD'
        
    return df