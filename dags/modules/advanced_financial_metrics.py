# modules/advanced_financial_metrics.py

import pandas as pd
from modules.utilities import setup_logging, create_identifier
import numpy as np
from datetime import datetime

def get_min_weeks_required() -> dict[str, int]:
    """
    Get the minimum number of weeks required for a watch model to be included in the analysis.
    """
    return {
        '1M': 52, # 1 year
        '3M': 104, # 2 years
        '6M': 104, # 2 years
        '1Y': 156 # 3 years 
    }

def models_that_qualify_for_volatility_calculations(df) -> dict[str, pd.DataFrame]:
    """
    This function groups the data by brand and reference number (creating groups of watch models).
    It then filters the groups based on the historical data requirement.

    Ultimately, what we are doing is identifying which watch models have the required historical data 
    to be included in the calculations of the volatility at the different time intervals.

    Args:
        df (pd.DataFrame): A dataframe containing watch listing data.

    Returns:
        dict: A dictionary of dataframes, where each dataframe is a group of data for a watch model.
        The keys are the intervals (1 month, 3 months, 6 months, 1 year), and the values are the dataframes.
    """
    def has_required_history(group, min_weeks):
        """ 
        This function checks if a watch model (identified by its unique combination of brand and reference number) 
        has the required historical data.
        It calculates the number of weeks between the maximum and minimum dates in the group.
        If the number of weeks is greater than or equal to the minimum number of weeks required, the function returns True.
        Otherwise, it returns False.

        Args:
            group (pd.DataFrame): A group of data representing a single watch model.
            min_weeks (int): The minimum number of weeks required for a watch model to be included in the analysis.

        Returns:
            bool: True if the watch model has the required historical data, False otherwise.
        """
        date_range = group['date'].max() -  group['date'].min()
        # date_range.days gives you the total number of days between two dates
        # floor division by 7 converts days to weeks, rounding down to the nearest whole number
        weeks = date_range.days // 7 
        
        return weeks >= min_weeks

    min_weeks_required = get_min_weeks_required()

    grouped = df.groupby(['brand', 'reference_number'])

    # Filter groups based on historical data requirement
    # For each interval we check and append all watch models that meets the requirements to be under each interval
    # x is the group in question, weeks is the threshold number of weeks the group needs to have to be count in
    # has_required_history is a boolean, that if true the group is added to the dicitonary under the interval is being tested on
    filtered_groups = {interval: grouped.filter(lambda x: has_required_history(x, weeks)) for interval, weeks in min_weeks_required.items()}

    return filtered_groups


def resample_and_calculate(group, freq):
    """
    Resample time series data for a watch model group and calculate various price metrics.

    This function resamples the data at specified frequency intervals and calculates key financial 
    metrics including opening/closing prices, mean/median prices, highs/lows, percentage changes,
    and logarithmic returns.

    Args:
        group (pd.DataFrame): A DataFrame containing time series data for a specific watch model,
            must include 'date' and 'rolling_avg_of_median_price' columns.
        freq (str): The resampling frequency (e.g., '1ME' for month end, 'QE' for quarter end,
            '2QE' for 2-quarter end, 'YE' for year end).

    Returns:
        pd.DataFrame: A resampled DataFrame containing the following columns:
            - date: The resampled date
            - open_price: First price in the period
            - close_price: Last price in the period
            - mean_price: Average price in the period (rounded to 2 decimals)
            - median_price: Median price in the period
            - high: Highest price in the period
            - low: Lowest price in the period
            - pct_change: Percentage change from previous period (%)
            - log_returns: Natural logarithm of the ratio of consecutive closing prices

    Note:
        All price calculations are based on the 'rolling_avg_of_median_price' column from the input data.
        Missing values in mean_price are forward-filled before calculating percentage changes.
    """
    resampled = group.resample(freq, on='date', label='right', closed='right').agg(
        # currency=pd.NamedAgg(column='currency', aggfunc='last'),
        open_price=pd.NamedAgg(column='rolling_avg_of_median_price', aggfunc='first'),
        close_price=pd.NamedAgg(column='rolling_avg_of_median_price', aggfunc='last'),
        mean_price=pd.NamedAgg(column='rolling_avg_of_median_price', aggfunc=lambda x: round(x.mean(), 2)),
        median_price=pd.NamedAgg(column='rolling_avg_of_median_price', aggfunc='median'),
        high=pd.NamedAgg(column='rolling_avg_of_median_price', aggfunc='max'),
        low=pd.NamedAgg(column='rolling_avg_of_median_price', aggfunc='min'),
        # pct_change=pd.NamedAgg(column='rolling_avg_of_median_price', aggfunc=lambda x: (x.iloc[-1] - x.iloc[0]) / x.iloc[0])
    ).reset_index()

   # Filling NA values with the previous value before calculating the percentage change
    # Although this column should not have any missing values
    resampled['mean_price'] = resampled['mean_price'].ffill()  
    # Calculate percentage change between months    
    resampled['pct_change'] = (resampled['mean_price'].pct_change() * 100).round(2)
    
    # Create a new column 'log_returns' and initialize with NaN
    resampled['log_returns'] = np.nan

    # Calculate log returns using transform
    # resampled['log_returns'] = resampled['close_price'].transform(lambda x: np.log(x / x.shift(1)))
    resampled['close_price'] = resampled['close_price'].astype(float)
    resampled['log_returns'] = np.log(resampled['close_price'] / resampled['close_price'].shift(1))

    return resampled


def resample_and_calc_volatility_precursors(filtered_groups: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Resample the data and calculate the volatility precursors for each watch model.
    The volatility precursors are the data that is used to calculate the volatility in the next step.
    The resampling is done at the following frequencies: 1 month (1ME), 3 months (QE), 6 months (2QE), 1 year (YE).
    """
    min_weeks_required = get_min_weeks_required()

    resampled_data = {
        interval: filtered_groups[interval].groupby(['brand', 'reference_number'], group_keys=True)\
                                            .apply(resample_and_calculate, freq=freq, include_groups=False) \
                                            .reset_index(level=['brand', 'reference_number'])
                                            for interval, freq in zip(min_weeks_required.keys(), ['1ME', 'QE', '2QE', 'YE'])
    }
    return resampled_data


def calculate_volatility(group, interval):
    """
    Calculate the annualized volatility (as a percentage) for each group.

    Args:
        group (pandas.DataFrame): A group of data representing a single watch model.
        interval (str): The resampling interval (e.g., '1M', '3M', '6M', '1Y').
        
    Returns:
        pandas.Series: A Series containing the annualized volatility for each group.
    """
    annualization_factor_map = {
        '1M': 12,
        '3M': 4,
        '6M': 2,
        '1Y': 1
    }

    annualization_factor = annualization_factor_map.get(interval, 1)
    volatility = group['log_returns'].std() * np.sqrt(annualization_factor) * 100
    return round(volatility, 2)



def volatility_dict_to_df_and_format(volatility_data_result: dict[str, pd.DataFrame], date_most_recent_data_point: datetime.date) -> pd.DataFrame:
    """
    Convert a dictionary of volatility dataframes into a single formatted dataframe.

    This function combines volatility data from different time intervals (1M, 3M, 6M, 1Y) into a single
    dataframe, adds a date column, creates a unique identifier for each watch model, and standardizes
    column names for database compatibility.

    Args:
        volatility_data_result (dict[str, pd.DataFrame]): Dictionary containing volatility data for different
            time intervals. Keys are intervals ('1M', '3M', '6M', '1Y') and values are DataFrames containing
            volatility calculations for each watch model.
        date_most_recent_data_point (datetime.date): The date of the most recent data point in the dataset.

    Returns:
        pd.DataFrame: A formatted DataFrame containing:
            - brand: Watch brand name
            - reference_number: Watch reference number
            - identifier: Unique identifier combining brand and reference number
            - volatility_1m: 1-month volatility
            - volatility_3m: 3-month volatility
            - volatility_6m: 6-month volatility
            - volatility_1y: 1-year volatility
            - date: Date of the most recent data point used to calculate the volatility
    """
    # Initialize the volatility dataframe with the 1M data
    volatility_df = volatility_data_result['1M']

    for interval in ['3M', '6M', '1Y']:
        volatility_df = volatility_df.merge(volatility_data_result[interval], on=['brand', 'reference_number'], how='outer')

    volatility_df['date'] = date_most_recent_data_point
    
    # Create a new column 'identifier' that combines 'brand' and 'reference_number'
    volatility_df = create_identifier(volatility_df, 'reference_number')

    # Rename the columns for better query compatibility with postgresql database
    new_column_names_dict = {'volatility_1M':'volatility_1m', 
                'volatility_3M':'volatility_3m',
                'volatility_6M':'volatility_6m',
                'volatility_1Y':'volatility_1y'
                }
    volatility_df = volatility_df.rename(columns=new_column_names_dict)
    
    return volatility_df
    

def apply_volatility_calculations(resampled_data: dict[str, pd.DataFrame], date_most_recent_data_point: datetime.date) -> pd.DataFrame:
    """
    Calculate volatility metrics for watch models across different time intervals and format the results.

    This function processes resampled price data to calculate annualized volatility for different time
    intervals (1M, 3M, 6M, 1Y). It performs the following steps:
    1. Calculates volatility for each interval using log returns and annualization factors
    2. Formats the results into a single DataFrame with standardized column names
    3. Adds identifier column and the most recent data date column

    Args:
        resampled_data (dict[str, pd.DataFrame]): Dictionary containing resampled price data for each
            time interval. Keys are intervals ('1M', '3M', '6M', '1Y') and values are DataFrames
            containing columns: brand, reference_number, date, log_returns, and other price metrics.
        date_most_recent_data_point (datetime.date): The date of the most recent data point in the dataset.

    Returns:
        pd.DataFrame: A formatted DataFrame containing:
            - brand: Watch brand name
            - reference_number: Watch reference number
            - identifier: Unique identifier combining brand and reference number
            - volatility_1m: 1-month annualized volatility (%)
            - volatility_3m: 3-month annualized volatility (%)
            - volatility_6m: 6-month annualized volatility (%)
            - volatility_1y: 1-year annualized volatility (%)
            - date: Date of the most recent data point

    Internal Functions Used:
        - calculate_volatility(): Calculates annualized volatility for a group using log returns
        - volatility_dict_to_df_and_format(): Combines and formats volatility data from all intervals
    """
    volatility_data_result = {
        interval: df.groupby(['brand', 'reference_number'])\
                    .apply(calculate_volatility, interval=interval, include_groups=False)\
                    .reset_index()\
                    .rename(columns={0:f'volatility_{interval}'})
                    for interval, df in resampled_data.items()
    }

    volatility_df = volatility_dict_to_df_and_format(volatility_data_result, date_most_recent_data_point)

    return volatility_df


# Liquidity related functions #

def analyze_listing_durations(df):
    """
    Analyze the average duration of watch listings by model, excluding active listings.
    
    Parameters:
    df (pandas.DataFrame): DataFrame with watch listing data containing columns:
        date, brand, reference_number, watch_url, and other metadata
    
    Returns:
    pandas.DataFrame: Analysis results with average listing duration by watch model
    """
    # Create a copy of the input DataFrame to avoid modifications to original
    df = df.copy()
    
    # Convert date column to datetime if it's not already
    # df['date'] = pd.to_datetime(df['date'])
    
    # Get the latest date in the dataset
    max_date = df['date'].max()
    
    # For each watch_url, get its first and last appearance
    listing_durations = df.groupby('watch_url').agg({
        'date': ['min', 'max'],
        'brand': 'first',
        'reference_number': 'first'
    })
    
    # Flatten the multi-level columns
    listing_durations.columns = ['first_seen', 'last_seen', 'brand', 'reference_number']
    listing_durations = listing_durations.reset_index()
    
    # Create a new DataFrame for inactive listings instead of filtering in place.
    # We look only at inactive listings because we cannot calculate the duration of a listings 
    # in the marketplace if it has not closed.
    inactive_listings = listing_durations[listing_durations['last_seen'] != max_date].copy()
    
    # Get the latest date in the inactive_listings table
    max_date_il = inactive_listings['last_seen'].max()

    # Calculate durations
    inactive_listings['duration_days'] = (inactive_listings['last_seen'] - inactive_listings['first_seen']).dt.days

    # Apply the 7-day assumption for listings that appear in only one weekly scrape
    inactive_listings.loc[inactive_listings['duration_days'] == 0, 'duration_days'] = 7
    
    inactive_listings['duration_weeks'] = (inactive_listings['duration_days'] / 7).round(1) #+ 1
    
    # Group by watch model and calculate statistics
    model_stats = inactive_listings.groupby(['brand', 'reference_number']).agg({
        'duration_days': ['count', 'mean', 'median', 'std', 'min', 'max'],
        'duration_weeks': ['mean', 'median', 'std', 'min', 'max']
    }).round(2)
    
    # Flatten the multi-level columns with more readable names
    model_stats.columns = [
        'number_of_listings',
        'avg_days_listed',
        'median_days_listed',
        'std_dev_days',
        'min_days_listed',
        'max_days_listed',
        'avg_weeks_listed',
        'median_weeks_listed',
        'std_dev_weeks',
        'min_weeks_listed',
        'max_weeks_listed',
    ]
    
    # Calculate active listings count
    # active_listings_count = df[df['date'] == max_date].groupby(
    #     ['brand', 'reference_number']
    # ).size().to_frame('active_listings') 
    
    # # Merge the active listings count with model_stats
    # model_stats = model_stats.reset_index()
    # active_listings_count = active_listings_count.reset_index()
    
    # # Perform an outer merge to ensure we keep all models
    # model_stats = pd.merge(
    #     model_stats,
    #     active_listings_count,
    #     on=['brand', 'reference_number'],
    #     how='left'
    # )
    
    # # Fill NaN values in active_listings with 0
    # model_stats['active_listings'] = model_stats['active_listings'].fillna(0).astype(int)
    
    # Create identifier
    model_stats = create_identifier(model_stats, 'reference_number')
    
    # Sort by number of listings (descending) to show most common models first
    model_stats = model_stats.sort_values('number_of_listings', ascending=False).reset_index(level=['brand', 'reference_number'])
    model_stats['date'] = max_date_il
    return model_stats



def generate_detailed_report(df, model_stats):
    """
    Generate a detailed report with additional insights about listing patterns.
    
    Parameters:
    df (pandas.DataFrame): Original DataFrame with watch listing data
    model_stats (pandas.DataFrame): Results from analyze_listing_durations
    
    Returns:
    dict: Dictionary containing additional analysis and insights
    """
    max_date = df['date'].max()
    total_listings = len(df['watch_url'].unique())
    active_listings = len(df[df['date'] == max_date]['watch_url'].unique())
    inactive_listings = total_listings - active_listings
    
    report = {
        'total_unique_listings': total_listings,
        'active_listings': active_listings,
        'inactive_listings': inactive_listings,
        'total_watch_models': len(model_stats),
        'avg_listing_duration_all_models': model_stats['avg_days_listed'].mean(),
        'median_listing_duration_all_models': model_stats['median_days_listed'].mean(),
        'models_with_longest_avg_duration': model_stats.nlargest(5, 'avg_days_listed')[
            ['brand', 'reference_number', 'avg_days_listed']
        ].to_dict('records'),
        'models_with_most_listings': model_stats.nlargest(5, 'number_of_listings')[
            ['brand', 'reference_number', 'number_of_listings']
        ].to_dict('records')
    }
    
    return report