# modules/data_aggregation.py
import pandas as pd
import numpy as np
from modules.utilities import setup_logging
from modules.data_loading import (        
    get_nearest_sunday    
    )
from modules.config import metrics_configurations


logger = setup_logging(__name__)

# Unlock configurations using metrics_config_key 
CONFIGURATIONS = metrics_configurations()


def filter_recent_dates(df, num_recent_dates=2, date_column='date'):
    """ 
    Filter a DataFrame to return data for the most recent dates.
    
    Parameters:
    df (pandas.DataFrame): Input DataFrame
    num_recent_dates (int): Number of most recent dates to return data for (default is 2)
    date_column (str): Name of the column containing date information (default is 'date')
    
    Returns:
    pandas.DataFrame: Filtered DataFrame containing data for the most recent dates
    """
    # Make a copy of the DataFrame to avoid modifying the original
    df = df.copy()

    # Get the specified number of most recent dates
    recent_dates = df[date_column].sort_values(ascending=False).unique()[:num_recent_dates]
    logger.info(f"Two most recent dates: {recent_dates}")
    
    # Filter the DataFrame for those dates
    filtered_df = df[df[date_column].isin(recent_dates)].copy()
    
    logger.info(f"Returning data for the {num_recent_dates} most recent dates: {recent_dates}")
    logger.info(f"Number of rows in filtered data: {len(filtered_df)}")
    
    return filtered_df


def calc_dollar_and_prct_change(df, groupby_cols, value_cols=['median_price', 'mean_price'], date_col='date', ascending=True):
    """
    Calculate rolling dollar and percentage changes for specified columns in a grouped DataFrame.

    Parameters:
    df (pandas.DataFrame): The input DataFrame.
    groupby_cols (list): List of column names to group by.
    value_cols (list): List of column names for which to calculate changes.
    date_col (str): Name of the date column to sort by. Default is 'date'.
    ascending (bool): Whether to sort dates in ascending order. Default is True.

    Returns:
    pandas.DataFrame: The input DataFrame with additional columns for dollar and percentage changes.
    """
    # Create a copy of the input DataFrame to avoid modifying the original
    result_df = df.copy()

    # Sort the DataFrame by groupby columns and date
    sort_cols = groupby_cols + [date_col]
    result_df = result_df.sort_values(sort_cols, ascending=ascending)

    # Group the DataFrame
    grouped = result_df.groupby(groupby_cols)

    # Calculate the rolling dollar and percentage changes
    for col in value_cols:
        # Calculate dollar change
        result_df[f'{col}_$_change'] = grouped[col].diff().round(2)
        
        # Calculate percentage change
        result_df[f'{col}_%_change'] = (grouped[col].pct_change() * 100).round(2)

    return result_df


def agg_func(x):
    x = x.dropna() # Drop missing values
    return ', '.join(map(str, x.unique()))

def mode_agg(x):
    return x.mode().iloc[0] if not x.mode().empty else np.nan

def first_non_null(series):
    non_null_values = series.dropna()
    return non_null_values.iloc[0] if not non_null_values.empty else pd.NA


def aggregation_active_listings(df, groupby_cols, freq, condition):
    """
    Process the DataFrame by grouping, resampling, and calculating statistics.

    Parameters:
    df (pandas.DataFrame): The input DataFrame.
    groupby_cols (list): List of columns to group by.
    freq (str): Resampling frequency (e.g., '1W' for one week).
    condition (str): Condition to add as a new column.    

    Returns:
    pandas.DataFrame: Processed DataFrame with specified columns and order.
    """
    grouped = df.groupby(groupby_cols).resample(freq, on='date', label='right', closed='right').agg(
        currency=pd.NamedAgg(column='currency', aggfunc='first'),
        median_price = pd.NamedAgg(column='price', aggfunc='median'), 
        mean_price = pd.NamedAgg(column='price', aggfunc=lambda x: x.mean().round(2)),        
        high = pd.NamedAgg(column='price', aggfunc='max'),
        low = pd.NamedAgg(column='price', aggfunc='min'),
        count = pd.NamedAgg(column='price', aggfunc='count'),      
        ).reset_index()

    
    grouped['condition'] = condition

    # Calculate the rolling dollar and percentage changes
    # The below operations are separate from the code block above because they must be performed outside the .agg() method

    grouped = calc_dollar_and_prct_change(grouped, groupby_cols, value_cols=['median_price', 'mean_price'], ascending=True)

    # replaced with the function above (testing)
    # for col in ['median_price', 'mean_price']:
    #     grouped[col + '_$_change'] = grouped.groupby(groupby_cols)[col].transform(lambda x: x.diff().round(2))  
    #     grouped[col + '_%_change'] = grouped.groupby(groupby_cols)[col].transform(lambda x: (x.pct_change() * 100).round(2))
        
    
    # We filter for the date of the most recent sunday, which is the date of interest.
    # The only reason rows dated 7 days before the most recent sunday were in the dataset was to calculate the dollar difference
    # and the percentage change difference between last sunday and the most recent sunday.
    grouped = filter_recent_dates(grouped, num_recent_dates=1, date_column='date')
    grouped = grouped.reset_index(drop=True)

    
   # Reorder the columns for better readability  
    column_order =[           
        "date",
        "currency",
        "median_price",
        "mean_price",    
        "high",
        "low", 
        "count",   
        "median_price_$_change",
        "mean_price_$_change",
        "median_price_%_change",
        "mean_price_%_change",
        "condition"     
    ]

    column_order = groupby_cols + column_order
    grouped = grouped[column_order]

    return grouped

### ***                                           *** ###
### ** FORWARD FILL WITHIN & OUTER EXTEND AND FILL ** ###
### ***                                           *** ###

def selective_ffill(group, columns_to_ffill):
    """
    Selectively apply forward fill (ffill) to specific columns within a group.

    This function is designed to be used with pandas' groupby().apply() method.
    It performs a forward fill operation on specified columns within each group,
    propagating the last valid observation forward to next valid observation.

    Parameters:
    -----------
    group : pandas.DataFrame
        A DataFrame representing a group of data. This is typically a subset
        of a larger DataFrame, grouped by one or more columns.

    columns_to_ffill : list of str
        A list of column names to which the forward fill operation should be applied.
        Only these specified columns will be affected by the ffill operation.

    Returns:
    --------
    pandas.DataFrame
        A DataFrame of the same shape as the input group, with forward fill
        applied to the specified columns.

    Example:
    --------
    >>> df = pd.DataFrame({
    ...     'brand': ['A', 'A', 'B', 'B'],
    ...     'reference_number': ['1', '1', '2', '2'],
    ...     'date': ['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-02'],
    ...     'price': [100, None, 200, None],
    ...     'stock': [10, None, 20, None]
    ... })
    >>> columns_to_ffill = ['price', 'stock']
    >>> result = df.groupby(['brand', 'reference_number']).apply(
    ...     selective_ffill, 
    ...     columns_to_ffill=columns_to_ffill
    ... ).reset_index(drop=True)

    Notes:
    ------
    - This function modifies only the specified columns within each group.
    - Other columns in the group remain unchanged.
    - The function is particularly useful when you need to fill missing values
      in specific columns while preserving the original values in others.
    """
    group[columns_to_ffill] = group[columns_to_ffill].ffill()
    return group


def extend_and_ffill(group, end_date, columns_to_ffill=None):
    """
    Extends the date range of a group to a specified end date and forward fills missing data.

    This function is designed to work with grouped time series data, specifically for
    luxury watch price tracking. It extends the date range of each group up to the
    specified end date (default is the nearest Sunday to the current date) and
    forward fills any missing data points.

    Parameters:
    -----------
    group : pandas.DataFrame
        A DataFrame representing a group of data for a specific watch model
        (brand and reference number combination). Expected to have a 'date' column
        and various price-related columns.

    end_date : datetime.date, optional
        The date to extend the group's data to. Example: The nearest Sunday
        to the current date, which should be pre-calculated and passed in
        using the get_nearest_sunday() function from the modules/data_loading.py module.
    
    columns_to_ffill : list of str, optional
        List of column names to forward fill. If None, all columns will be forward filled

    Returns:
    --------
    pandas.DataFrame
        A DataFrame with the same columns as the input group, but with the date
        range extended to the specified end_date and all missing values
        forward filled.

    Notes:
    ------
    - If the group's last date is already equal to or later than the end_date,
      the function returns the group unchanged.
    - The function assumes weekly data frequency and uses 'W' (week) as the
      frequency for date range generation.
    - All columns in the original group are forward filled, carrying the last
      known values forward to fill gaps.

    Example:
    --------
    >>> current_date = datetime.now().date()
    >>> nearest_sunday = get_nearest_sunday(current_date)
    >>> extended_data = df.groupby(['brand', 'reference_number']).apply(extend_and_ffill, end_date=nearest_sunday)
    """
    last_date = group['date'].max()
    if last_date.date() == end_date:
        return group

    date_range = pd.date_range(start=last_date + pd.Timedelta(days=1), end=end_date, freq='W')
    extended_df = pd.DataFrame({'date': date_range})
    merged = pd.concat([group, extended_df]).sort_values('date')
    
    if columns_to_ffill is None:
        # If no specific columns are provided, forward fill all columns
        filled = merged.ffill()
    else:
        # Forward fill only the specified columns
        filled = merged.copy()
        filled[columns_to_ffill] = filled[columns_to_ffill].ffill()    

    return filled

def within_series_missing_values_test(df, groupby_columns=['brand', 'reference_number'], value_column='median_price'):
    """
    Calculate the number of missing values within each group for a specified column.

    This can be used to shed light to what extend does the groups in the dataframe
    need techniques such as forward fill to fill missing values within the series 
    of each group. 

    Parameters:
    df (pandas.DataFrame): The input DataFrame
    groupby_columns (list, optional): List of column names to group by. 
                                      Default is ['brand', 'reference_number'].
    value_column (str, optional): The column name to check for missing values. 
                                  Default is 'median_price'.

    Returns:
    pandas.DataFrame: A DataFrame with the count of missing values for each group

    Example:
    >>> result = within_series_missing_values_test(df)
    >>> result = within_series_missing_values_test(df, ['category', 'model'], 'price')
    """
    missing_values_by_group = (df.groupby(groupby_columns)[value_column]
                                 .apply(lambda x: x.isna().sum())
                                 .reset_index(name='missing_values'))

    missing_values_by_group = missing_values_by_group.sort_values(['missing_values'], ascending=[False])\
                                 .reset_index(drop=True)
    
    return missing_values_by_group


def out_of_series_missing_values_test(df, groupby_columns=['brand', 'reference_number'], date_column='date'):
    """
    Helps diagnose if groups within a dataframe need treatment for forward fill of 
    missing values to catch up to the current date of the dataframe. 

    Group the DataFrame by specified columns and find the latest date for each group.

    This function groups the input DataFrame by the specified columns and identifies
    the most recent date for each group. It then returns both the grouped latest dates
    and a list of distinct latest dates across all groups.

    Parameters:
    df (pandas.DataFrame): The input DataFrame containing the data.
    groupby_columns (list): A list of column names to group by.
    date_column (str, optional): The name of the date column. Defaults to 'date'.

    Returns:
    tuple: A tuple containing two elements:
        - pandas.DataFrame: A DataFrame with the latest dates for each group.
        - numpy.ndarray: An array of distinct latest dates across all groups.

    Example:
    >>> import pandas as pd
    >>> df = pd.DataFrame({
    ...     'brand': ['A', 'A', 'B', 'B'],
    ...     'reference_number': [1, 1, 2, 2],
    ...     'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']
    ... })
    >>> latest_dates, distinct_dates = get_latest_dates(df, ['brand', 'reference_number'])
    >>> print(latest_dates)
    >>> print(distinct_dates)

    Example 2:    
    >>> latest_dates, distinct_dates = get_latest_dates(wcc_wkly_price_df, ['brand', 'reference_number'])

    """
    # Group by specified columns and find the latest date for each group
    latest_dates = df.groupby(groupby_columns)[date_column].max().reset_index()

    # Extract distinct latest dates
    distinct_latest_dates = latest_dates[date_column].unique()

    return distinct_latest_dates


def apply_extend_and_forward_fill(
        combined_data: pd.DataFrame, 
        metrics_config_key: str
    ) -> pd.DataFrame:
    """
    - It would be uncommon for the dataframe to need forward-fill within the date boundaries of the dataframe.
    Because, we fetch proccessed historical data from rds. 

    - But, it should be expected for the dataframe to need extend and fill outside the date boundaries of the dataframe.
    Because this weeks data is unlikely to contain all reference numbmers in historical data.
    For watch models without any listings in the current week we fill forward to cover the gaps.

    Parameters
    ----------
    combined_data : pd.DataFrame
        Input DataFrame containing time series data
    metrics_config_key : str
        Key to retrieve columns list from FFILL_COLUMNS config
        Valid options: 'price_metrics', 'inventory_metrics', 'basic_info'   

    Returns
    -------
    pd.DataFrame
        Extended and forward-filled DataFrame
    """
    
    max_date = combined_data["date"].max().date()
    nearest_sunday = get_nearest_sunday(max_date)
    logger.info(f"{nearest_sunday} is the latest date in the combined_data dataframe")     

    columns_to_ffill = CONFIGURATIONS[metrics_config_key]['ffill_columns']
    if metrics_config_key == 'brand_metrics_config_key':
        group_by_columns = CONFIGURATIONS[metrics_config_key]['groupby_1_columns']     
    else:
        group_by_columns = CONFIGURATIONS[metrics_config_key]['groupby_2_columns']

    logger.info("Starting Extend and Fill")

    extended_and_ffilled = (combined_data
                    .groupby(group_by_columns)
                    .apply(extend_and_ffill, 
                           end_date=nearest_sunday, 
                           columns_to_ffill=columns_to_ffill, 
                           include_groups=False)
                    .reset_index(level=group_by_columns)
    )
    
    out_of_bounds_test = out_of_series_missing_values_test(
        extended_and_ffilled, 
        groupby_columns=group_by_columns, 
        date_column='date'
    )
    logger.info(f" Out of bounds test: {out_of_bounds_test}")
    
    logger.info(f"""
        Initially there were {len(combined_data)} rows in the combined df.
        After extend and ffilled we have {len(extended_and_ffilled)} rows.
        """
    )
    return extended_and_ffilled

### ***                           *** ###
### ** ROLLING AVERAGE CALCULATIONs ** ###
### ***                           *** ###


def incremental_calc_rolling_avg(group):
    """
    Created to be used when processing data with the goal of adding data incrementally to
    tables such as the "metrics.wcc_wkl_prices_and_stats_mixed" in RDS database.

    Calculate the 5-week rolling average of median prices for a group of watch data.

    This function is designed to be used with pandas' groupby.apply() method. It calculates
    the rolling average only for rows where it's needed (identified by the 'needs_rolling_avg'
    column), optimizing performance for incremental updates to the dataset.

    Parameters:
    -----------
    group : pandas.DataFrame
        A grouped DataFrame containing data for a specific watch model. 
        Expected columns:
        - 'needs_rolling_avg': boolean, indicates whether the row needs a rolling average calculation
        - For example created like: 
        >>> df['needs_rolling_avg'] = df['rolling_avg_of_median_price'].isna()
        - 'median_price': float, the median price for the watch
        - 'rolling_avg_of_median_price': float, the column to be filled with rolling averages

    Returns:
    --------
    pandas.DataFrame
        The input DataFrame with the 'rolling_avg_of_median_price' column updated for rows
        that needed calculation.

    Notes:
    ------
    - The function assumes the data is sorted by date within each group.
    - For example:
    >>> df = df.sort_values(['brand', 'reference_number', 'date'], ascending=[True, True, True]).reset_index(drop=True)
    - It uses a 5-week window for the rolling average, with a minimum of 1 period.
    - For efficiency, it only calculates new values for rows where 'needs_rolling_avg' is True.
    - When calculating new values, it includes up to 5 previous rows to ensure accurate continuation
      of the rolling average.
    """
    mask = group['needs_rolling_avg']
    if mask.any():
        # Find the last row where we have a rolling average
        last_valid_index = group[~mask].index.max()

        if pd.isna(last_valid_index):
            # If all rows need calculation, calculate for the entire group
            rolling_avg = group['median_price'].rolling(window=5, min_periods=1).mean().round(2)
            group.loc[mask, 'rolling_avg_of_median_price'] = rolling_avg.loc[mask]
        else:
            # Calculate only for the rows after the last valid rolling average
            new_data = group.loc[group.index > last_valid_index]
            if not new_data.empty:
                # Get the 5 previous rows (including the last valid one) to calculate the first new rolling average
                prev_data = group.loc[group.index <= last_valid_index].tail(5)
                calc_data = pd.concat([prev_data, new_data])

                rolling_avg = calc_data['median_price'].rolling(window=5, min_periods=1).mean().round(2)
                group.loc[new_data.index, 'rolling_avg_of_median_price'] = rolling_avg.tail(len(new_data))

    return group


def apply_incremental_calc_rolling_avg(
    extended_and_ffilled: pd.DataFrame,
    metrics_config_key: str    
    ) -> pd.DataFrame:
    """
    Is a DAG facing function.
    
    Calculate rolling averages for price metrics based on specified grouping configuration.

    This function is designed to work with Airflow tasks for incremental updates to price
    metrics tables. It processes data that has been extended and forward-filled, calculating
    rolling averages only where needed.

    Parameters
    ----------
    extended_and_ffilled : pd.DataFrame
        Input DataFrame containing extended and forward-filled data with columns:
        - date: DateTime of the record
        - brand: Watch brand
        - Additional grouping columns based on metrics_config_key
        - median_price: Median price for the group
        - rolling_avg_of_median_price: Column to store rolling averages (may contain NaN)
    metrics_config_key : str
        Key to access grouping configuration from CONFIGURATIONS dict.
        Valid options:
        - 'ref_num_metrics_config_key': Reference number level metrics
        - 'specific_mdl_metrics_config_key': Specific model level metrics
        - 'parent_mdl_metrics_config_key': Parent model level metrics
        - 'brand_metrics_config_key': Brand level metrics

    Returns
    -------
    pd.DataFrame
        DataFrame with calculated rolling averages, containing:
        - All original columns
        - Updated 'rolling_avg_of_median_price' column
        - 'needs_rolling_avg' column removed

    Notes
    -----
    - Uses configuration from CONFIGURATIONS dictionary for grouping columns
    - Sorts data by 3-column grouping (e.g., ['brand', 'reference_number', 'date'])
    - Calculates rolling averages only for rows where needed (where rolling_avg_of_median_price is NaN)
    - Uses incremental_calc_rolling_avg function for the actual rolling average calculation
    - Maintains data integrity by processing each group independently

    Example
    -------
    >>> config_key = 'specific_mdl_metrics_config_key'
    >>> df_with_rolling_avg = apply_incremental_calc_rolling_avg(input_df, config_key)
    
    See Also
    --------
    incremental_calc_rolling_avg : Function used for group-wise rolling average calculation
    """
    logger.info("Starting rolling average calculation")

    if metrics_config_key == 'brand_metrics_config_key':
        group_by_columns_a = CONFIGURATIONS[metrics_config_key]['groupby_1_columns']
        group_by_columns_b = CONFIGURATIONS[metrics_config_key]['groupby_2_columns']
    else:
        group_by_columns_a = CONFIGURATIONS[metrics_config_key]['groupby_2_columns']
        group_by_columns_b = CONFIGURATIONS[metrics_config_key]['groupby_3_columns']
    
    # Sorting the dataframe by date to ensure correct rolling calculation
    extended_and_ffilled.sort_values(group_by_columns_b, ascending=True, inplace=True)
    extended_and_ffilled.reset_index(drop=True, inplace=True)
    # Searches for rows with missing values in the rolling_avg_of_median_price column and marks the rows with a True or False value.
    # Marking rows needing rolling average calculation
    extended_and_ffilled['needs_rolling_avg'] = extended_and_ffilled['rolling_avg_of_median_price'].isna()

    # Apply the incremental rolling average calculation to each group
    with_rolling_avg_calc = (extended_and_ffilled.groupby(group_by_columns_a)
                             .apply(incremental_calc_rolling_avg, include_groups=False)
                             .reset_index(level=group_by_columns_a)
    )

    # Drop the temporary 'needs_rolling_avg' column
    with_rolling_avg_calc.drop('needs_rolling_avg', axis=1, inplace=True)
    logger.info("Rolling average calculation completed")
    
    return with_rolling_avg_calc


### ***              *** ###
### ** $ AND % CHANGE ** ###
### ***              *** ###

def calculate_pct_dol_changes_for_date(combined_df, current_date, days_offset=30, groupby_columns=['brand', 'reference_number']):
    """
    Calculate percentage and dollar changes in rolling average prices for luxury watches,
    comparing the current date to a specified historical point.

    This function processes a DataFrame of watch prices, grouped by brand and reference number.
    It calculates changes between the current date's rolling average price and a historical price
    determined by the specified days offset. The function handles various time periods and
    accounts for potentially insufficient historical data.

    Parameters:
    -----------
    combined_df : pandas.DataFrame
        A DataFrame containing historical price data for luxury watches.
        Expected columns:
        - 'date': Date of the price observation (datetime or string in 'YYYY-MM-DD' format)
        - 'brand': Watch brand name
        - 'reference_number': Watch reference number
        - 'rolling_avg_price': Rolling average price for each date

    current_date : str
        The date for which to calculate changes, in 'YYYY-MM-DD' format.

    days_offset : int, optional (default=30)
        Number of days to look back for price comparison. Common values:
        30 (monthly), 90 (quarterly), 180 (semi-annually), 365 (annually).
        The function converts this to the nearest number of weeks.
    
    groupby_columns : list, optional (default=['brand', 'reference_number'])
        A list of columns to group by before calculations. The order of the columns here does not matter.

    Returns:
    --------
    pandas.DataFrame
        A copy of the input DataFrame with two additional columns:
        - 'percent_change': Percentage change in price (float or None)
        - 'dollar_change': Absolute dollar change in price (float or None)
        These columns will have values only for rows matching the current_date.
        None values indicate insufficient historical data for calculation.

    Notes:
    ------
    - The function groups data by 'brand' and 'reference_number' before calculations.
    - It uses a weekly offset derived from days_offset, rounding to the nearest week.
    - For standard periods (30, 90, 180, 365 days), it uses predefined week offsets.
    - Calculations return None if there's insufficient historical data.
    - The original DataFrame is not modified; a new copy is returned.

    Example:
    --------
    >>> df = pd.DataFrame({
    ...     'date': ['2024-08-25', '2024-08-25', '2024-07-28', '2024-07-28'],
    ...     'brand': ['Rolex', 'Patek', 'Rolex', 'Patek'],
    ...     'reference_number': ['116500', '5711', '116500', '5711'],
    ...     'rolling_avg_price': [10000, 20000, 9500, 19000]
    ... })
    >>> result = calculate_changes_for_date(df, '2024-08-25', days_offset=30)
    >>> print(result[result['date'] == '2024-08-25'])
    """
    # Define common financial periods
    financial_periods = {
        7: '1w',    # Weekly (1 week)
        30: '1m',    # Monthly (5 weeks)
        90: '3m',   # Quarterly (13 weeks)
        180: '6m',  # Semi-annually (26 weeks)
        365: '1y',  # Annually (52 weeks)
    }

    # Determine the number of weeks to offset
    if days_offset == 30:
        weeks_offset = 5 # In financial markets the monthly period in weeks is represented by 5 wks. 
        col_prefix = financial_periods[days_offset]
    elif days_offset in financial_periods:
        weeks_offset = round(days_offset / 7)
        col_prefix = financial_periods[days_offset]
    else:
        weeks_offset = round(days_offset / 7)
        col_prefix = f"{days_offset}d"

    # Create column names
    pct_col = f"pct_change_{col_prefix}"
    dol_col = f"dol_change_{col_prefix}"

    # Ensure the date column is datetime
    combined_df['date'] = pd.to_datetime(combined_df['date'])

    combined_df['rolling_avg_of_median_price'] = combined_df['rolling_avg_of_median_price'].astype(float)

    # Sort the DataFrame and create a copy to avoid SettingWithCopyWarning
    df_sorted = combined_df.sort_values(groupby_columns + ['date']).copy()

    # The function creates a boolean mask 'current_date_mask'that identifies rows matching the specified current_date.
    current_date_mask = df_sorted['date'] == pd.to_datetime(current_date)

    # Group by brand and reference number
    grouped = df_sorted.groupby(groupby_columns)


    def calc_changes(group):
        """
        The calc_changes function is applied to each group (grouped by brand and reference number).
        It calculates the percent and dollar changes only for the rows matching the current_date
        """
        current_date_rows = group[group['date'] == pd.to_datetime(current_date)]
        if current_date_rows.empty:
            return pd.Series({pct_col: None, dol_col: None})
            

        current_price = float(current_date_rows['rolling_avg_of_median_price'].iloc[0])

        # Check if we have enough history for the offset
        if len(group) <= weeks_offset:
            return pd.Series({pct_col: None, dol_col: None})
            
        # .iloc[-1]: This selects the last element of the resulting shifted series.
        lagged_price = float(group['rolling_avg_of_median_price'].shift(weeks_offset).iloc[-1])

        # Handle the case where lagged_price is zero
        if lagged_price == 0:
            percent_change = None
        else:
            percent_change = (current_price - lagged_price) / lagged_price * 100
            percent_change = round(percent_change, 2)
            
        dollar_change = current_price - lagged_price
        dollar_change = round(dollar_change, 2)

        return pd.Series({pct_col: percent_change, dol_col: dollar_change})
        

    # Apply the calculation to each group
    changes = grouped.apply(calc_changes)

    # The calculated changes are then assigned only to the rows matching the current_date_mask
    # Merging the changes back to the original DataFrame
    # If the columns dont exist, they are created implicitly here
    df_sorted.loc[current_date_mask, [pct_col, dol_col]] = changes.values    

    return df_sorted


def process_multiple_offsets(df, current_date, offsets, groupby_columns=['brand', 'reference_number']):
    """
    Process a DataFrame to calculate percentage and dollar changes for multiple time offsets.

    This function applies the calculate_pct_dol_changes_for_date() function iteratively
    for each specified offset, accumulating the results in a single DataFrame.

    Parameters:
    -----------
    df : pandas.DataFrame
        The input DataFrame containing historical price data for luxury watches.
        Expected to have columns: 'date', 'brand', 'rolling_avg_of_median_price' and 
        a variety of other columns (ex: 'reference_number', 'parent_model', or 'specific_model') 
        depending on the desired groupby operations.

    current_date : str or datetime.date
        The reference date for which to calculate changes, typically the most recent date in the dataset.
        If string, should be in 'YYYY-MM-DD' format.

    offsets : list of int
        A list of day offsets for which to calculate changes. Common values include:
        [7, 30, 90, 180, 365] for weekly, monthly, quarterly, semi-annual, and annual changes.

    groupby_columns : list, optional (default=['brand', 'reference_number'])
        A list of columns to group by before calculations. The order of the columns here does not matter.

    Returns:
    --------
    pandas.DataFrame
        A copy of the input DataFrame with additional columns for each offset:
        - '{prefix}_pct_change': Percentage change in price
        - '{prefix}_dol_change': Absolute dollar change in price
        where {prefix} is determined by the offset (e.g., '1w' for 7 days, '1m' for 30 days, etc.)

    Notes:
    ------
    - The function uses calculate_pct_dol_changes_for_date() for each offset calculation.
    - Changes are calculated only for the rows matching the current_date.
    - The original DataFrame is not modified; a new copy with additional columns is returned.
    - For offsets not corresponding to standard periods (7, 30, 90, 180, 365 days),
      column prefixes will be in the format 'Xd' where X is the number of days.

    Example:
    --------
    >>> df = pd.DataFrame({
    ...     'date': ['2024-08-25', '2024-08-25', '2024-07-28', '2024-07-28'],
    ...     'brand': ['Rolex', 'Patek', 'Rolex', 'Patek'],
    ...     'reference_number': ['116500', '5711', '116500', '5711'],
    ...     'rolling_avg_of_median_price': [10000, 20000, 9500, 19000]
    ... })
    >>> current_date = '2024-08-25'
    >>> offsets = [7, 30, 90]
    >>> result = process_multiple_offsets(df, current_date, offsets)
    >>> print(result.columns)
    Index(['date', 'brand', 'reference_number', 'rolling_avg_of_median_price',
           '1w_pct_change', '1w_dol_change', '1m_pct_change', '1m_dol_change',
           '3m_pct_change', '3m_dol_change'], dtype='object')
    """
    result_df = df.copy()
    for offset in offsets:
        result_df = calculate_pct_dol_changes_for_date(result_df, current_date, days_offset=offset, groupby_columns=groupby_columns)
        
    return result_df


def apply_dollar_and_pct_change_calc(
        with_rolling_avg_calc: pd.DataFrame,
        metrics_config_key: str)->pd.DataFrame:
    """
    A DAG facing function. 

    Apply to dataframe after having calculated the rolling average.

    Calculate dollar and percentage changes for price metrics over various time periods.

    Parameters
    ----------
    with_rolling_avg_calc : pd.DataFrame
        Input DataFrame containing rolling averages with columns:
        - date: DateTime of the record
        - brand: Watch brand
        - Additional grouping columns based on metrics_config_key
        - median_price: Median price for the group
        - rolling_avg_of_median_price: Rolling average of median prices
    metrics_config_key : str
        Key to access grouping configuration from CONFIGURATIONS dict.
        Valid options:
        - 'ref_num_metrics_config_key': Reference number level metrics
        - 'specific_mdl_metrics_config_key': Specific model level metrics
        - 'parent_mdl_metrics_config_key': Parent model level metrics
        - 'brand_metrics_config_key': Brand level metrics

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame containing only the most recent Sunday's data with
        calculated price changes for different time periods:
        - 7-day changes (week)
        - 30-day changes (month)
        - 90-day changes (quarter)
        - 180-day changes (half-year)
        - 365-day changes (year)

    Notes
    -----
    - Uses process_multiple_offsets() to calculate changes for different periods
    - Groups data according to configuration specified by metrics_config_key
    - Returns only the most recent Sunday's data
    - Sorts output by grouping columns specified in configuration
    """    
    if metrics_config_key == 'brand_metrics_config_key':
        group_by_columns = CONFIGURATIONS[metrics_config_key]['groupby_1_columns']
    else:
        group_by_columns = CONFIGURATIONS[metrics_config_key]['groupby_2_columns']

    logger.info("Calculating dollar and percentage changes")
    min_date = with_rolling_avg_calc["date"].min().date()
    max_date = with_rolling_avg_calc["date"].max().date()
    current_date = get_nearest_sunday(max_date)
    
    logger.info(f"The input dataframe ranges from {min_date} to {max_date}")
    
    offsets = [7, 30, 90, 180, 365] # In days ex: 7 days, 30 days, 90 days, 180 days, 365 days
    final_df = process_multiple_offsets(with_rolling_avg_calc, current_date, offsets, groupby_columns=group_by_columns)
    
    # The filtered df contain the rows that will be loaded to the RDS table. 
    final_filtered = final_df[final_df['date'] == pd.to_datetime(current_date)
                    ].sort_values(group_by_columns, ascending=True).reset_index(drop=True)
    
    logger.info(f"Final filtered dataframe has {len(final_filtered)} rows")
    logger.info(f"Number of rows to be uploaded: {len(final_filtered)}")
    
    return final_filtered


### ***                               *** ###
### ** Market Makeup Related Functions ** ###
### ***                               *** ###

def market_makeup_condition_stats(df, groupby_columns):
    """
    Calculate statistics for new and pre-owned watch listings.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame containing watch listings data
    groupby_columns (list): Columns to group by
    
    Returns:
    pd.DataFrame: Statistics DataFrame with new and pre-owned counts and percentages
    """
    # Create condition masks
    new_mask = df['condition'] == 'New'
    pre_owned_mask = df['condition'].isin(['Pre-Owned', 'Pre-Owned, Seller Refurbished', 'Pre-Owned, Vintage'])

    # Group and aggregate
    market_makeup_stats_df = df.groupby(groupby_columns).agg(
        numb_of_new_listings=('condition', lambda x: np.sum(new_mask[x.index])),
        numb_of_pre_owned_listings=('condition', lambda x: np.sum(pre_owned_mask[x.index]))
    )

    # Calculate total listings 
    market_makeup_stats_df['numb_of_total_listings']  = market_makeup_stats_df['numb_of_new_listings'] + market_makeup_stats_df['numb_of_pre_owned_listings']
    
    # Calculate and percentages and round to 2 decimal places
    market_makeup_stats_df['pct_new'] = np.round((market_makeup_stats_df['numb_of_new_listings'] / market_makeup_stats_df['numb_of_total_listings']) * 100, 2)
    market_makeup_stats_df['pct_pre_owned'] = np.round((market_makeup_stats_df['numb_of_pre_owned_listings'] / market_makeup_stats_df['numb_of_total_listings']) * 100, 2)
    
    # Calculate Listings Popularity Score
    max_listings = market_makeup_stats_df['numb_of_total_listings'].max()
    market_makeup_stats_df['listings_popularity_score'] = np.round((market_makeup_stats_df['numb_of_total_listings'] / max_listings) * 100, 2)
    
    # Reset index and reorder columns
    result_columns = groupby_columns + ['pct_new', 'pct_pre_owned', 'numb_of_total_listings', 'numb_of_new_listings', 'numb_of_pre_owned_listings', 'listings_popularity_score']
    market_makeup_stats_df = market_makeup_stats_df.sort_values(['date', 'numb_of_total_listings' ], ascending=[True, False]).reset_index()[result_columns].copy()

    return market_makeup_stats_df