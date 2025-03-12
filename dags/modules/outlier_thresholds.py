# modules/outlier_thresholds.py

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import logging
from modules.utilities import setup_logging

from scipy import stats


logger = setup_logging(__name__)

def calculate_groupby_statistics(df, group_columns, price_column='price'):
    """
    Calculate precursor statistics for calculating outlier threshold boundaries
    for each group in the dataframe.

    Parameters:
    df (pandas.DataFrame): The input dataframe containing watch data.
    group_columns (list): Columns to group by (e.g., ['brand', 'specific_model']).
    price_column (str): The name of the column containing price data. Default is 'price'.

    Returns:
    pandas.DataFrame: A dataframe with group statistics including parent_model, count, Q1, Q3, and IQR.
    """
    return df.groupby(group_columns).agg(
        parent_model=pd.NamedAgg(column='parent_model', aggfunc='first'),
        specific_model=pd.NamedAgg(column='specific_model', aggfunc='first'),
        count=pd.NamedAgg(column=group_columns[-1], aggfunc='size'),
        # median=pd.NamedAgg(column=price_column, aggfunc='median'),
        q1=pd.NamedAgg(column=price_column, aggfunc=lambda x: round(x.quantile(0.25), 2)),
        q2=pd.NamedAgg(column=price_column, aggfunc=lambda x: round(x.quantile(0.50), 2)),
        q3=pd.NamedAgg(column=price_column, aggfunc=lambda x: round(x.quantile(0.75), 2)),
        low=pd.NamedAgg(column=price_column, aggfunc='min') ,    
        high=pd.NamedAgg(column=price_column, aggfunc='max'),        
        # mad=pd.NamedAgg(column=price_column, aggfunc=lambda x: stats.median_abs_deviation(x, scale='normal'))        
        iqr=pd.NamedAgg(column=price_column, aggfunc=lambda x: round(x.quantile(0.75) - x.quantile(0.25), 2)),
        condition = pd.NamedAgg(column='condition', aggfunc='first')
    ).reset_index()


def categorize_groups(df, count_column='count'):
    """
    Categorize groups based on their count.

    Parameters:
    df (pandas.DataFrame): The input dataframe containing group statistics.
    count_column (str): The name of the column containing count data. Default is 'count'.

    Returns:
    pandas.DataFrame: The input dataframe with an additional 'data_category' column.
    """
    df['data_category'] = pd.cut(
        df[count_column],
        bins=[0, 10, 20, np.inf], # Use with Inter quartile range based outlier threshold calculation method.
        labels=['insufficient', 'caution', 'confidence']
    )
    return df

# def brand_multipliers_dict():
#     # Define the dictionary for brand-specific upper_IQR_multipliers
#     brand_multipliers = {
#         'Rolex': 10,
#         'Omega': 3.5,
#         'Patek Philippe': 4,
#         'Breitling':4, 
#         'default': 4 # The default will be used if brand is not found in dictionary
#         # Add more brands and their multipliers as needed
#     }
#     return brand_multipliers

def brand_multipliers_dict():
    # Define the dictionary for brand-specific multipliers
    brand_multipliers = {
        'Audemars Piguet': {
            'upper_boundary_multiplier': 10,
            'lower_boundary_multiplier': 4.5
        },
        'Breitling': {
            'upper_boundary_multiplier': 4,
            'lower_boundary_multiplier': 2.5
        },
        'Cartier': {
            'upper_boundary_multiplier': 10,
            'lower_boundary_multiplier': 4
        },
        'Frederique Constant': {
            'upper_boundary_multiplier': 4,
            'lower_boundary_multiplier': 4
        },
        'Hamilton': {
            'upper_boundary_multiplier': 5,
            'lower_boundary_multiplier': 2.5
        },
        'Longines': {
            'upper_boundary_multiplier': 4.5,
            'lower_boundary_multiplier': 2.5
        },         
        'Omega': {
            'upper_boundary_multiplier': 3.5,
            'lower_boundary_multiplier': 2
        },
        'Patek Philippe': {
            'upper_boundary_multiplier': 4,
            'lower_boundary_multiplier': 2.5
        },
        'Panerai': {
            'upper_boundary_multiplier': 4,
            'lower_boundary_multiplier': 3
        },
        'Rolex': {
            'upper_boundary_multiplier': 10,
            'lower_boundary_multiplier': 4
        },
        'Seiko': {
            'upper_boundary_multiplier': 5,
            'lower_boundary_multiplier': 3.5
        }, 
        'Tudor': {
            'upper_boundary_multiplier': 10,
            'lower_boundary_multiplier': 4
        },       
        'default': {
            'upper_boundary_multiplier': 4,
            'lower_boundary_multiplier': 2.5
        }
       
    }
    
    return brand_multipliers

def calculate_outliers_thresholds(row, group_columns):    
    """
    Calculate outlier threshold boundaries based on interquartile range (IQR) and data category
    [insufficient, caution, confidence].

    This function computes the upper and lower outlier thresholds for a given row in the 
    group statistics DataFrame. The thresholds are determined based on the data category 
    and whether 'reference_number' is included in the grouping columns. In other words, if the 
    dataframe we are calculating the boundaries for is being groupby using the referece_number column
    or the specific_model column.

    Parameters:
    row (pandas.Series): A single row from the group statistics DataFrame containing 
                         statistical measures such as Q1, Q2 (median), Q3, and IQR.
    group_columns (list): A list of columns used for grouping the data, which may include 
                          'reference_number' or other identifiers.

    Returns:
    pandas.Series: A series containing 'lower_boundary' and 'upper_boundary', rounded to 
                   two decimal places. If the data category is 'insufficient', both boundaries 
                   will be NaN.
    
    Notes:
    - If the data category is 'insufficient', outlier boundaries are not calculated.
    - For 'caution' and 'confidence' categories, different formulas are applied based on 
      whether 'reference_number' is in `group_columns`.
    - The minimum lower boundary is applied only if 'reference_number' is included in 
      `group_columns`, ensuring flexibility in outlier detection criteria.    
    """
    if row['data_category'] == 'insufficient':
        return pd.Series({'lower_boundary': np.nan, 'upper_boundary': np.nan})   
   
    else:
        # Get the upper_IQR_multiplier from dicitonary based on the brand, use default if brand is not found
        brand_multipliers = brand_multipliers_dict()
        upper_IQR_multiplier = brand_multipliers.get(row['brand'], brand_multipliers['default'])['upper_boundary_multiplier']
        lower_IQR_multiplier = brand_multipliers.get(row['brand'], brand_multipliers['default'])['lower_boundary_multiplier']

        if row['data_category'] == 'caution':
            upper = row['q3'] + upper_IQR_multiplier * row['iqr']
            if 'reference_number' in group_columns:
                lower = row['q1'] - 2 * row['iqr']
            else: # If 'specific-model' is in group_columns
                lower = row['q2'] * 0.15  # 15% of median  
            
        else: # if 'data_category' == confidence'
            upper = row['q3'] + upper_IQR_multiplier * row['iqr']

            if 'reference_number' in group_columns:
                lower = row['q1'] - lower_IQR_multiplier * row['iqr']
            else: # If 'specific-model' is in group_columns
                lower = row['q2'] * 0.15 # 15% of median            

        # Set a minimum lower boundary (e.g., 12.5% of the median) only if 'reference_number' is in group_columns
        if 'reference_number' in group_columns:
            min_lower = row['q2'] * 0.125
            lower = max(lower, min_lower)              

        return pd.Series({'lower_boundary': round(lower, 2), 'upper_boundary': round(upper, 2)})


def detect_outlier_thresholds(df, group_columns, price_column='price'):
    """
    Perform outlier detection on the given dataframe.

    This function groups the data, calculates statistics, categorizes groups based on count,
    and determines outlier boundaries for each group.

    Parameters:
    df (pandas.DataFrame): The input dataframe containing watch data.
    group_columns (list): Columns to group by (e.g., ['brand', 'specific_model']).
    price_column (str): The name of the column containing price data. Default is 'price'.

    Returns:
    pandas.DataFrame: A dataframe with group statistics and outlier boundaries
    """
    stats_df = calculate_groupby_statistics(df, group_columns, price_column)
    stats_df = categorize_groups(stats_df)
    stats_df[['lower_boundary', 'upper_boundary']] = stats_df.apply(
        lambda row: calculate_outliers_thresholds(row, group_columns), 
        axis=1
        )
    # stats_df[['lower_boundary_50%_IQR', 'lower_boundary_15', 'upper_boundary']] = stats_df.apply(calculate_outliers_thresholds, axis=1)
    return stats_df

##### *** Outliers Visualization *** #####

def calculate_custom_stats_4_boxplot(data, selected_brand, model_column):
    q1, q2, q3 = np.percentile(data, [25, 50, 75])
    iqr = q3 - q1
    
    brand_multipliers = brand_multipliers_dict()
    # upper_IQR_multiplier = brand_multipliers.get(selected_brand, brand_multipliers['default'])
    upper_IQR_multiplier = brand_multipliers.get(selected_brand, brand_multipliers['default'])['upper_boundary_multiplier']
    lower_IQR_multiplier = brand_multipliers.get(selected_brand, brand_multipliers['default'])['lower_boundary_multiplier']
  
    
    upper_bound = q3 + upper_IQR_multiplier * iqr
    
    if model_column == 'reference_number':
        pre_lower_bound = q1 - lower_IQR_multiplier * iqr
        # Set a minimum lower boundary (e.g., 12.5% of the median) only if 'reference_number' is in group_columns
        min_lower_bound_value = q2 * 0.125
        lower_bound = max(pre_lower_bound, min_lower_bound_value) 
    else: # 'specific-model'
        lower_bound = q2 * 0.15 # 15% of median        

    # lower_bound = q1 - 2.5 * iqr
    # upper_bound = q3 + 10 * iqr
    
    counts = {
        'q1': sum((data >= lower_bound) & (data < q1)),
        'q2': sum((data >= q1) & (data < q2)),
        'q3': sum((data >= q2) & (data < q3)),
        'Q4': sum((data >= q3) & (data <= upper_bound)),
        'lower_outliers': sum(data < lower_bound),
        'upper_outliers': sum(data > upper_bound),
        'total': len(data)
    }
    
    return counts, q1, q2, q3, lower_bound, upper_bound

def create_box_plot(df, model_column, selected_brand, num_models=10):
    # model_column could be reference_number, specific_model, etc.
    brand_df = df[df['brand'] == selected_brand]
    model_counts = brand_df.groupby(model_column).size().sort_values(ascending=False)
    top_models = model_counts.head(num_models).index
    top_df = brand_df[brand_df[model_column].isin(top_models)]

    for model in top_models:
        model_data = top_df[top_df[model_column] == model]['price']
        
        counts, q1, q2, q3, lower_bound, upper_bound = calculate_custom_stats_4_boxplot(model_data, selected_brand, model_column)
        
        fig = go.Figure()
        
        # Add box plot
        fig.add_trace(go.Box(
            y=model_data,
            name=model,
            boxpoints='outliers',  # Only show outlier points
            jitter=0.3,
            whiskerwidth=0.2,
            marker_color='lightskyblue',
            line_color='darkblue',
            marker=dict(
                size=5,
                color='darkblue',
                outliercolor='rgba(219, 64, 82, 0.6)',
                line=dict(
                    outliercolor='rgba(219, 64, 82, 1.0)',
                    outlierwidth=2
                )
            ),
            quartilemethod="linear"
        ))
        
        # Add custom threshold lines
        fig.add_shape(
            type="line", 
            x0=-0.5, x1=0.5, 
            y0=lower_bound, y1=lower_bound,
            line=dict(color="magenta", width=2, dash="dash"),
            label=dict(
                text=f"Lower Bound: {lower_bound:.2f}",            
                textposition="start",
                font=dict(size=10, color="magenta"),
                yanchor="bottom"
            ),
        )
        
        fig.add_shape(
            type="line", 
            x0=-0.5, x1=0.5, 
            y0=upper_bound, y1=upper_bound,
            line=dict(color="magenta", width=2, dash="dash"),
            label=dict(
                text=f"Upper Bound: {upper_bound:.2f}",
                textposition="start",
                font=dict(size=10, color="magenta"),
                yanchor="top"
            )
        )
        
        # Calculate annotation positions
        middle_y = (upper_bound + lower_bound) / 2
        available_height = upper_bound - lower_bound
        num_annotations = 5
        spacing = available_height / (num_annotations + 1)

        # Add annotations
        annotations = [       
            dict(x=1.05, y=middle_y + 2*spacing, text=f"Q4: {counts['Q4']} Data Points", showarrow=False, xref="paper", yref="y"),
            dict(x=1.05, y=middle_y + spacing, text=f"Q3: {counts['q3']} Data Points", showarrow=False, xref="paper", yref="y"),
            dict(x=1.05, y=middle_y, text=f"Q2: {counts['q2']} Data Points", showarrow=False, xref="paper", yref="y"),
            dict(x=1.05, y=middle_y - spacing, text=f"Q1: {counts['q1']} Data Points", showarrow=False, xref="paper", yref="y"),
            dict(x=1.05, y=middle_y - 2*spacing, text=f"Total: {counts['total']}", showarrow=False, xref="paper", yref="y", font=dict(size=12)),
            dict(x=1, y=upper_bound, yshift=+10, text=f"Upper Outliers: {counts['upper_outliers']}", showarrow=False, xref="paper", yref="y", font=dict(size=10, color="Magenta")),
            dict(x=1, y=lower_bound, yshift=-10, text=f"Lower Outliers: {counts['lower_outliers']}", showarrow=False, xref="paper", yref="y", font=dict(size=10, color="Magenta"))      
        ]
        fig.update_layout(annotations=annotations)

        # Update layout
        fig.update_layout(
            title=f'<b>Price Distribution for {selected_brand} Model {model}</b>',
            height=850, # Increase this value to make the plot taller,
            width=900, # Decrease this value to make the plot narrower,
            yaxis_title='<b>Price (USD)</b>', # Add bold HTML tags  
            yaxis=dict(
                tickformat=',.0f', # Format y-axis labels as integers with commas
                tickmode='linear',
                tick0=0, # Start ticks at 0
                dtick=5000,  # Major tick every 5000 units
                range=[max(0, model_data.min() * 0.9), model_data.max() * 1.1],
                scaleanchor="x",
                scaleratio=1,
                constrain="domain",
                rangemode="tozero", 
            ),
            showlegend=False,
            dragmode="pan",
            xaxis_title=f"<b>Reference Number: {model}</b>",
            xaxis_title_font=dict(size=14, family='Arial', color='black'),
        )
        
        # Add buttons for zoom and pan
        fig.update_layout(
            updatemenus=[
                dict(
                    type="buttons",
                    direction="left",
                    buttons=[
                        dict(args=[{"yaxis.autorange": True}], label="Reset Y", method="relayout"),
                        dict(args=[{"dragmode": "zoom"}], label="Zoom", method="relayout"),
                        dict(args=[{"dragmode": "pan"}], label="Pan", method="relayout"),
                    ],
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    x=0.11,
                    xanchor="left",
                    y=1.07,
                    yanchor="top"
                ),
            ]
        )

        fig.show()
