# modules/config.py

def brand_to_library_dict():
    # Mapping brands' library tables with their corresponding brand name
    brand_to_library = {
        "A. Lange & Söhne": "library_a_lange_sohne",
        "Audemars Piguet": "library_audemars_piguet",
        "Ball": "library_ball",
        "Blancpain": "library_blancpain",
        "Breitling": "library_breitling",
        "Bvlgari": "library_bulgari",
        "Bulova": "library_bulova",
        "Cartier": "library_cartier",
        "Chanel": "library_chanel",
        "Eterna": "library_eterna",
        "Frederique Constant": "library_frederique_constant",
        "Girard-Perregaux": "library_girard_perregaux",
        "Grand Seiko": "library_grand_seiko",
        "Hamilton": "library_hamilton",
        "Hublot": "library_hublot",
        "HYT": "library_hyt",
        "IWC": "library_iwc",
        "Jaeger-LeCoultre": "library_jaeger_lecoultre",
        "Longines": "library_longines",
        "Maurice Lacroix": "library_maurice_lacroix",
        "Michele": "library_michele",
        "Mido": "library_mido",
        "Montblanc": "library_montblanc",
        "Omega": "library_omega",
        "Oris": "library_oris",
        "Panerai": "library_panerai",
        "Patek Philippe": "library_patek_philippe",
        "Piaget": "library_piaget",
        "Rec": "library_rec",
        "Rolex": "library_rolex",
        "Seiko": "library_seiko",
        "TAG Heuer": "library_tag_heuer",
        "Tissot": "library_tissot",
        "Tudor": "library_tudor",
        "Ulysse Nardin": "library_ulysse_nardin",
        "Vacheron Constantin": "library_vacheron_constantin",
        "Zenith": "library_zenith"
    }
    return brand_to_library


def datatype_dictionary():
    dtype_dict = {
                "date": "datetime64[ns]",
                "brand": "string",
                "reference_number": "string",
                "identifier":"string",
                "group_reference": "string",
                "parent_model": "string",
                "specific_model": "string",
                "currency": "string",
                "price": "float",
                "mean_price": "float",
                "high": "float",
                "low": "float",
                "pct_change_1w": "float",
                "dol_change_1w": "float",
                "dol_change_1w": "float",
                "pct_change_1m": "float",
                "dol_change_1m": "float",
                "pct_change_3m": "float",
                "dol_change_3m": "float",
                "pct_change_6m": "float",
                "dol_change_6m": "float",
                "pct_change_1y": "float",
                "dol_change_1y": "float",
                "condition": "string",
                "status": "string",
                "parent_model": "string",
                "specific_model": "string",
                # "year_introduced": "int32",
                # "year_of_production": "int32",
                "year_introduced": "object",
                "year_of_production": "object",
                "count": "object",
                "source": "string",    
                "listing_title": "string",
                "watch_url": "string",
                "auction_end_time": "string",
                "authenticity_guarantee": "string",
                "between_lugs": "string",
                "bezel_color": "string",
                "bezel_material": "string",
                "bracelet_color": "string",
                "bracelet_material": "string",
                "brand_slug": "string",
                "caliber": "string",
                "case_finish": "string",
                "case_material": "string",
                "case_shape": "string",
                "case_thickness": "string",
                "caseback": "string",
                "clasp_type": "string",
                "country": "string",
                "crystal": "string",
                "dial_color": "string",
                "diameter": "string",
                "features": "string",
                "frequency": "string",
                "jewels": "string",
                "listing_type": "string",
                "lug_to_lug": "string",
                "made_in": "string",
                "movement": "string",
                "numerals": "string",
                "power_reserve": "string",
                "seller_type": "string",
                "serial_number": "string",
                "specific_location": "string",
                "style": "string",
                "type": "string",
                "water_resistance": "string",
                "weight": "string",
                "time_updated": "string"
    }
    return dtype_dict

def brands_ready_for_metrics():
    """
    These brands have library tables and have enough reference_number rules in place
    for proper metrics calculations. 
    """
    brands_list = [
            'Audemars Piguet',
            'A. Lange & Söhne',
            'Breitling',
            'Bulova',
            'Cartier',
            'Frederique Constant',
            'Hamilton',                
            'IWC',
            'Jaeger-LeCoultre',
            'Longines',
            'Maurice Lacroix',               
            'Montblanc', 
            'Omega',
            'Oris',
            'Panerai',
            'Patek Philippe',
            'Rolex',
            'Seiko',
            'TAG Heuer',
            'Tissot',
            'Tudor',               
    ]
    return brands_list


def metrics_configurations():
    """
    This dictionary contains the configurations for the metrics calculations.
    """
    CONFIGURATIONS = {
        'ref_num_metrics_config_key': {
            'groupby_2_columns': ['brand', 'reference_number'],
            'groupby_3_columns': ['brand', 'reference_number', 'date'],
            'ffill_columns': [            
                'group_reference', 
                'currency',  
                'median_price', 
                'mean_price', 
                'high', 
                'low', 
                'parent_model', 
                'specific_model', 
                'year_introduced',
                'identifier',
                'brand_slug'
            ],
            'database': {
                'production': {
                    'schema': 'metrics',
                    'price_hist_and_stats_tbl':{
                        'condition': {
                            'mixed': 'wcc_wkl_prices_and_stats_mixed',
                            'new': 'wcc_wkl_prices_and_stats_new',
                            'pre_owned': 'wcc_wkl_prices_and_stats_preowned'
                        },
                        'tbl_description': {
                            'mixed': 'Weekly Price and Stats Mixed Condition Data',
                            'new' : 'Weekly Price and Stats New Condition Data',
                            'preowned' : 'Weekly Price and Stats Preowned Condition Data',
                        }
                    },
                    'volatility_calc_tbl': {
                        'condition': {
                            'mixed': 'volatility_of_ref_num_prices_mixed'                
                        },
                        'tbl_description': {
                            'mixed': 'Volatility of Reference Number Prices Mixed Condition Data'
                        }
                    },
                    'liquidity_calc_tbl': {
                        'condition': {
                            'mixed': 'liquidity'                
                        },
                        'tbl_description': {
                            'mixed': 'Liquidity Data - duration in weeks of listings on the market.'
                        }
                    }
                },
                'data_source': {
                    'schema': 'public',
                    'enriched_tbl': 'metrics_wcc_data_enriched',
                    'no_outliers_tbl': 'metrics_wcc_data_enriched_rm_outl'
                }
            },
            'sql_query': {
                'price_hist_last_1_dot_5_yrs_ago': 'ref_numb_get_price_hist_for_the_last_1_dot_5_yrs.sql',
                'latest_week_listings': 'ref_numb_get_latest_week_listings.sql',
                'volatility_required_price_hist': 'ref_numb_get_volatility_required_price_hist.sql',
                'weeks_on_market_required_historic_listings': 'ref_num_listings_weeks_on_market_get_required_hist_listings.sql'
            }         
        },
        'specific_mdl_metrics_config_key': {
            'groupby_2_columns': ['brand', 'specific_model'],
            'groupby_3_columns': ['brand', 'specific_model', 'date'],
            'ffill_columns': ['currency',                              
                            'median_price', 
                            'mean_price', 
                            'high', 
                            'low', 
                            'parent_model',                 
                            'brand_slug'
                        ],
            'database': {
                'production': {
                    'schema': 'metrics',
                    'price_hist_and_stats_tbl':{
                        'condition': {
                            'mixed': 'wcc_specific_mdl_wkl_price_and_stats_mixed',
                            'new': 'wcc_specific_mdl_wkl_price_and_stats_new',
                            'pre_owned': 'wcc_specific_mdl_wkl_price_and_stats_preowned'
                        },
                        'tbl_description': {
                            'mixed': 'Specific Model Weekly Price and Stats Mixed Condition Data',
                            'new' : 'Specific Model Weekly Price and Stats New Condition Data',
                            'preowned' : 'Specific Model Weekly Price and Stats Preowned Condition Data',
                        }
                    }                
                },
                'data_source': {
                    'schema': 'public',
                    'enriched_tbl': 'metrics_wcc_data_enriched',
                    'no_outliers_tbl': 'metrics_wcc_data_enriched_rm_outl'
                }
            },
            'sql_query': {
                'price_hist_last_1_dot_5_yrs_ago': 'specific_mdl_get_price_hist_for_the_last_1_dot_5_yrs.sql',
                'latest_week_listings': 'specific_mdl_get_latest_week_listings.sql'
            }                   
        },
        'parent_mdl_metrics_config_key': {
            'groupby_2_columns': ['brand', 'parent_model'],
            'groupby_3_columns': ['brand', 'parent_model', 'date'],
            'ffill_columns': ['currency', 
                              'median_price', 
                              'mean_price', 
                              'high', 'low', 
                              'condition', 
                              'brand_slug'
            ],
            'database': {
                'production': {
                    'schema': 'metrics',
                    'price_hist_and_stats_tbl':{                    
                        'condition': {
                            'mixed': 'wcc_parent_mdl_wkl_price_and_stats_mixed',
                            'new': 'wcc_parent_mdl_wkl_price_and_stats_new',
                            'pre_owned': 'wcc_parent_mdl_wkl_price_and_stats_preowned'
                        },
                        'tbl_description': {
                            'mixed': 'Parent Model Weekly Price and Stats Mixed Condition Data',
                            'new' : 'Parent Model Weekly Price and Stats New Condition Data',
                            'preowned' : 'Parent Model Weekly Price and Stats Preowned Condition Data',
                        }
                    }                                  
                },
                'data_source': {
                    'schema': 'public',
                    'enriched_tbl': 'metrics_wcc_data_enriched',
                    'no_outliers_tbl': 'metrics_wcc_data_enriched_rm_outl'
                }
            },
            'sql_query': {
                'price_hist_last_1_dot_5_yrs_ago': 'parent_mdl_get_price_hist_for_the_last_1_dot_5_yrs.sql',
                'latest_week_listings': 'parent_mdl_get_latest_week_listings.sql'
            }   
        },
        'brand_metrics_config_key': {
            'groupby_1_columns': ['brand'],
            'groupby_2_columns': ['brand', 'date'],
            'ffill_columns': ['currency', 
                              'median_price', 
                              'mean_price', 
                              'high', 
                              'low', 
                              'brand_slug'
            ], 
            'database': {
                'production': {
                    'schema': 'metrics',
                    'price_hist_and_stats_tbl':{
                        'condition': {
                            'mixed': 'wcc_brand_wkl_price_and_stats_mixed',
                            'new': 'wcc_brand_wkl_price_and_stats_new',
                            'pre_owned': 'wcc_brand_wkl_price_and_stats_preowned'
                        },
                        'tbl_description': {
                            'mixed': 'Brand Weekly Price and Stats Mixed Condition Data',
                            'new' : 'Brand Weekly Price and Stats New Condition Data',
                            'preowned' : 'Brand Weekly Price and Stats Preowned Condition Data',
                        }               
                    }
                },
                'data_source': {
                    'schema': 'public',
                    'enriched_tbl': 'metrics_wcc_data_enriched',
                    'no_outliers_tbl': 'metrics_wcc_data_enriched_rm_outl'
                }
            },
            'sql_query': {
                'price_hist_last_1_dot_5_yrs_ago': 'brand_get_price_hist_for_the_last_1_dot_5_yrs.sql',
                'latest_week_listings': 'brand_get_latest_week_listings.sql'
            }         
        }
    }
    return CONFIGURATIONS
