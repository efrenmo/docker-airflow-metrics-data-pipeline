import re
from forex_python.converter import CurrencyCodes # pip install forex-python==1.8
from currencies import Currency # pip install currencies==2020.12.12
from unidecode import unidecode # pip install unidecode==1.3.8
import pycountry # pip install pycountry==23.12.11
# from slugify import slugify # pip install python-slugify==8.0.4
from datetime import datetime
import numpy as np # pip install numpy==1.21.5
import pandas as pd # pip install pandas==2.0.2
import time
from time import sleep
import os
import argparse
from datetime import datetime, timedelta

##################################
###### script define section######  
##################################

# Only returns if the entire word is in the search text eg. "eel" in "steel" will return false while "steel" in "steel watch" will return true 
def word_in_text(word, text):
    pattern = r'\b' + re.escape(word) + r'\b'
    if re.search(pattern, text):
        return True
    return False
    # Pre-compile the pattern for splitting the text into words
    # pattern = re.compile(r'\W+')
    # words = pattern.split(text)
    # return word in words

### parent_model_and_specific_model ###
###
# df here for rules made on Google Sheet
# col 0 is brand
# col 1 is specific_model name
# col 2 is parent_model name
# col 3 is alternate values to capture for specific_model
def get_match_dict(df):
    # Initialize an empty dictionary
    match_dict = {}

    # Iterate over the rows of the DataFrame
    for index, row in df.iterrows():
        key = row[0] # brand
        sub_key = row[2] # parent model
        value = row[1] # specific model
        alters = row.iloc[3:].tolist() # alternate values

        # Initialize a dictionary for the key if it doesn't exist
        if key not in match_dict:
            match_dict[key] = {}

        # Update the dictionary with the value
        value_list = match_dict[key].get(sub_key, [])
        if value not in value_list and value != 'nan' and value is not None:
            value_list.append(value) 
        for alter in alters:
            if str(alter) != 'nan' and alter is not None:
                value_list.append(str(alter))
        match_dict[key][sub_key] = value_list
    return match_dict


def get_alter_dict(df):
    # Initialize an empty dictionary
    match_dict = {}
    df_alter = df.dropna(subset=df.columns[3])
    # Iterate over the rows of the DataFrame
    for index, row in df_alter.iterrows():
        keys = row.iloc[3:].tolist() # alternate values
#         key = str(row[3]) # alternate values
        value = row[1] # specific model
        for key in keys:
            # Initialize a dictionary for the key if it doesn't exist
            if key not in match_dict and key != 'nan' and key is not None:
                match_dict[key] = value


    return match_dict
###

###
# All changes will take place in the original df columns
def lower_clean_text(df,column_name,target_column=None):
    if target_column is None:
        target_column = column_name    
# Chain .str.replace() and .str.lower() methods
    df[target_column] = df[column_name].str.lower() \
                                       .str.replace(r'[^a-z0-9.+/&éèêàâîôöûçëï \'"-]', '', regex=True)
def check_parent(x, match_dict):
    for key, value in match_dict.items():
        for v in value:
            if x == None:
                return None
            if v.lower() in x.lower():
                return key
    return None

def check_specific(x, match_dict,alter_dict):
    for key, value in match_dict.items():
        for v in value:
            if x == None:
                return None
            if v.lower() in str(x).lower():
                if v in alter_dict:
                    return alter_dict[v]
                else:
                    return v
    return None


def parent_model_and_specific_model(df):
    # load csv file and reverse it, to prioritize the longer string
    df_config = pd.read_csv('parent_model_and_specific_model.csv',encoding='latin1',header=None)
    df_config.dropna(how='all', inplace=True)
    df_config.dropna(how='all',subset=[2],inplace=True)
    df_config = df_config.iloc[::-1]
    match_dict = get_match_dict(df_config)
    alter_dict = get_alter_dict(df_config)
    # Lower and Clean special character
    lower_clean_text(df,'listing_title','cleaned_listing_title')
    brand_list = list(match_dict.keys())
    df['specific_model'] = df['specific_model'].astype(object)
    df['parent_model'] = df['parent_model'].astype(object)
    # Change specific_model and parent_model according to the rule
    for brand in match_dict:
        df.loc[df['brand'] == brand, 'specific_model'] = df.loc[df['brand'] == brand, 'specific_model'].apply(lambda x: check_specific(x, match_dict[brand],alter_dict))
    df['specific_model_temp'] = df['specific_model']
    for brand in match_dict:    
        condition = (df['brand'] == brand) & ((df['specific_model'].isna()) | (df['specific_model'].isin(match_dict[brand])))
        df.loc[condition, 'specific_model'] = df.loc[condition, 'cleaned_listing_title'].apply(lambda x: check_specific(x, match_dict[brand], alter_dict))
    condition = (df['specific_model'].isna()) & (df['specific_model_temp'].notnull())
    df.loc[condition, 'specific_model'] = df['specific_model_temp']
    for brand in match_dict:    
        df.loc[df['brand'] == brand, 'parent_model'] = df.loc[df['brand'] == brand, 'specific_model'].apply(lambda x: check_parent(x, match_dict[brand]))
    
    # Delete specific_model and parent_model which is not in the list
    df.loc[~df['brand'].isin(brand_list), ['specific_model','parent_model']] = None
    
    df.drop(columns=['cleaned_listing_title','specific_model_temp'], inplace=True)
###
    
### case_material ###
case_material_dict = {
    'Ceramic, Metal':['Ceramic, Metal','Ceramos'],
    'Diamond, Gemstones':['Diamond, Gemstones','diamond','diamonds','diamond-set'],
    'Gemstones':['Gemstones','emerald','topaz','sapphires','amethyst','rubies','ruby','gemstones','gem-set'],
    'ADLC':['ADLC'],
    'Aluminum':['Aluminum','aluminium'],
    'Bronze':['Bronze'],
    'Ceramic':['Ceramic'],
    'Cermet':['Cermet'],
    'Composite':['Composite'],
    'DLC':['DLC'],
    'Magnesium':['Magnesium'],
    'Palladium':['Palladium'],
    'Polymer':['Polymer','Breitlight','Titaplast'],
    'PVD':['PVD'],
    'Rose Gold':['Rose Gold','pink gold','red gold','orange gold','5N','6N','sedna','everose','everrose','king gold'],
    'Rubber':['Rubber'],
    'Sapphire':['Sapphire'],
    'Silver':['Silver'],
    'Steel':['Steel','omegasteel'],
    'Tantalum':['Tantalum'],
    'Titanium':['Titanium','titalyt','Ceratanium'],
    'Tungsten':['Tungsten'],
    'Two-Tone':['Two-Tone','two tone','gold and stainless','gold and steel','gold & steel'],
    'White Gold':['White Gold','canopus gold'],
    'Zalium':['Zalium'],
    'Zirconium':['Zirconium'],
    # Requirements to include "Gold": either it must contain one of the following alternates OR it can include just "gold" but does NOT match any of the values or alternate values for Rose Gold or White Gold 
    'Gold':['Gold','yellow gold','moonshine gold','beige gold','honeygold','honey gold']
}
def check_case_material(x, match_dict):
    material_list = []
    for key, value in match_dict.items():
        if key == 'Gold':
            if 'Rose Gold' in material_list or 'White Gold' in material_list:
                continue
        if key == 'DLC' and 'ADLC' in material_list:
            continue
        for v in value:
            if x == None:
                return None
            if word_in_text(v.lower(), str(x).lower()) and key not in material_list:
                material_list.append(key)
    if material_list == []:
        return None
    return ', '.join(material_list)

### dial_color ###
dial_color_dict = {
    'Diamond, Gemstones':['Diamond, Gemstones','diamond','diamonds','diamond-set'],
    'Gemstones':['Gemstones','emerald','topaz','sapphire','amethyst','rubies','ruby','gemstones','gem-set'],
    'Silver':['Silver','Silvered','argente','platinum'],
    'Rose Gold':['Rose Gold','rose'],
    'Steel':['Steel'],
    'Bronze':['Bronze'],
    'Red':['Red','burgundy'],
    'Orange':['Orange'],
    'Yellow':['Yellow','White Grape'],    
    'Green':['Green','olive','mint'],
    'Blue':['Blue','turquoise','Ice Blue','aqua','navy','midnight'],
    'Purple':['Purple','aubergine','violet','lavender','Red Grape'],    
    'Black':['Black','onyx'],    
    'Gray':['Gray','slate','charcoal','grey','Anthracite','Ruthenium'],
    'White':['White','ivory','cream'],    
    'Salmon':['Salmon'],
    'Brown':['Brown','chocolate','beige','tan','khaki','walnut','Bwoodrown','Havana','Taupe'],
    'Pink':['Pink','ruby'],    
    'Multi':['Multi','multiple','Multi-Color','Cloisonn','motif','Multi-Coloured','Camouflage','floral','pattern'],   
    'Transparent':['Transparent','skeleton','openwork','skeletonized'], 
    'Mother of Pearl':['Mother of Pearl','MOP','Mother-of-pearl'],    
    'Pearl':['Pearl'], # must contain Pearl but NOT Mother of Pearl or MOP  
    'Champagne':['Champagne'], 
    'Meteorite':['Meteorite'],    
    'Rhodium':['Rhodium'], 
    'Sundust, Rose Gold':['Sundust, Rose Gold','sundust'],
    'Copper':['Copper'],
    # Requirements to include "Gold": either it must contain one of the following alternates OR it can include just "gold" but does NOT match any of the values or alternate values for Rose Gold or White Gold 
    'Gold':['Gold','yellow gold','moonshine gold','beige gold','honeygold','honey gold','golden']
}

def check_dial_color(x, match_dict):
    color_list = []
    for key, value in match_dict.items():
        if key == 'Gold':
            if 'Rose Gold' in color_list or 'White' in color_list:
                continue
        if key == 'Pearl' and 'Mother of Pearl' in color_list:
            continue
        for v in value:
            if x == None:
                return None
            if v.lower() in str(x).lower() and key not in color_list:
                color_list.append(key)
    if color_list == []:
        return None
    return ', '.join(color_list)

### condition ###
condition_dict={
    'Pre-Owned, Manufacturer Refurbished': ['Pre-Owned, Manufacturer Refurbished', 'Manufacturer Refurbished'],
    'Pre-Owned, Seller Refurbished': ['Pre-Owned, Seller Refurbished', 'Seller Refurbished'],
    'Pre-Owned, Vintage': ['Pre-Owned, Vintage', 'Vintage', 'Circa 1'],
    'Pre-Owned':['Pre-Owned', 'very good', 'good', 'fair', 'pre-owned','preowned','used','barely worn','worn once','worn twice','worn three','only worn','almost new','like new','near new','excellent condition','excellent overall condition','good condition','great condition','amazing condition', 
                 'Fantastic condition', 'Near-mint condition', 'Near mint condition', 'Outstanding', 'good overall condition', 'great overall condition', 'superb', 'strong condition', 'light signs of wear', 'poor', 'previously worn', 'as new'],
    'Modified':['Modified','modified','aftermarket','custom'],
    'New':['New','unworn','brand new','never worn','new-in-box', 'Pre-Owned/Unworn', 'mint condition'],
    'Incomplete':['Incomplete','parts'],
    'Unspecified':['Unspecified'],
}
def check_condition(x, match_dict):
    # Only one value allowed. Apply these rules in order of the listed allowed values. 
    for key, value in match_dict.items():
        for v in value:
            if x == None:
                return None
            if v.lower() in str(x).lower():
                return key
    return None

### movement ###
movement_dict={
    'Automatic':['Automatic','self-winding','perpetual','self winding'],
    'Manual winding':['Manual winding','manual','hand winding','hand-wound'],
    'Kinetic':['Kinetic'],
    'Quartz':['Quartz','battery'],
    'Solar':['Solar'],
    'Spring Drive':['Spring Drive']
}
def check_movement(x, match_dict):
    # Only one value allowed. Apply these rules in order of the listed allowed values. 
    for key, value in match_dict.items():
        for v in value:
            if x == None:
                return None
            if v.lower() in str(x).lower():
                return key
    return None

### contents ###
# contents_dict={
#     'Box, Papers':['Box, Papers','Box: Yes Papers: Yes','box and papers','boxes & papers','box papers','boxes papers','box, papers','Box and Service Paper','Boxes and Paperwork','box with tags and papers','Boxes, Booklets and Card','full set','full kit'],
#     'Box':['Box: Yes Papers: No','box included','New in the Box','new in box','with box','New-in-Box','w/ box','box set'],
#     'Papers':['Box: No Papers: Yes','with papers'],
# }

# contents_recheck_dict={
#     'Box, Papers':['Box, Papers'],
#     'Box':['Box'],
#     'Papers':['Papers'],
# }

def check_contents(x, match_dict):
    # Only one value allowed. Apply these rules in order of the listed allowed values. 
    for key, value in match_dict.items():
        for v in value:
            if x == None:
                return None
            if v.lower() in str(x).lower():
                return key
    return x

def recheck_contents(x, match_dict):
    # Only one value allowed. Apply these rules in order of the listed allowed values. 
    for key, value in match_dict.items():
        for v in value:
            if x == None:
                return None
            if v.lower() == str(x).lower():
                return key
    return None

def check_contents_title(x, match_dict):
    # Only one value allowed. Apply these rules in order of the listed allowed values. 
    for key, value in match_dict.items():
        for v in value:
            if x == None:
                return None
            if v.lower() in str(x).lower():
                return key
    return None

### style ###
style_dict = {
    'Sport':['Sport','sports'],
    'Dive':['Dive','Diver','Diving','Dive Watch','Diving Watch','Diver Watch'],
    'Chronograph':['Chronograph','stopwatch'],
    'Military / Field':['Military / Field','Military','Field','Military Watch','Field Watch','Field / Military','field/military watches'],
    'GMT':['GMT','GMT Watch','Greenwich Mean Time Watch','GMT watches','Greenwich Mean Time'],
    'Dress':['Dress','Dress Watch','dress watches'],
    'Aviator':['Aviator','Aviator Watch','aviator watches','flieger watches'],
    'Atomic Radio Controlled':['Atomic Radio Controlled','Atomic Radio Controlled Watch','Atomic watch','radio controlled watch','radio watch','atomic radio'],
    'GPS':['GPS Watch Watch'],
    'Smartwatch':['Smartwatch','Smart Watch','Apple Watch','mobile watch','wearable tech'],
    'Pilot':['Pilot','Pilot Watch'],
    'Skeleton':['Skeleton','Skeleton Watch','openwork','skeleton dial','skeletonized watch','transparent','open work'],
    'Racing / Driving':['Racing / Driving','Racing','Driving','Racing Watch','Driving Watch','motorsport watch','car watch']
}

def check_style(x, match_dict):
    style_list = []
    for key, value in match_dict.items():
        for v in value:
            if x == None:
                return None
            if v.lower() in str(x).lower() and key not in style_list:
                style_list.append(key)
    # if style is dive, pilot, or racing / driving, also include Sport
    sport_list = ['Dive','Pilot','Racing / Driving']
    if any(string in sport_list for string in style_list) and 'Sport' not in style_list:
        style_list.append('Sport')
    if style_list == []:
        return None
    return ', '.join(style_list)

### numerals ###
numerals_dict = {
    'Arabic numerals':['Arabic numerals','arabic dial watch','arabic','arabic numerals','Breguet'],
    'Roman numerals':['Roman numerals','roman'],
    'Chinese numerals':['Chinese numerals','chinese'],
    'Hebrew numerals':['Hebrew numerals','hebrew'],
    'No numerals':['bars','stick','dot','baton','none','diamond','gem-set'],
    'No markers':['No markers']
}

def check_numerals(x, match_dict):
    numerals_list = []
    for key, value in match_dict.items():
        for v in value:
            if x == None:
                return None
            if v.lower() in str(x).lower() and key not in numerals_list:
                numerals_list.append(key)
    if numerals_list == []:
        return None
    return ', '.join(numerals_list)

### bracelet_material ###
bracelet_material_dict = {
    'Acrylic':['Acrylic'],
    'Aluminum':['Aluminum','aluminum','aluminium'],
    'Alligator, Leather':['Alligator, Leather','alligator','alligator leather','aligator','gator'],
    'Anthracite':['Anthracite'],
    'Bronze':['Bronze'],
    'Buffalo, Leather':['Buffalo, Leather','buffalo'],
    'Calfskin, Leather':['Calfskin, Leather','calf','calfskin'],
    'Canvas, Fabric':['Canvas, Fabric','canvas'],
    'Carbon':['Carbon'],
    'Ceramic':['Ceramic'],
    'Crocodile, Leather':['Crocodile, Leather','crocodile','croc leather','croc'],
    'Crystal':['Crystal'],
    'Denim, Fabric':['Denim, Fabric','denim','jean'],
    'Diamond, Gemstones':['Diamond, Gemstones','diamond','diamonds'],
    'DLC':['DLC'],
    'Eel, Leather':['Eel, Leather','eel'],
    'Fabric':['Fabric','textile','cloth'],
    'Gemstones':['Gemstones','gem-set','rubies','emerald','topaz','ruby','amethyst'],
    'Kevlar':['Kevlar'],
    'Kudu, Leather':['Kudu, Leather','kudu'],
    'Leather':['Leather','leather'],
    'Lizard, Leather':['Lizard, Leather','lizard'],
    'Nylon, Fabric':['Nylon, Fabric','nylon'],
    'Platinum':['Platinum'],
    'Polymer':['Polymer','Breitlight','Titaplast','elastomer'],
    'PVD':['PVD'],
    'Rose Gold':['Rose Gold','pink gold','red gold','orange gold','5N','6N','sedna','everose','everrose','king gold'],
    'Rubber':['Rubber'],
    'Sapphire':['Sapphire'],
    'Satin, Fabric':['Satin, Fabric','satin'],
    'Snakeskin, Leather':['Snakeskin, Leather','snake','snakeskin'],
    'Steel':['Steel','stainless','oystersteel'],
    'Stringray, Leather':['Stringray, Leather','stingray','Galuchat'],
    'Tantalum':['Tantalum'],
    'Titanium':['Titanium','titalyt','Ceratanium'],
    'Two-Tone':['Two-Tone','two tone','gold and stainless','gold and steel','gold & steel'],
    'White Gold':['White Gold','canopus'],
    'Wood':['Wood','bark'],
    'Gold, Steel':['Gold, Steel','Rolesor'],
    # Requirements to include "Gold": either it must contain one of the following alternates OR it can include just "gold" but does NOT match any of the values or alternate values for Rose Gold or White Gold 
    'Gold':['Gold','yellow gold','moonshine gold','beige gold','honeygold','honey gold']
}

def check_bracelet_material(x, match_dict):
    material_list = []
    for key, value in match_dict.items():
        if key == 'Gold':
            if 'Rose Gold' in material_list or 'White Gold' in material_list:
                continue
        for v in value:
            if x == None:
                return None
            if word_in_text(v.lower(), str(x).lower()) and key not in material_list:
                material_list.append(key)
    if material_list == []:
        return None
    return ', '.join(material_list)

### bracelet_color ###
bracelet_color_dict = {
    'Black':['Black','onyx'],  
    'Blue':['Blue','turquoise','Ice Blue','aqua','navy','midnight'],
    'Bronze':['Bronze'],
    'Brown':['Brown','chocolate','beige','tan','wood','taupe'],    
    'Diamond':['Diamond','diamond','diamonds','diamond-set'],
    'Gray':['Gray','grey','slate','charcoal'],
    'Green':['Green','emerald'],
    'Multi':['Multi','multicolor','artistic','pattern','camo','multi'],
    'Orange':['Orange'],
    'Pearl':['Pearl'],
    'Pink':['Pink'],
    'Purple':['Purple','aubergine','violet','lavender'],
    'Red':['Red','burgundy'],
    'Rose Gold':['Rose Gold','pink gold','red gold','orange gold','5N','6N','sedna','everose','everrose','king gold','rose'],
    'Silver':['Silver','Silvered','argente'],
    'Steel':['Steel'],
    'Two-Tone':['Two-Tone'],
    'White Gold':['White Gold','canopus'], 
    'White':['White','ivory','cream'],
    # Requirements to include "Gold": either it must contain one of the following alternates OR it can include just "gold" but does NOT match any of the values or alternate values for Rose Gold or White Gold 
    'Gold':['Gold','yellow gold','moonshine gold','beige gold','honeygold','honey gold'],
    'Yellow':['Yellow'], # but NOT yellow gold
    'Interchangable':['Interchangable']
}

def check_bracelet_color(x, match_dict):
    color_list = []
    for key, value in match_dict.items():
        if key == 'Gold':
            if 'Rose Gold' in color_list or 'White Gold' in color_list:
                continue           
        for v in value:
            if x == None:
                return None
            if v == 'White' and 'White Gold' in color_list:
                continue
            if v == 'Yellow' and 'Gold' in color_list:
                continue
            if v.lower() in str(x).lower() and key not in color_list:
                color_list.append(key)
    if color_list == []:
        return None
    return ', '.join(color_list)

### clasp_type ###
clasp_type_dict = {
    'Pin Buckle, Buckle':['Pin Buckle, Buckle','pin','tang','ardiillon','tang-type','prong buckle'],
    'Sliding Buckle, Buckle':['Sliding Buckle, Buckle','sliding'],
    'Hidden Clasp, Folding Clasp':['Hidden Clasp, Folding Clasp','hidden'],
    'Butterfly Clasp, Folding Clasp':['Butterfly Clasp, Folding Clasp','double fold','double folding','butterfly'],
    'Triple Folding Clasp, Folding Clasp':['Triple Folding Clasp, Folding Clasp','triple'],
    'Velcro':['Velcro'],
    'Non-closure':['Non-closure','nonclosure'],
    'NATO':['NATO'],
    'Buckle':['Buckle'],
    'Folding Clasp':['Folding Clasp','fold','folding','fold-over','t-fit','Oysterlock']  
}

def check_clasp_type(x, match_dict):
    material_list = []
    for key, value in match_dict.items():
        if key == 'Buckle':
            if 'Pin Buckle, Buckle' in material_list or 'Sliding Buckle, Buckle' in material_list:
                continue
        if key == 'Folding Clasp':
            if 'Hidden Clasp, Folding Clasp' in material_list or 'Butterfly Clasp, Folding Clasp' in material_list or 'Triple Folding Clasp, Folding Clasp' in material_list:
                continue
        for v in value:
            if x == None:
                return None
            if v.lower() in str(x).lower() and key not in material_list:
                material_list.append(key)
    if material_list == []:
        return None
    return ', '.join(material_list)

### features ###
features_dict = {
    '24-Hour Indication':['24-Hour Indication','24 hour'],
    '3D Moon Phases':['3D Moon Phases','3-d moon','3d moon','3d moonphase'],
    '4-Year Calendar':['4-Year Calendar','4 year calendar'],
    'Alarm':['Alarm'],
    'Altitude Indicator':['Altitude Indicator','altitude'],
    'AM/PM Indication':['AM/PM Indication','am pm','am/pm'],
    'Annual Calendar':['Annual Calendar','yearly calendar','year calendar'],
    'Anti‑magnetic':['Anti‑magnetic','antimagnetic'],
    'Bi‑directional rotating bezel':['Bi‑directional rotating bezel','bidirectional rotating','bidirectional bezel'],
    'Calendar':['Calendar'],
    'Carousel':['Carousel'],
    'Central Minutes':['Central Minutes','central mins','center minutes'],
    'Central Seconds':['Central Seconds','center seconds'],
    'Chain & Fusée':['Chain & Fusée','chain fusee','fusee & chain','fusee','chain and fusee','fusee and chain'],
    'Chiming Clock':['Chiming Clock','striking','chime','chiming','sonnerie'],
    'Chinese Calendar':['Chinese Calendar'],
    'Chronograph':['Chronograph'],
    'Chronometer':['Chronometer'],
    'Co-Axial Escapement':['Co-Axial Escapement'],
    'Column Wheel':['Column Wheel'],
    'Compass':['Compass'],
    'Complete Calendar':['Complete Calendar'],
    'Constant Force':['Constant Force'],
    'Date':['Date'],
    'Day / Night Indication':['Day / Night Indication','day-night','day night','night and day'],
    'Day‑Date':['Day‑Date','day date','day and date','date-day','day+date'],
    'Depth Gauge':['Depth Gauge','depth'],
    'Diamonds':['Diamonds','diamond'],
    'Double Balance':['Double Balance'],
    'Equation of Time':['Equation of Time'],
    'Flashing Seconds':['Flashing Seconds','foudroyante'],
    'Flyback':['Flyback','flyback chronograph'],
    'Gemstones':['Gemstones','pave','diamond','gem-set','gemset'],
    'GMT':['GMT','Greenwich Mean Time','UTC'],
    'Grande Sonnerie':['Grande Sonnerie'],
    'Helium Escape Valve':['Helium Escape Valve','HEV','helium escape','escape valve','helium valve'],
    'Hours':['Hours','hour','HMS'],
    'Jumping Hours':['Jumping Hours','jumping hour'],
    'Jumping Minutes':['Jumping Minutes','jumping min'],
    'Jumping Seconds':['Jumping Seconds','jumping sec'],
    'Leap Year':['Leap Year','leapyear'],
    'Limited edition':['Limited edition','limited','boutique edition','one-of-a-kind','unique piece'],
    'Luminous':['Luminous'],
    'Master Chronometer Certified':['Master Chronometer Certified','Master Chronometer','METAS','Master Chrono'],
    'Minute Repeater':['Minute Repeater','repeating minutes','minute repeat'],
    'Minutes':['Minutes','HMS'],
    'Month':['Month','months'],
    'Monopusher':['Monopusher','monopoussoir'],
    'Moon Phases':['Moon Phases','moonphase','moon phase','moon phase','moon'],
    'Moon Position / Rising Time':['Moon Position / Rising Time','moon position','moon rising'],
    'Perpetual Calendar':['Perpetual Calendar'],
    'Petite Sonnerie':['Petite Sonnerie','Petit Sonnerie'],
    'Power Reserve Indication':['Power Reserve Indication','power reserve indicator'],
    'Pulsometer':['Pulsometer'],
    'Rattrapante':['Rattrapante','split-seconds'],
    'Regatta Timer':['Regatta Timer','Regatta timing','regatta'],
    'Retrograde Date':['Retrograde Date'],
    'Retrograde Display':['Retrograde Display','retrograde'],
    'Retrograde Hours':['Retrograde Hours','Retrograde Hour'],
    'Retrograde Minutes':['Retrograde Minutes','Retrograde Minute'],
    'Retrograde Seconds':['Retrograde Seconds','Retrograde Second'],
    'Screw‑in crown':['Screw‑in crown','screw-down','screw-in'],
    'Seconds':['Seconds','second','HMS'],
    'Sidereal Time':['Sidereal Time'],
    'Skeletonized':['Skeletonized','skeleton','see-through','transparent','openwork','open work'],
    'Sky Chart':['Sky Chart','skychart'],
    'Slide Rule':['Slide Rule'],
    'Small Seconds':['Small Seconds','small second'],
    'Split-Seconds':['Split-Seconds','Rattrapante'],
    'Stop-Second':['Stop-Second','stop seconds'],
    'Sun Position / Rising Time':['Sun Position / Rising Time','sun position','sun rising','sun rise'],
    'Sweeping Seconds':['Sweeping Seconds','sweeping sec'],
    'Tachymeter':['Tachymeter'],
    'Telemeter':['Telemeter'],
    'Thermometer':['Thermometer'],
    'Tide Indication':['Tide Indication','tide'],
    'Travel Time':['Travel Time','time zone','time zones','worldtime','timezone','timezones','GMT'],
    'Tourbillon':['Tourbillon'],
    'Transparent caseback':['Transparent caseback'],
    'Triple Calendar':['Triple Calendar','complete calendar'],
    'Unidirectional rotating bezel':['Unidirectional rotating bezel','uni-directional bezel','uni-directional rotating'],
    'Week Indicator':['Week Indicator','week'],
    'Weekly Calendar':['Weekly Calendar'],
    'World Time':['World Time','worldtime'],
    'Year Indicator':['Year Indicator','year'],
    'Zodiac Indications':['Zodiac Indications','zodiac'] 
}
def check_features(x, match_dict):
    features_list = []
    for key, value in match_dict.items():
        for v in value:
            if x == None:
                return None
            if v.lower() in str(x).lower() and key not in features_list:
                features_list.append(key)
    if features_list == []:
        return None
    return ', '.join(features_list)

### crystal ###
crystal_dict = {
    'Sapphire':['Sapphire','saphire','cream'],    
    'Plexiglass':['Plexiglass','plexi'],
    'Hesalite':['Hesalite'],
    'Acrylic':['Acrylic'],
    'Hardlex':['Hardlex'],
    'Ion-X':['Ion-X','ion x','ionx'],
    'Mineral Glass':['Mineral Glass','mineral']
}
def check_crystal(x, match_dict):
    color_list = []
    for key, value in match_dict.items():
        for v in value:
            if x == None:
                return None
            if v.lower() in str(x).lower() and key not in color_list:
                color_list.append(key)
    if color_list == []:
        return None
    return ', '.join(color_list)

### case_shape ###
case_shape_dict = {
    'Round':['Round'],    
    'Rectangular':['Rectangular','rectangle'],  
    'Tonneau':['Tonneau','barrel'],  
    'Cushion':['Cushion'],  
    'Irregular':['Irregular'],  
    'Oval':['Oval'],  
    'Square':['Square'],  
    'Octagon':['Octagon','octagonal'],
    'Hexagon':['Hexagon']
}
def check_case_shape(x, match_dict):
    shape_list = []
    for key, value in match_dict.items():
        for v in value:
            if x == None:
                return None
            if v.lower() in str(x).lower() and key not in shape_list:
                shape_list.append(key)
    if shape_list == []:
        return None
    return ', '.join(shape_list)

### caseback ###
caseback_dict = {
    'Solid':['Solid', 'Closed'],    
    'Transparent':['Transparent','Open','Openwork','Skeleton','Exhibition'],  
    'Hinged':['Hinged'],  
    'Hunter':['Hunter']      
}

def check_caseback(x, match_dict):
    caseback_list = []
    for key, value in match_dict.items():
        for v in value:
            if x == None:
                return None
            if v.lower() in str(x).lower() and key not in caseback_list:
                caseback_list.append(key)
    if caseback_list == []:
        return None
    return ', '.join(caseback_list)

# Clean currency field

currency_codes = CurrencyCodes()
all_currency_codes = Currency.get_currency_formats()

def get_currency_code_from_symbol(symbol):
    
    '''
    Convert currency symbol to the currency code
    '''
    
    if symbol.strip()=='$': # The currency library doesn't always return the correct currency code, as "$" is ambigious. For our purposes, we assume it's USD
        return 'USD'
    elif symbol.strip()=='£':
        return 'GBP'
    else:
        code = currency_codes.get_currency_code_from_symbol(symbol)
        if code:
            return code
        else:
            return ''

def currency_function(currency):
    try:
        # Return the currency code if we can extract the 3 letter currency code from this field
        currency_code = [code for code in all_currency_codes if code in currency.upper()][0]
        return currency_code
    except:
        # Otherwise, we try to extract the currency symbol and then convert to currency code
        try:
            currency_symbol = ''.join(re.findall(r'[^A-Za-z0-9]', currency))
            currency_code = get_currency_code_from_symbol(currency_symbol)
            return currency_code
        except:
            return ''

# Clean price field

def price_function(price):
    # Remove anything that isn't a digit or "." eg. "$3,000.00" -> "3000.00"
    try:
        price = str(int(float(re.sub(r'[^\d.]', '', price))))
    except:
        price = ''
    return(price)

# Clean Authenticity Guarantee field

authenticity_guarantee_dict = {
    'chrono24': 'Yes',
    'timepeaks': 'Yes',
    'redditwatchexchange': 'No',
    'watchuseek': 'Depends on seller',
    'rolex': 'Depends on seller',
    'bezel': 'Yes',
    'chronoext': 'Yes',
    'watchfinder': 'Yes',
    'affordableswisswatchesinc': 'Yes',
    'luxurywatchesusa': 'Yes',
    'worldofluxuryus': 'Yes',
    'cwsellors': 'Yes',
    'hodinkee': 'Yes',
    'the1916company': 'Yes',
    'catawiki': 'Depends on seller',
    'chronocentric': 'Depends on seller',
    'collectorsquare': 'Yes',
    'crownandcaliber': 'Yes',
    'farfetch': 'Yes',
    'hairspring': '',
    'bobswatches': 'Yes',
    'analogshift': 'Yes',
    'omegaforums': 'Depends on seller',
    'stockx': 'Yes',
    'timezone': 'Depends on seller',
    '1stdibs': 'Yes',
    'amjwatches': 'Yes',
    'ernestjones': 'Yes',
    'tourneau': 'Yes',
    'grailzee': 'Yes',
    'sothebys': '',
    'liveauctioneers': '',
    'watchcollecting': '',
    'uhrforum': 'Depends on seller',
    'watchcentre': 'Yes',
    'watchnet': '',
    'loro': 'Yes',
    'tropicalwatch': 'Yes',
    'redditwatchswap': 'Depends on seller',
    'amsterdamwatchcompany': 'Yes',
    'shucktheoyster': '',
    'omegavintage': 'Yes',
    'antoinedemacedo': 'Yes',
    'seikoandcitizenwatchforum': 'Depends on seller',
    'watchprosit': 'Depends on seller',
    'watchtalkforums': 'Depends on seller',
    'wristsushi': 'Depends on seller',
    'sothebysbucherer': '',
    'omegaenthusiast': '',
    'windvintage': '',
    'thekeystone': 'Yes',
    'rarebirds': '',
    'davideparmegiani': '',
    'watches83': '',
    'amsterdamvintagewatches': '',
    'vintagerolexforum': 'Depends on seller',
    'watchfreeks': 'Depends on seller',
    'watchesdotcom': 'Yes',
    'bernardinimilano': '',
    'auctionata': '',
    'bonham': '',
    'phillips': '',
    'therealreal': '',
    'thewatchforum': 'Depends on seller',
    'watchclub': '',
    'watchesofswitzerland': 'Yes',
    'etsy': 'Depends on seller',
    'jomashop': 'Yes',
}

def authenticity_guarantee_function(listing_title, store_name):
    if store_name=='ebay': # If the source is ebay, we try to extract whether there is authenticity guarantee from the listing title
        try:
            if re.findall(r'authentic(?:ity)? guarantee', listing_title.lower()) or re.findall(r'guarantee[d]? authentic', listing_title.lower()):
                return 'Yes'
            else:
                return 'No'
        except:
            return 'No'
    else: # Otherwise, we return the dictionary value
        try:
            authenticy_guarantee = authenticity_guarantee_dict[store_name]
            return authenticy_guarantee
        except:
            return ''

# Clean jewels field

def jewels_function(jewels, store_name):
    if store_name in ['chrono24', 'hodinkee']:
        to_remove = r'[^\d.]'
        search_pattern = r'\d{1,3}(?:,?\d{3})*(?:\.\d+)?'
    else:
        to_remove = r'[^\d]'
        search_pattern = r'\d{1,3}\)?\s{,3}(?:diamond|jewel|rub|stone)'
    # Remove anything that isn't a number eg. "25.0 jewels" -> "25.0"
    try:
        jewels_cleaned = re.sub(to_remove, "", re.findall(search_pattern, jewels.lower())[0])
    except:
        return ''
    try:
        if float(jewels_cleaned)>500:
            return ''
    except:
        return ''
    return jewels_cleaned

# Clean up power reserve

def power_reserve_function(power_reserve):
    # Assumption is that power reserve is measured in hours. Unless "day" is specified
    if 'day' in power_reserve.lower() or 'days' in power_reserve.lower():
        power_reserve = re.sub(r'[^\d.]', '', power_reserve) # Remove anything that isn't a digit nor "."
        try:
            power_reserve = f'{round(float(power_reserve)*24, 2)} hours' # Convert days to hours
        except:
            power_reserve = ''
        return power_reserve
    else:
        power_reserve = re.sub(r'[^\d.]', '', power_reserve) # Remove anything that isn't a digit nor "."
        if power_reserve:
            return f'{power_reserve} hours'
        else:
            return ''
        
# Clean up lugs

def lugs_function(lug, store_name):
    if store_name=='ebay':
        # eBay data is messier, so we need stricter rules for cleaning the data
        lug=lug.replace(',', '.').lower().strip()
        results = re.findall(r'\d{1,3}(?:\.\d+)?\)?\s*(?:mm|milli)', lug)
        if results:
            numerical = float(re.sub(r'[^\d.]', '', results[0]))
            return f'{numerical} mm'
        results = re.findall(r'\d{1,3}(?:\.\d+)?\)?\s*cm', lug)
        if results:
            numerical = float(re.sub(r'[^\d.]', '', results[0]))
            return f'{numerical*10} mm'
        results = re.findall(r'\d{1,3}(?:\.\d+)?\)?\s*(?:in|"|\')', lug)
        if results:
            numerical = float(re.sub(r'[^\d.]', '', results[0]))
            return f'{round(numerical*25.4, 2)} mm'
        return ""
    else:
        if store_name!='chrono24':
            lug = lug.replace(',', '.')
        # Remove anything that isn't a digit nor "."
        lug = re.sub(r'[^\d.]', '', lug)
        if lug:
            lug = f'{lug} mm'
        else:
            lug = ''
        return lug

# Clean up weight

def weight_function(weight):
    # If measured in grams
    results = re.findall(r'\d{1,3}(?:,?\d{3})*(?:\.\d+)?\s*g', weight.lower().strip())
    if results:
        try:
            number = float(re.sub(r'[^\d.]', '', results[0]))
            return f"{number} g"
        except:
            return ''
    # If measured in ounces
    results = re.findall(r'\d{1,3}(?:,?\d{3})*(?:\.\d+)?\s*(?:ounce|oz)', weight.lower().strip())
    if results:
        try:
            number = float(re.sub(r'[^\d.]', '', results[0]))*28.3495
            return f"{number} g"
        except:
            return ''
    return ''
        
# Clean up year of production

def year_of_production_function(year_of_production):
    # Finding the first 4 digit year and ensuring it's integer format
    try:
        year_of_production = re.findall(r'\d{4}', year_of_production)[0].split('.')[0]
    except:
        return ''
    # Ignore any years before 1800 and after 2100, and years that don't adhere to 4 digit format
    if year_of_production[:2] not in ['18', '19', '20']:
        return ''
    return year_of_production

# Clean up decade of production

def decade_of_production_function(decade_of_production, year_of_production):
    # If decade of production is empty, we would grab the year of production if it exists
    if decade_of_production=='':
        decade_of_production = year_of_production
    # Remove any values that aren't digits, "-", nor "." eg. "Circa 2004-2008" -> "2004-2008"
    decade_of_production = re.sub(r'[^0-9\-.]', '', decade_of_production.lower().replace(' to ', '-'))
    split_years = []
    # To deal with messy data, we only get the first two or four digit number for the "from" and "to"
    # eg. "1975 1982 - 1998" -> [1975, 1998]
    for decade in decade_of_production.split('-'):
        try:
            split_years.append(re.findall(r'(\d{4}|\d{2})', decade)[0].split('.')[0])
        except:
            pass
    decade_list = []
    for n in split_years:
        try:
            if len(n)==2 and len(split_years[0])==4:
                # Only if the "from" year is 4 digits do we add the century of the "from" year to that of the "to" year
                # eg. 1988-96 -> 1988-1996
                decade = split_years[0][:2] + "{:02d}".format(int(n)//10*10)
                decade_list.append(decade)
            elif len(n)==4 and n[:2] in ['18', '19', '20']:
                # Checking if the 4 letter year is between the 19th-21st century
                decade = "{:04d}".format(int(n)//10*10)
                decade_list.append(decade)
        except:
            pass
    # Removing duplicate decades ["1970", "1970"] -> ["1970"] and appending "s" to the end of the decade eg. "1970" -> "1970s"
    decade_list = ', '.join(map(lambda x: x+'s', list(sorted(set(decade_list)))))
    return(decade_list)

# Clean water resistance

def water_resistance_function(water_resistance):
    
    # If water resistance is blank, we return blank
    if (not water_resistance):
        return ''
    # If water resistance consists of either "none", "not water resistant", "no", or is 0 or its variations (0.0, 0.00, etc.), we return "Not water resistant"
    not_water_resistant_list = ['none', 'not water resistant', 'no']
    text_only = re.sub('[^A-Za-z]', ' ', water_resistance.lower()).strip()
    for nwr in not_water_resistant_list:
        if word_in_text(nwr, text_only):
            return 'Not water resistant'
    if re.match(r'^0+$', re.sub(r'\D', '', water_resistance.lower()).strip()):
        return 'Not water resistant'
    
    # We extract the number and the unit (only ATM, bar, meter, m, meter are applicable units)
    # If the number is 0, it is not water resistant
    results = re.findall(r'\d{1,3}(?:,?\d{3})*(?:\.\d+)?\s*(?:atm|bar)', water_resistance.lower().strip())
    if results: # If ATM or bar
        numerical = float(re.sub(r'[^\d.]', '', results[0]))
        if numerical==0:
            return 'Not water resistant'
        water_resistance = f'{numerical} ATM'
        return water_resistance
    results = re.findall(r'\d{1,3}(?:,?\d{3})*(?:\.\d+)?\s*(?:m|meter|metre)', water_resistance.lower().strip())
    if results: # If meters
        numerical = float(re.sub(r'[^\d.]', '', results[0]))
        if numerical==0:
            return 'Not water resistant'
        water_resistance = f'{numerical/10} ATM'
        return water_resistance
    results = re.findall(r'\d{1,3}(?:,?\d{3})*(?:\.\d+)?\s*(?:ft|feet)', water_resistance.lower().strip())
    if results: # If feet
        numerical = float(re.sub(r'[^\d.]', '', results[0]))
        if numerical==0:
            return 'Not water resistant'
        water_resistance = f'{round(numerical/(3.281*10), 2)} ATM'
        return water_resistance
    return ''

# Clean seller type

allowed_sellers = ['Dealer', 'Private Seller', 'Retailer', 'Auction House']

def seller_type_function(seller_type):
    
    if seller_type.lower().strip()=='professional dealer':
        return 'Dealer'

    seller_type = seller_type.title().strip()
    if seller_type not in allowed_sellers:
        seller_type = ''
    return seller_type

# Clean listing type

allowed_listings = {'Auction': ['place bid', 'click here for auction'],
                    'Buy it Now': ['buy now', 'add to cart'],
                    'Contact Seller': ['inquire']
                    }

def listing_type_function(listing_type):
    
    # The allowable listing formats are the keys in allowed_listings dict
    # If we find that the listing type is the same as the values in the allowed_listings dict, we return the key
    # eg. "buy now" -> "Buy it Now"
    proper_listings = []
    listing_types = re.split(r',\s?', listing_type)
    for listings in listing_types:
        try:
            listings = [val for val in allowed_listings if listings.lower().strip()==val.lower().strip()][0]
            if listings:
                proper_listings.append(listings)
        except:
            for k, v in allowed_listings.items():
                if listings.lower().strip() in v:
                    proper_listings.append(k)
    if proper_listings:
        return ', '.join(proper_listings)
    else:
        return ''

# Clean "type" column

allowed_types = {"Unisex Watches, Men's Watches, Women's Watches": ["men/women",
                                                                    "mens/womens watch",
                                                                    "mens/womens watches",
                                                                    "Men's watch/Unisex",
                                                                    "unisex",
                                                                    "unisex watch",
                                                                    "unisex watches",
                                                                    "Unisex's Watches",
                                                                   "Women's Watches, Men's Watches"],
                    "Women's Watches": ["lady", "ladies", "ladys", "women", "Female", "Women's watch"],
                    "Men's Watches": ["Mens", 
                                      "Men",
                                      "Mens Watch",
                                      "Mens Watches",
                                      "Men's",
                                      "gent",
                                      "gents",
                                      "Male"],
                    "Pocket Watches": ["pocket watch", "FOB watch", "fob watch", "pocket"],
                    "Other / Clock": ["clock", "clocks"]
                    }

def type_function(_type):
    try:
        _type = [val for val in allowed_types if _type.lower().strip()==val.lower().strip()][0]
        if _type:
            return _type
    except:
        for k, v in allowed_types.items():
            for value in v:
                if _type.lower().strip()==value.lower().strip():
                    return k
    return ''

# Cleaning country

# List of accepted country spellings
allowed_countries = '''
Afghanistan
Albania
Algeria
Andorra
Angola
Antigua and Barbuda
Argentina
Armenia
Australia
Austria
Azerbaijan
Bahamas
Bahrain
Bangladesh
Barbados
Belarus
Belgium
Belize
Benin
Bhutan
Bolivia
Bosnia and Herzegovina
Botswana
Brazil
Brunei
Bulgaria
Burkina Faso
Burundi
Cabo Verde
Cambodia
Cameroon
Canada
Central African Republic
Chad
Chile
China
Colombia
Comoros
Congo
Costa Rica
Croatia
Cuba
Cyprus
Czech Republic
Denmark
Djibouti
Dominica
Dominican Republic
Ecuador
Egypt
El Salvador
Equatorial Guinea
Eritrea
Estonia
Eswatini
Ethiopia
Fiji
Finland
France
Gabon
Gambia
Georgia
Germany
Ghana
Greece
Grenada
Guatemala
Guinea
Guinea-Bissau
Guyana
Haiti
Honduras
Hong Kong
Hungary
Iceland
India
Indonesia
Iran
Iraq
Ireland
Israel
Italy
Jamaica
Japan
Jordan
Kazakhstan
Kenya
Kiribati
North Korea
South Korea
Kosovo
Kuwait
Kyrgyzstan
Laos
Latvia
Lebanon
Lesotho
Liberia
Libya
Liechtenstein
Lithuania
Luxembourg
Madagascar
Malawi
Malaysia
Maldives
Mali
Malta
Marshall Islands
Mauritania
Mauritius
Mexico
Micronesia
Moldova
Monaco
Mongolia
Montenegro
Morocco
Mozambique
Burma
Namibia
Nauru
Nepal
Netherlands
New Zealand
Nicaragua
Niger
Nigeria
North Macedonia
Norway
Oman
Pakistan
Palau
Palestine
Panama
Papua New Guinea
Paraguay
Peru
Philippines
Poland
Portugal
Qatar
Romania
Russia
Rwanda
Saint Kitts and Nevis
Saint Lucia
Saint Vincent and the Grenadines
Samoa
San Marino
Sao Tome and Principe
Saudi Arabia
Senegal
Serbia
Seychelles
Sierra Leone
Singapore
Slovakia
Slovenia
Solomon Islands
Somalia
South Africa
South Sudan
Spain
Sri Lanka
Sudan
Suriname
Sweden
Switzerland
Syria
Taiwan
Tajikistan
Tanzania
Thailand
Timor-Leste
Togo
Tonga
Trinidad and Tobago
Tunisia
Turkey
Turkmenistan
Tuvalu
Uganda
Ukraine
United Arab Emirates
United Kingdom
United States
Uruguay
Uzbekistan
Vanuatu
Vatican City
Venezuela
Vietnam
Yemen
Zambia
Zimbabwe
    '''

allowed_countries = allowed_countries.split('\n')[1:-1]

# Getting the alternative values for the countries (based on what WCC identified)
allowed_countries_alt_values = {'Burma': ['myanmar'],
                            'United Arab Emirates': ['uae'],
                            'United Kingdom': ['uk'],
                            'United States': ['usa', 'us', 'america', 'united states of america'],
                            'Netherlands': ['the netherlands']
                            }

allowed_countries_dict = {}

# Getting the two and three letter country codes for the countries (eg. "United States" -> "US", "USA")
def get_alternative_names(country_name):
    country = pycountry.countries.get(name=country_name)
    if country:
        alternative_names = [country.alpha_2.lower(), country.alpha_3.lower()]  # ISO codes
        return alternative_names
    else:
        return []
    
# For every country in our list, we extract the alternative values as well as the 2 and 3 letter country codes
for country in allowed_countries:
    allowed_countries_dict[country] = get_alternative_names(country)
    if country in allowed_countries_alt_values:
        allowed_countries_dict[country] += allowed_countries_alt_values[country]
        allowed_countries_dict[country] = list(set(allowed_countries_dict[country]))

def country_function(country, store_name):
    
    # If the country has an exact match with the key in our dictionary of allowed countries after standardizing certain characters (eg. "-" -> " " and " and " -> " & "), lower casing it, and removing trailing white spaces,
    # We return the country in the format we set in our list of allowed countries
    try:
        if store_name in ['timepeaks', 'timezone']: # For timepeaks, the country field is a sentence
            country = [val for val in allowed_countries_dict if unidecode(val.lower().replace('-', ' ').replace(' and ', ' & ').strip()) in unidecode(country.lower().replace('-', ' ').replace(' and ', ' & ').strip())][0]
        else:
            country = [val for val in allowed_countries_dict if unidecode(country.lower().replace('-', ' ').replace(' and ', ' & ').strip())==unidecode(val.lower().replace('-', ' ').replace(' and ', ' & ').strip())][0]
        return country
    except:
        # If there is no exact match for our key, we try to see if there's a match in the values. If so, we return the key of that particular value. (eg. "UK" -> "United Kingdom")
        for k, v in allowed_countries_dict.items():
            if unidecode(country.lower().replace('-', ' ').replace(' and ', ' & ').strip()) in v:
                return k
    return ''

# Cleaning seller rating

# If seller rating is a percentage (0-100%)
def seller_conversion_percentage(rating):
    if rating<=0:
        return 'Unavailable'
    elif rating<50:
        return 'Very Poor'
    elif rating<60:
        return 'Poor'
    elif rating<70:
        return 'Average'
    elif rating<80:
        return 'Good'
    elif rating<90:
        return 'Great'
    elif rating<=100:
        return 'Excellent'
    return ''

# If the seller rating is from a 0-5 scale
def seller_conversion_out_of_five(rating):
    if rating<=0:
        return 'Unavailable'
    elif rating<2.5:
        return 'Very Poor'
    elif rating<3:
        return 'Poor'
    elif rating<3.5:
        return 'Average'
    elif rating<4:
        return 'Good'
    elif rating<4.5:
        return 'Great'
    elif rating<=5:
        return 'Excellent'
    return ''

def seller_rating_function(seller_rating):
    
    allowed_ratings = ['Excellent', 'Great', 'Good', 'Average', 'Poor', 'Very Poor', 'Unavailable']
    
    # If the seller rating exists in our list of allowed ratings, we return it as such
    seller_rating = seller_rating.title().strip()
    if seller_rating in allowed_ratings:
        return seller_rating
    
    seller_rating_numerical = re.sub(r'[^\d.]', '', seller_rating) # Removing anything that isn't a digit nor "."
    if seller_rating_numerical:
        # If seller is expressed as a percentage, we convert the percentage to our list of allowed seller ratings
        if '%' in seller_rating:
            try:
                seller_rating = seller_conversion_percentage(float(seller_rating_numerical))
            except:
                seller_rating = ''
            return seller_rating
        else:
            # Otherwise, we assume it's from a 0-5 scale and we convert this to our list of allowed seller ratings
            try:
                seller_rating = seller_conversion_out_of_five(float(seller_rating_numerical))
            except:
                seller_rating = ''
            return seller_rating
    return ''
    
# Cleaning diameter

def diameter_function(diameter):
    # Splitting diameter (eg. "45x22mm" -> ["45", "22mm"]) and replacing "," with ".". I noticed sometimes they express decimals with "," rather than "." (eg. 20,5mm)
    diameter_split = re.split(r'[_&x×хX*:\|]', diameter.replace(',', '.'))
    diameter_cleaned = []
    for d in diameter_split:
        try: # Finding all numbers in the dimension
            nums = re.findall(r'\d+(?:\.\s?\d+)?', d)
            if len(nums)>1: # If a dimension (length, width, or height) has more than 1 value, we return empty to avoid ambiguity
                return ''
            extracted_nums = re.sub(r'[^\d.]', '', nums[0])
        except:
            extracted_nums = ''
        if extracted_nums: # Only if extracted_nums isn't blank we add it to our diameter dimensions. 
            # We don't want to have a situation where we have "x" without anything attached to it (eg. "x 22mm")
            diameter_cleaned.append(extracted_nums)
    if diameter_cleaned:
        # If "cm" is in the original diameters data, we convert to "mm"
        if re.search(r'\d\s*cm', diameter.lower()):
            try:
                return (' x '.join([str(float(d)*10) for d in diameter_cleaned]) + ' mm')
            except:
                return ''
        # If measurement is provided in inches we convert to "mm"
        if re.search(r'(\d\s*in|\d\s*[\'”"])', diameter.lower()):
            try:
                return (' x '.join([str(float(d)*25.4) for d in diameter_cleaned]) + ' mm')
            except:
                return ''
        # Otherwise, we convert our list to the desired string format (eg. ["22", "44"] -> "22 x 44 mm")
        return (' x '.join([str(float(d)) for d in diameter_cleaned]) + ' mm')
    return ''
# Cleaning brand

allowed_brands = '''
A. Lange & Söhne
ABP Paris
Accutron
AD-Chronographen
Aerowatch
Aigle
Aigner
Alain Silberstein
Alexander Shorokhoff
Alexandre Meerson
Alfred Dunhill
Alfred Rochat & Fils
Alpina
Altanus
Andersen Genéve
Angelus
Angular Momentum
Anonimo
Apple
Aquanautic
Aquastar
Archimede
Aristo
Armand Nicolet
Armani
Armin Strom
Arnold & Son
Artisanal
Artya
Askania
Ateliers deMonaco
Atlantic
Audemars Piguet
Auguste Reymond
Auricoste
Azimuth
Azzaro
B.R.M
Ball
Backes & Strauss
Balmain
Baltic
Barington
Baume & Mercier
Bausele
Bedat & Co
Behrens
Bell & Ross
Benrus
Benzinger
Bertolucci
Beuchat
Bifora
Black-Out Concept
Blancpain
blu
Boegli
Bogner Time
Boldr
Bomberg
Boucheron
Bovet
Breguet
Breil
Breitling
Brellum
Bremont
Breva
Bruno Söhnle
Bvlgari
Bulova
Bunz
Burberry
BWC-Swiss
C.H. Wolf
Cabestan
Cadet Chronostar
Camel Active
Camille Fournet
Candino
Carl F. Bucherer
Carl Suchy & Söhne
Carlo Ferrara
Cartier
Casio
Catena
Catorex
Cattin
Century
Cerruti
Certina
Chanel
Charmex
Charriol
Chase-Durer
Chaumet
Chopard
Chris Benz
Christiaan van der Klaauw
Christofle
Christophe Claret
Christopher Ward
Chronographe Suisse Cie
Chronoswiss
Churpfälzischen UhrenManufactur
Citizen
ck Calvin Klein
Claude Bernard
Claude Meylan
Clerc
Code41
Concord
Condor
Cornehl
Cortébert
Corum
Cronus
Cuervo y Sobrinos
Cvstos
CWC
Cyclos
Cyma
Cyrus
Czapek
D. Dornblueth & Sohn
Damasko
Daniel Roth
Davidoff
Davosa
De Bethune
De Grisogono
Deep Blue
Defakto
DeLaCour
DeLaneau
Delbana
Delma
Devon
Dewitt
Diesel
Dietrich
Dior
Dodane
Dolce & Gabbana
Dom Baiz International
Doxa
Dubey & Schaldenbrand
DuBois 1785
DuBois et fils
Dufeau
Dugena
Ebel
Eberhard & Co.
Edox
Eichmüller
Election
Elgin
Elysee
Emile Chouriet
Engelhardt
Enicar
Ennebi
Epos
Ernest Borel
Ernst Benz
Erwin Sattler
Esprit
Eterna
Eulit
Eulux
F.P. Journe
Fabergé
Favre Leuba
Fendi
Festina
Fiona Krüger
Fluco
Fludo
Formex
Fortis
Fossil
Franc Vila
Franck Dubarry
Franck Muller
Fred
Frederique Constant
Furlan Marri
Gerald Genta
Gübelin
Gaga Milano
Gallet
Gant
Gard√©
Garmin
Georges V
Gerald Charles
Germano & Walter
Gevril
Gigandet
Girard-Perregaux
Giuliano Mazzuoli
Glashütte Original
Glycine
Grönefeld
Graf
Graham
Grand Seiko
Greubel Forsey
Grovana
Gruen
GUB Glashütte
Gucci
Guy Ellia
Guess
H. Moser & Cie.
Habring²
Hacher
Haemmer
Hagal
Hamilton
Hanhart
Harry Winston
Hautlence
HD3
Hebdomas
Hebe
Helvetia
Hentschel Hamburg
Hermès
Herzog
Heuer
Hirsch
Hublot
Hugo Boss
HYT
Ice Watch
Ikepod
Illinois
Ingersoll
Invicta
Iron Annie
Itay Noy
IWC
Jacob & Co.
Jacques Etoile
Jacques Lemans
Jaeger-LeCoultre
Jaermann & Stübi
Jaquet Droz
JB Gioacchino
Jean d'Eve
Jean Lassale
Jean Marcel
JeanRichard
Joop
Hysek
Jules Jurgensen
Junghans
Junkers
Justex
Juvenia
Kelek
KHS
Kienzle
Kobold
Konstantin Chaykin
Korloff
Krieger
Kronsegler
L.Leroy
L'Ep√©e
L√ºm-Tec
La Joux-Perret
Laco
Lacoste
Lancaster
Lanco
Lang & Heyne
Laurent Ferrier
Lebeau-Courally
Lemania
Leonidas
Leroy
Limes
Lindburgh + Benson
Linde Werdelin
Lip
Liv Watches
Locman
Longines
Longio
Lorenz
Lorus
Louis Erard
Louis Moinet
Louis Vuitton
Louis XVI
Lucien Rochat
Ludovic Ballouard
Luminox
Lundis Bleus
M.A.D. Editions
M&M Swiss Watch
Mühle Glashütte
Maîtres du Temps
Marcello C.
Margi
Marlboro
Martin Braun
Marvin
Maserati
Mathey-Tissot
Mauboussin
Maurice de Mauriac
Maurice Lacroix
MB&F
Meccaniche Veloci
Meistersinger
Mercure
Mercury
Meva
Meyers
Michael Kors
Michel Herbelin
Michel Jordi
Michele
Mido
Milleret
Milus
Minerva
Ming
Momentum
Momo Design
Mondaine
Mondia
Montblanc
Montega
Morellato
Moritz Grossmann
Movado
N.B. Yäeger
N.O.A
Nautica
Nauticfish
Nethuns
Nike
Nina Ricci
Nivada Grenchen
Nivrel
Nixon
Nomos Glashütte
Norqain
Nouvelle Horlogerie Calabrese (NHC)
ODM
Officina del Tempo
Offshore Limited
Ollech & Wajs
Omega
Orator
Orbita
Orfina
Orient
Oris
Otumm
Out of Order
Pacardt
Panerai
Parmigiani Fleurier
Patek Philippe
Paul Picot
Pequignet
Perigáum
Perrelet
Perseo
Philip Stein
Philip Watch
Piaget
Pierre Balmain
Pierre Cardin
Pierre DeRoche
Pierre Kunz
Police
Poljot
Porsche Design
Prim
Pro-Hunter
Pryngeps
Pulsar
Puma
Purnell
Quinting
Rado
Raidillon
Rainer Brand
Rainer Nienaber
Raketa
Ralf Tech
Ralph Lauren
Raymond Weil
Rebellion
REC Watches
Record
Reservoir
Ressence
Revue Thommen
RGM
Richard Mille
Rios1931
Roamer
Roger Dubuis
Rolex
Rolf Lang
Romain Gauthier
Romain Jerome
Rotary
Rothenschild
ROWI
RSW
Ryser Kentfield
S.Oliver
S.T. Dupont
Salvatore Ferragamo
Sandoz
Sarcar
Scalfaro
Schaumburg
Schwarz Etienne
Sea-Gull
Sector
Seiko
Sevenfriday
Shinola
Sicura
Sinn
Skagen
Slava
Snyper
Solvil
Sothis
Speake-Marin
Spinnaker
Squale
Starkiin
Steelcraft
Steinhart
Stowa
Strom
Stuhrling
Swatch
Swiss Military
TAG Heuer
Tavannes
TB Buti
Technomarine
Technos
Tecnotempo
Temption
Tendence
Terra Cielo Mare
Theorein
Thomas Ninchritz
Tiffany & Co.
Timberland Watches
Timex
Tissot
Titoni
Titus
Tockr
Tommy Hilfiger
Tonino Lamborghini
Traser
Tudor
Tutima Glashütte
Trilobe
TW Steel
U-Boat
Ublast
Ulysse Nardin
Unikatuhren
Unimatic
Union Glashütte
Universal Genève
Urban Jürgensen
Urwerk
Vacheron Constantin
Valbray
Valentino
Van Cleef & Arpels
Van Der Bauwede
Vangarde
Venezianico
Ventura
Versace
Vianney Halter
Viceroy
Victorinox Swiss Army
Villemont
Vincent Calabrese
Visconti
Vixa
Vogard
Volna
Vostok
Vulcain
Wakmann
Waltham
Welder
Wempe Glashütte
Wenger
Werenbach
Wittnauer
Women's watch
Wyler
Wyler Vetta
Xemex
Xetum
Yantar
Yema
Yes Watch
Yves Saint Laurent
Zeitwinkel
Zelos
Zenith
Zeno-Watch Basel
ZentRa
Zeppelin
Zodiac
ZRC
'''

# Sorting from longest to shortest (prevent returning "Heuer" when the listing title has "TAG Heuer")
allowed_brands = sorted(allowed_brands.split('\n')[1:-1], key=len, reverse=True)

# Allowed brands and their alternative values
allowed_brands_alt_values = {'F.P. Journe': ['FP Journe', 'F.P.Journe', 'FPJourne'],
                            'Ball': ['Ball Watch', 'Ball Watches'],
                            'Frederique Constant': ['Frédérique Constant'],
                            'Eberhard & Co.': ['Eberhard & Co', 'Eberhard'],
                            'H. Moser & Cie.': ['H. Moser & Cie', 'H Moser & Cie', 'H Moser & Cie.', 'H. Moser'],
                            'Ateliers deMonaco': ['Ateliers de Monaco'],
                            'Jacob & Co.': ['Jacob & Co'],
                            'Bvlgari': ['Bulgari'],
                            'Alfred Dunhill': ['Dunhill'],
                            'Armani': ['Emporio Armani'],
                            'Carl F. Bucherer': ['Carl F.Bucherer'],
                            'Victorinox Swiss Army': ['Victorinox'],
                            'A. Lange & Söhne': ['A Lange & Söhne', 'A Lange & Sohne', 'A. Lange & Sohne', 'A.Lange & Sohne'],
                            'Tiffany & Co.': ['Tiffany & Co', 'Tiffany'],
                            'Parmigiani Fleurier': ['Parmigiani', 'Parmigani Fleurier'],
                            'Graf': ['Graff'],
                            'Nomos Glashütte': ['Nomos'],
                            'Moritz Grossmann': ['Moritz Grossman'],
                            'Bedat & Co': ['Bedat & Co.'],
                            'B.R.M': ['B.R.M Watches'],
                            'Dior': ['Christian Dior', 'Dior Homme'],
                            'Patek Philippe': ['Patek Phillippe'],
                            'JeanRichard': ['Jean Richard'],
                            'TB Buti': ['Buti'],
                            'Tutima Glashütte': ['Tutima'],
                            'Raymond Weil': ['Raimond Weil'],
                            'Pierre Kunz': ['Pierrekunz'],
                            'Sevenfriday': ['Seven Friday'],
                            'Schaumburg': ['Schauburg'],
                            'Meistersinger': ['Meister Singer'],
                            'Yves Saint Laurent': ['Saint Laurent'],
                            'Dewitt': ['De Witt']
                            }

# Converting the alternative values to their base format (eg. "A Lange & Söhne" -> "a lange & sohne") to standardize it so that we account for variability in how different sites format their brands
for k, v in allowed_brands_alt_values.items():
    allowed_brands_alt_values[k] = [unidecode(val.lower().strip().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' ')) for val in v]

allowed_brands_dict = {}
    
# Creating the dictionary for every brand in our list of allowed brand formats (includes alternative values if they exist)
for brand in allowed_brands:
    if brand in allowed_brands_alt_values:
        allowed_brands_dict[brand] = allowed_brands_alt_values[brand]
    else:
        allowed_brands_dict[brand] = []

# def brand_function(brand, listing_title):
    
#     # If brand exists
#     if brand:
#         # we would scan it to our list of allowed brands. If there is a match, we return the key
#         # eg. "vacheron constantin" -> "Vacheron Constantin"
#         try:
#             brand = [val for val in allowed_brands_dict if unidecode(brand.lower().strip().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' '))==unidecode(val.lower().strip().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' '))][0]
#             return brand
#         # Otherwise, we would check if it's in the alternative values list. If so, we return the key of that value
#         # eg. "A Lange & Sohne" -> "A. Lange & Söhne"
#         except:
#             for k, v in allowed_brands_dict.items():
#                 if unidecode(brand.lower().strip().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' ')) in v:
#                     return k
#     # If the brand doesn't exist, then we extract it from the listing_title field
#     else:

#         for k, v in allowed_brands_dict.items():
#             if word_in_text(unidecode(k.lower().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' ')), unidecode(listing_title.lower().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' '))):
#                 return k
#             for val in v:
#                 if word_in_text(val, unidecode(listing_title.lower().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' '))):
#                     return k

#         # for k, v in allowed_brands_dict.items():
#         #     if k.lower().strip() not in ['ball', 'blu', 'lip']: # Create a special rule for these brands because they're words that can be part of another word and we don't want false positives when extracting from listing titles
#         #         if unidecode(k.lower().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' ')) in unidecode(listing_title.lower().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' ')):
#         #             return k.strip()
#         #         for val in v:
#         #             if val in unidecode(listing_title.lower().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' ')):
#         #                 return k.strip()
#         #     else:
#         #         if listing_title.lower().strip().startswith(f"{k.lower().strip()} ") or listing_title.lower().strip().endswith(f" {k.lower().strip()}") or f' {k.lower().strip()} ' in listing_title.lower().strip():
#         #             return k.strip()
#     return ''

def brand_function(brand, listing_title):
    try:
        if brand:
            brand = [val for val in allowed_brands_dict if unidecode(brand.lower().strip().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' ')) == unidecode(val.lower().strip().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' '))][0]
            return brand
        else:
            for k, v in allowed_brands_dict.items():
                if word_in_text(unidecode(k.lower().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' ')), unidecode(listing_title.lower().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' '))):
                    return k
                for val in v:
                    if word_in_text(val, unidecode(listing_title.lower().replace(' and ', ' & ').replace(' et ', ' & ').replace('-', ' '))):
                        return k
    except Exception as e:
        print(f"Error processing brand: {brand}, listing_title: {listing_title}, error: {e}")
    return ''
    
# Clean reference number

# Defining different rules for extracting reference numbers depending on the brand

def nomos_glashutte(ref_no, return_blank=False):
    pattern = r'\d{3,4}(?:\.[A-Z0-9]{1,3})?'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no


def meister_singer(ref_no, return_blank=False):
    patterns = [r'(?:S|ED)\-[A-Z0-9]+', r'[A-Z]{2,3}\d{3}\w*']
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            return ref_no_cleaned[0]
    if return_blank:
        return ''
    else:
        return ref_no

def rolex(ref_no, return_blank=False):
    pattern = r'[Mm]?\d{6}[A-Za-z]{,4}(?:\-\d{4})?'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

def omega(ref_no, return_blank=False):

    patterns = {r'\d{3}\.\d{2}\.\d{2}\.\d{2}\.\d{2}\.\d{3}': [(r'\d{3}[\s\-]?\d{2}[\s\-]?\d{2}[\s\-]?\d{2}[\s\-]?\d{2}[\s\-]?\d{3}', r're.sub(r"(\d{3})(\d{2})(\d{2})(\d{2})(\d{2})(\d{3})", r"\\1.\\2.\\3.\\4.\\5.\\6", re.sub(r"\D", "", extracted_str[0]))')],
                r'\d{4}\.\d{2}\.\d{2}': [(r'\d{4}\.\d{2}', 'extracted_str[0] + ".00"')],
                r'\d{3}\.\d{3}\-\d{2}': [],
                r'[A-Z]{2}\s\d{3}\.\d{3}\.\d{4}': [],
                r'[A-Z]{2}\s\d{3}\.\d{4}': [],
                r'\d{2}\.\d{3}\.\d{4}': [],
               }
    
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            return ref_no_cleaned[0]
        for alt_pattern in patterns[pattern]:
            extracted_str = re.findall(alt_pattern[0], ref_no)
            if extracted_str:
                ref_no_cleaned = eval(alt_pattern[1])
                return ref_no_cleaned
        
    if return_blank:
        return ''
    else:
        return ref_no

def breitling(ref_no, return_blank=False):
    pattern = r'[A-Z0-9]{8}\d[A-Z]\d[A-Z]\d'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

def seiko(ref_no, return_blank=False):
    pattern = r'S[A-Z0-9]{5,7}'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

def longines(ref_no, return_blank=False):
    pattern = r'L\d\.\d{3}\.\d\.\d{2}\.[A-Z0-9]'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

def tag_heuer(ref_no, return_blank=False):
    pattern = r'[A-Z0-9]{7,8}\.[A-Z]{2}\d{4}'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

def patek_philippe(ref_no, return_blank=False):
    patterns = [r'\d{4}/\d{1,4}[a-zA-Z]{1,2}-\d{3}', r'\d{3,4}[a-zA-Z]{1,2}-\d{3}']
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            return ref_no_cleaned[0]
    if return_blank:
        return ''
    else:
        return ref_no
    

def audemars_piguet(ref_no, return_blank=False):
    pattern = r'\d{5}[A-Z]{2}\.[A-Z]{2}\.[A-Z0-9]+\.\d{2}(?:[.\-][A-Z])?'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

def hublot(ref_no, return_blank=False):
    pattern = r'\d{3}\.[A-Z]{2}\.[A-Z0-9]{3,4}\.[A-Z]{2}(?:\.[A-Z0-9]{3,5}(?:\.[A-Z0-9]{5})?)?'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

def tissot(ref_no, return_blank=False):
    patterns = [r'T\d{3}\.\d{3}\.\d{2}\.\d{3}\.\d{2}[A-Za-z]*', r'T\d{2}\.\d{1}\.\d{3}\.\d{2}']
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            return ref_no_cleaned[0]
    if return_blank:
        return ''
    else:
        return ref_no

def tudor(ref_no, return_blank=False):
    pattern = r'[Mm]?\d{4}[A-Z0-9/]{1,6}(?:\-\d{4})?'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no
    
def citizen(ref_no, return_blank=False):
    pattern = r'[A-Z]{2}\d{4}\-\d{2}[A-Z]'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no
    
def IWC(ref_no, return_blank=False):
    
    patterns = {r'IW\d{6}': [(r'IW\-?\d{4}\-?\d{2}', 'extracted_str[0].replace("-", "")')],
                r'IW\d{4}': [(r'IW\-?\d{4}', 'extracted_str[0].replace("-", "")')],
                r'\d{4}': [],
                r'Reference\s\d{3}': [(r'[Rr]ef\s\d{3}', 're.sub("[Rr]ef", "Reference", extracted_str[0])'), (r'reference\s\d{3}', 'extracted_str[0].capitalize()')],
               }
    
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            return ref_no_cleaned[0]
        for alt_pattern in patterns[pattern]:
            extracted_str = re.findall(alt_pattern[0], ref_no)
            if extracted_str:
                ref_no_cleaned = eval(alt_pattern[1])
                return ref_no_cleaned
        
    if return_blank:
        return ''
    else:
        return ref_no  
    
def cartier(ref_no, return_blank=False):
    pattern = r'[A-Z0-9]{4}\d{4}'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

def chopard(ref_no, return_blank=False):
    pattern = r'\d{2}[A-Z0-9]\d{3}\-\d{4}'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

def zenith(ref_no, return_blank=False):
    patterns = [r'\d{2}\.\w?\d{3,4}\.\d{3,4}\-\d{1}\/\w*\d{2,4}\.\w+\d{3,4}', r'\d{2}\.\w?\d{3,4}\.\d{3,4}\/\w*\d{2,4}\.\w+\d{3,4}']
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            return ref_no_cleaned[0]
    if return_blank:
        return ''
    else:
        return ref_no

def hamilton(ref_no, return_blank=False):
    pattern = r'H\d{8}'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no
    
def jaeger_lecoultre(ref_no, return_blank=False):
    pattern = r'Q[A-Z0-9]{7}'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

def baume_mercier(ref_no, return_blank=False):
    pattern = r'M0A\d{5}'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

def bvlgari(ref_no, return_blank=False):
    pattern = r'10\d{4}'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no
    
def rado(ref_no, return_blank=False):
    patterns = [r'R\d{8}', r'01\.763\.\d{4}\.3\.\d{3}']
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            return ref_no_cleaned[0]
    if return_blank:
        return ''
    else:
        return ref_no

def grand_seiko(ref_no, return_blank=False):
    pattern = r'[Ss][A-Za-z][Gg][A-Za-z]\d{3}[A-Z]?'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no    

def vacheron_constantin(ref_no, return_blank=False):
    pattern = r'[A-Za-z0-9]{5}\/[A-Za-z0-9]{4}[\-\s][A-Za-z0-9]{4}'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no
    
def blancpain(ref_no, return_blank=False):
    pattern = r'[A-Z0-9]{4,6}\s[A-Z0-9]{4,5}\s[A-Z0-9]{3,4}'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

def maurice_lacroix(ref_no, return_blank=False):
    pattern = r'[A-Z]{2}\d{4}-[A-Z0-9]{5}-[A-Z0-9]{3}-[A-Z0-9]{1}'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

def breguet(ref_no, return_blank=False):
    patterns = [r'[A-Z0-9]{9}\.\d{4}\/[A-Z0-9]{3}', 
                r'[A-Z0-9]{10,16}\/[A-Z0-9]{4,6}', 
                r'[A-Z0-9]{6}\/[A-Z0-9]{2}(?:\/[A-Z0-9]{3,5}(?:\/[A-Z0-9]{2,6})?)?']
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            return ref_no_cleaned[0]
    if return_blank:
        return ''
    else:
        return ref_no

def mido(ref_no, return_blank=False):
    pattern = r'M\d{3,4}\.\d{1,3}\.[A-Z0-9]{2}\.\d{1,3}(?:\.\d{2})?'
    try:
        return re.findall(pattern, ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no
    
def bulova(ref_no, return_blank=False):
    patterns = [r'\d{2}[A-Z]\d{2,3}[A-Z]{,2}', r'\d[A-Z]{2}\d[A-Z]\d{3}']
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            return ref_no_cleaned[0]
    if return_blank:
        return ''
    else:
        return ref_no
    
# This is a dictionary that returns a particular function based on the key (eg. "Omega" would be passed on to the omega function)
# This is because each brand has a different way of defining their reference numbers, so a different set of rules, as defined
# in the functions, would be needed depending on the brand passed
reference_cleaning_map = {
    'Nomos Glashütte': nomos_glashutte,
    'Meistersinger': meister_singer,
    'Rolex': rolex,
    'Omega': omega,
    'Breitling': breitling,
    'Seiko': seiko,
    'Longines': longines,
    'TAG Heuer': tag_heuer,
    'Patek Philippe': patek_philippe,
    'Audemars Piguet': audemars_piguet,
    'Hublot': hublot,
    'Tissot': tissot,
    'Tudor': tudor,
    'Citizen': citizen,
    'IWC': IWC,
    'Cartier': cartier,
    'Chopard': chopard,
    'Zenith': zenith,
    'Hamilton': hamilton,
    'Jaeger-LeCoultre': jaeger_lecoultre,
    'Baume & Mercier': baume_mercier,
    'Bvlgari': bvlgari,
    'Rado': rado,
    'Grand Seiko': grand_seiko,
    'Vacheron Constantin': vacheron_constantin,
    'Blancpain': blancpain,
    'Maurice Lacroix': maurice_lacroix,
    'Breguet': breguet,
    'Mido': mido,
    'Bulova': bulova,
}

def reference_number_function(reference_number, listing_title, brand):
    
    # If reference number already exists we return the raw reference number if we can't find the pattern we're looking for
    if reference_number:
        
        if brand in reference_cleaning_map:
            return reference_cleaning_map[brand](reference_number, False)
        else:
            return reference_number
        
    # If reference number doesn't exist, we try to extract it from the listing_title field. If there is no match, we return blank
    if brand in reference_cleaning_map:
        return reference_cleaning_map[brand](listing_title, True)
    else:
        return ''

# Clean group reference field
def group_reference_function(brand, reference_number):
    if brand.lower() in ['rolex', 'tudor']: # Rules are currently not complete yet as they only cover Rolex and Tudor
        try:
            group_reference = re.split(r'[-\s]', re.findall(r'\d.*', reference_number)[0])[0]
        except:
            group_reference = ''
    else:
        group_reference = ''
    return group_reference

# Clean listing_title and seller_name field to 255 characters
def cap_string_function(x):
    return(x[:252] + '...' if len(x)>255 else x)


def timestamp():
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M")
    return current_datetime

###############################
###### script run section###### 
###############################

def clean_up_combined(df, file_key):

    
    start_time = time.time()
    print(f'starting clean up.')
    print("")

    # parser = argparse.ArgumentParser()
    # parser.add_argument('--input_path')
    # parser.add_argument('--output_path')

    # args = parser.parse_args()

    # input_path = args.input_path
    # output_path = args.output_path
    store_name = file_key.split('/')[-1].split('_')[0].lower() # Get the source name from the input path

    # df = pd.read_csv(file_key)
    df = df.replace(r'\\u0026', '&', regex=True) # Replace "\u0026" with "&"

    # df['time_updated'] = pd.to_datetime(df['time_updated'])
    # df['time_updated'] = df['time_updated'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))

    # Convert all data (except timestamp) to string
    for column in df.columns[1:]:
        df[column] = df[column].astype(str).replace('nan', '')

    # df below all stand for the scrapped csv file before clean up

    ### parent_model_and_specific_model ###
    # All changes will take place in the original df columns, no return value needed
    # Need to create a 'parent_model_and_specific_model.csv' in same folder
    # col 0 is brand
    # col 1 is specific_model name
    # col 2 is parent_model name
    # col 3 is alternate values to capture for specific_model
    parent_model_and_specific_model(df)

    ### case_material ###
    df['case_material'] = df['case_material'].apply(lambda x: check_case_material(x, case_material_dict))

    ### dial_color ###
    df['dial_color'] = df['dial_color'].apply(lambda x: check_dial_color(x, dial_color_dict))

    ### condition ###
    df['condition'] = df['condition'].apply(lambda x: check_condition(x, condition_dict))
    # If none of the above apply from the condition field, use the listing_title field. If still none apply, clear the condition field data.
    df.loc[df['condition'].isna(), 'condition'] = df['listing_title'].apply(lambda x: check_condition(x, condition_dict))

    ### movement ###
    df['movement'] = df['movement'].apply(lambda x: check_movement(x, movement_dict))
    # If none of the above apply from the movement field, use the listing_title field. If still none apply, clear the movement field data.
    df.loc[df['movement'].isna(), 'movement'] = df['listing_title'].apply(lambda x: check_movement(x, movement_dict))

    ### contents ###
    # df['contents'] = df['contents'].apply(lambda x: check_contents(x, contents_dict))
    # df['contents'] = df['contents'].apply(lambda x: recheck_contents(x, contents_recheck_dict))
    # # If none of the above apply from the contents field, use the listing_title field. If still none apply, clear the movement field data.
    # df.loc[df['contents'].isna(), 'contents'] = df['listing_title'].apply(lambda x: check_contents_title(x, contents_dict))

    ### style ###
    df['style'] = df['style'].apply(lambda x: check_style(x, style_dict))

    ### numerals ###
    df['numerals'] = df['numerals'].apply(lambda x: check_numerals(x, numerals_dict))

    ### bracelet_material ###
    df['bracelet_material'] = df['bracelet_material'].apply(lambda x: check_bracelet_material(x, bracelet_material_dict))

    ### bracelet_color ###
    df['bracelet_color'] = df['bracelet_color'].apply(lambda x: check_bracelet_color(x, bracelet_color_dict))

    ### clasp_type ###
    # This would need to use 'bracelet_material' column after clear up, so run this after ### bracelet_material ###
    df['clasp_type'] = df['clasp_type'].apply(lambda x: check_clasp_type(x, clasp_type_dict))
    # If clasp_type is empty, look at bracelet_material field to see if any of the allowed values apply
    df.loc[df['clasp_type'].isna(), 'clasp_type'] = df['bracelet_material'].apply(lambda x: check_clasp_type(x, clasp_type_dict))

    ### features ###
    df['features'] = df['features'].apply(lambda x: check_features(x, features_dict))

    ### bezel_material ###
    df['bezel_material'] = df['bezel_material'].apply(lambda x: check_case_material(x, case_material_dict))

    ### bezel_color ###
    df['bezel_color'] = df['bezel_color'].apply(lambda x: check_dial_color(x, dial_color_dict))

    ### crystal ###
    df['crystal'] = df['crystal'].apply(lambda x: check_crystal(x, crystal_dict))

    ### case_shape ###
    df['case_shape'] = df['case_shape'].apply(lambda x: check_case_shape(x, case_shape_dict))

    # Currency
    df['currency'] = df['currency'].apply(currency_function)

    # Price
    df['price'] = df['price'].apply(price_function)

    # Jewels
    df['jewels'] = df['jewels'].apply(lambda x: jewels_function(x, store_name))

    # Power Reserve
    df['power_reserve'] = df['power_reserve'].apply(power_reserve_function)

    # Between Lugs
    df['between_lugs'] = df['between_lugs'].apply(lambda x: lugs_function(x, store_name))

    # Lug to Lug
    df['lug_to_lug'] = df['lug_to_lug'].apply(lambda x: lugs_function(x, store_name))
    
    # Case thickness
    df['case_thickness'] = df['case_thickness'].apply(lambda x: lugs_function(x, store_name))

    # Weight
    df['weight'] = df['weight'].apply(weight_function)

    # Year of Production
    df['year_of_production'] = df['year_of_production'].apply(year_of_production_function)

    # Decade of Production
    # df['decade_of_production'] = df.apply(lambda row: decade_of_production_function(row['decade_of_production'], row['year_of_production']), axis=1)

    # Water Resistance
    df['water_resistance'] = df['water_resistance'].apply(water_resistance_function)

    # Seller Type
    df['seller_type'] = df['seller_type'].apply(seller_type_function)

    # Listing Type
    df['listing_type'] = df['listing_type'].apply(listing_type_function)

    # Type
    df['type'] = df['type'].apply(type_function)

    # Country
    df['country'] = df['country'].apply(lambda x: country_function(x, store_name))

    # Seller Rating
    # df['seller_rating'] = df['seller_rating'].apply(seller_rating_function)

    # Diameter
    df['diameter'] = df['diameter'].apply(diameter_function)

    # Brand
    df['brand'] = df.apply(lambda row: brand_function(row['brand'], row['listing_title']), axis=1)

    # Authenticity Guarantee
    df['authenticity_guarantee'] = df.apply(lambda row: authenticity_guarantee_function(row['listing_title'], store_name), axis=1)
    
    # Reference Number
    df['reference_number'] = df.apply(lambda row: reference_number_function(row['reference_number'], row['listing_title'], row['brand']), axis=1)

    # Group Reference
    # df['group_reference'] = df.apply(lambda row: group_reference_function(row['brand'], row['reference_number']), axis=1)

    # Listing Title
    df['listing_title'] = df['listing_title'].apply(cap_string_function)

    # Caseback
    df['caseback'] = df['caseback'].apply(lambda x: check_caseback(x, caseback_dict))

    # # Seller Name
    # df['seller_name'] = df['seller_name'].apply(cap_string_function)

    # Brand Slug
    # df['brand_slug'] = df['brand'].apply(lambda x: slugify(x))

    # Converting "" to None type
    for column in df.columns[1:]:
        df[column] = df[column].replace('', np.nan)
        df[column] = df[column].infer_objects(copy=False)

    # Putting authenticity_guarantee column right beside brand column in dataframe
    columns = list(df.columns)
    columns.insert(list(df.columns).index('brand')+1, columns.pop(columns.index('authenticity_guarantee')))
    df = df[columns]

    # Converting cleaned numeric based values to numeric data type
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    # Convert the 'year_introduced' column to numeric, coercing errors to NaN
    df['year_of_production'] = pd.to_numeric(df['year_of_production'], errors='coerce')
    # Replace NaN and infinity values with None
    df['year_of_production'] = df['year_of_production'].replace([np.nan, np.inf, -np.inf], None)
    # Convert to integer type where possible
    df['year_of_production'] = df['year_of_production'].astype('Int64')

    
    # Convert the 'year_introduced' column to numeric, coercing errors to NaN
    df['year_introduced'] = pd.to_numeric(df['year_introduced'], errors='coerce')
    # Replace NaN and infinity values with None
    df['year_introduced'] = df['year_introduced'].replace([np.nan, np.inf, -np.inf], None)
    # Convert to integer type where possible
    df['year_introduced'] = df['year_introduced'].astype('Int64')

    df['jewels'] = pd.to_numeric(df['jewels'], errors='coerce')

    # Removing description column if it exists
    col = 'description'
    if col in df.columns:
        df = df.drop(columns=[col])

    
    print(f'Finished cleaning {store_name} at {timestamp()} ')
    print(f'It took {time.time() - start_time} seconds to clean data from {df.shape[0]} rows')


    return df

    # df.to_csv(output_path, index=False)
