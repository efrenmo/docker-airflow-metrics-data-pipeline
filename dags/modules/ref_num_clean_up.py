import re
import pandas as pd
import numpy as np 
from modules.utilities import setup_logging

logger = setup_logging(__name__)

# # Redirect stderr to capture error messages
# old_stderr = sys.stderr
# sys.stderr = StringIO()

# try:
#     import clean_up_combined
#     from clean_up_combined import *
# except SystemExit:
#     pass  # Ignore the SystemExit exception raised by argparse

# # Restore stderr
# sys.stderr = old_stderr




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
    
    def is_valid_year(year):
        try:
            year = int(year)
            return year < 1950 or year > 2025
        except ValueError:
            return True
    
#     return_blank = True
    
    patterns = {r'\d{5,6}[A-Z]{,4}\-\d{4}': [(r'\d{5,6}[A-Z]{,4}\s?[\-\/]?\s?\d{4}' , 're.sub(r"(\d{5,6}[A-Z]{,4})(\d{4})", r"\\1-\\2", re.sub(r"[^\dA-Z]", "", extracted_str[0]))')],
               r'\d{5,6}[A-Z]{,4}': [],
               r'\d{4}(?:\/\d|[A-Z])?': []}
    
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            if is_valid_year(ref_no_cleaned[0]):
                return ref_no_cleaned[0]
            else:
                return ref_no if not return_blank else ''
        for alt_pattern in patterns[pattern]:
            extracted_str = re.findall(alt_pattern[0], ref_no)
            if extracted_str:
                ref_no_cleaned = eval(alt_pattern[1])
                if is_valid_year(ref_no_cleaned):
                    return ref_no_cleaned
                else:
                    return ref_no if not return_blank else ''
        
    return ref_no if not return_blank else ''

def a_lange_sohne(ref_no, return_blank=False):
    
    # return_blank = True
    
    try:
        ref_no_extracted = re.findall(r'\d{3}[^A-Z0-9,]?\d{3}\s?[A-Z]{,3}(?![A-Z0-9])', ref_no)[0]
    except:
        return ref_no if not return_blank else ''
    
    ref_no_cleaned = re.sub(r'(\d{3})(\d{3})([A-Z]{,3})', r'\1.\2\3' ,re.sub(r'[^A-Z0-9]', '', ref_no_extracted))
    
    if ref_no_cleaned:
        return ref_no_cleaned
    else:
        return ref_no if not return_blank else ''

def omega(ref_no, return_blank=False):

    patterns = {r'(?<!\d)\d{3}\.\d{2}\.\d{2}\.\d{2}\.\d{2}\.\d{3}(?!\d)': [(r'(?<!\d)\d{3}[\s\-\.]?\d{2}[\s\-\.]?\d{2}[\s\-\.]?\d{2}[\s\-\.]?\d{2}[\s\-\.]?\d{3}(?!\d)', 're.sub(r"(\d{3})(\d{2})(\d{2})(\d{2})(\d{2})(\d{3})", r"\\1.\\2.\\3.\\4.\\5.\\6", re.sub(r"\D", "", extracted_str[0]))')],
                r'(?<!\d)\d{4}\.\d{2}\.\d{2}(?!\d)': [(r'(?<!\d)\d{4}[\s\-\.]?\d{2}[\s\-\.]?\d{2}(?!\d)', 're.sub(r"(\d{4})(\d{2})(\d{2})", r"\\1.\\2.\\3", re.sub(r"\D", "", extracted_str[0]))'), (r'(?<!\d)\d{4}[\s\-\.]?\d{2}(?!\d)', 're.sub(r"(\d{4})(\d{2})", r"\\1.\\2", re.sub(r"[^\d]", "", extracted_str[0])) + ".00"')],
                r'(?<!\d)\d{3}\.\d{3}\-\d{2}(?!\d)': [(r'(?<!\d)\d{3}[\s\-\.]\d{3}[\s\-\.]\d{2}(?!\d)', 're.sub(r"(\d{3})(\d{3})(\d{2})", r"\\1.\\2-\\3", re.sub(r"\D", "", extracted_str[0]))')],
                r'(?<![A-Z])[A-Z]{2}\s\d{3}\.\d{3}\.\d{4}(?!\d)': [(r'(?<![A-Z])[A-Z]{2}[\s\-\.]?\d{3}[\s\-\.]?\d{3}[\s\-\.]?\d{4}(?!\d)', 're.sub(r"([A-Z]{2})(\d{3})(\d{3})(\d{4})", r"\\1 \\2.\\3.\\4", re.sub(r"[^\dA-Z]", "", extracted_str[0]))')],
                r'(?<!\d)\d{2}\.\d{3}\.\d{4}(?!\d)': [(r'(?<!\d)\d{2}[\s\-\.]?\d{3}[\s\-\.]?\d{4}(?!\d)', 're.sub(r"(\d{2})(\d{3})(\d{4})", r"\\1.\\2.\\3", re.sub(r"\D", "", extracted_str[0]))')],
                r'(?<![A-Z])[A-Z]{2}\s\d{3}\.\d{4}\.\d{3}(?!\d)': [(r'(?<![A-Z])[A-Z]{2}[\s\-\.]?\d{3}[\s\-\.]?\d{4}[\s\-\.]?\d{3}(?!\d)', 're.sub(r"([A-Z]{2})(\d{3})(\d{4})(\d{3})", r"\\1 \\2.\\3.\\4", re.sub(r"[^\dA-Z]", "", extracted_str[0]))')],
                r'(?<![A-Z])[A-Z]{2}\s\d{3}\.\d{4}(?!\d)': [(r'(?<![A-Z])[A-Z]{2}[\s\-\.]?\d{3}[\s\-\.]?\d{4}(?!\d)', 're.sub(r"([A-Z]{2})(\d{3})(\d{4})", r"\\1 \\2.\\3", re.sub(r"[^\dA-Z]", "", extracted_str[0]))'), (r'(?<![A-Z])[A-Z]{2}[\s\-\.]?\d{3}[\s\-\.]?\d{3}(?!\d)', 're.sub(r"([A-Z]{2})(\d{3})(\d{3})", r"\\1 \\2.0\\3", re.sub(r"[^\dA-Z]", "", extracted_str[0]))')],
                r'(?<!\d)\d{3}\.\d{4}\.\d{3}(?!\d)': [(r'(?<!\d)\d{3}[\s\-\.]?\d{4}[\s\-\.]?\d{3}(?!\d)', 're.sub(r"(\d{3})(\d{4})(\d{3})", r"\\1.\\2.\\3", re.sub(r"\D", "", extracted_str[0]))')],
                r'(?<!\d)\d{3}\.\d{4}(?!\d)': [(r'(?<!\d)\d{3}[\s\/]?\d{4}(?!\d)', "re.sub(r'(\d{3})(\d{4})', r'\\1.\\2', re.sub(r'[^\d]', '', extracted_str[0]))"), (r'(?<!\d)\d{3}[\s\/\.]\d{3}(?!\d)', "re.sub(r'(\d{3})(\d{3})', r'\\1.0\\2', re.sub(r'[^\d]', '', extracted_str[0]))")],
                r'(?<![A-Z])[A-Z]{2}\s\d{4}\s[A-Z]{1,2}(?![A-Z])': [(r'(?<![A-Z])[A-Z]{2}[\s\-\.]?\d{4}[\s\-\.]?[A-Z]{1,2}(?![A-Z])', 're.sub(r"([A-Z]{2})(\d{4})([A-Z]{1,2})", r"\\1 \\2 \\3", re.sub(r"[^\dA-Z]", "", extracted_str[0]))')],
                r'(?<![A-Z])[A-Z]{1}\s\d{4}\s[A-Z]{1}(?![A-Z])': [(r'(?<![A-Z])[A-Z]{1}[\s\-\.]?\d{4}[\s\-\.]?[A-Z]{1}(?![A-Z])', 're.sub(r"([A-Z]{1})(\d{4})([A-Z]{1})", r"\\1 \\2 \\3", re.sub(r"[^\dA-Z]", "", extracted_str[0]))')],
                r'(?<![A-Z])[A-Z]{2}\s\d{3,5}(?!\d)': [(r'(?<![A-Z])[A-Z]{2}[\s\-\.]?\d{3,5}(?!\d)', 're.sub(r"([A-Z]{2})(\d{3,5})", r"\\1 \\2", re.sub(r"[^\dA-Z]", "", extracted_str[0]))')],
                r'(?<!\d)\d{5}(?!\d)': [(r'(?<!\d)\d{4}[\s\-\.]?\d(?!\d)', 're.sub(r"[^\d]", "", extracted_str[0])')],
                r'(?<!CAL)(?<!CAL.)(?<!CAL..)(?<!\d)\d{4}(?!\d)': [(r'(?<!CAL)(?<!CAL.)(?<!CAL..)(?<!\d)\d{3}[\s\-\.]?\d(?!\d)', 're.sub(r"[^\d]", "", extracted_str[0])')],
                r'(?<!CAL)(?<!CAL.)(?<!CAL..)(?<!\d)\d{3}(?!\d)': []
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

def montblanc(ref_no, return_blank=False):
    
    for x in range(6,3, -1):
        pattern = fr"(?:MB)?\d{{{x}}}"
        try:
            ref_no = re.findall(pattern, ref_no)[0]
            return ref_no.replace('MB', '')
        except:
            pass
        
    if return_blank:
        return ''
    else:
        return ref_no

def breitling(ref_no, return_blank=False):
    pattern = r'(?<![A-Z0-9])[A-Z0-9]{8}\d[A-Z]\d[A-Z]\d'
    try:
        return re.findall(pattern, re.sub(r'[^A-Z0-9]', '', ref_no))[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no

# def seiko(ref_no, return_blank=False):
#     pattern = r'S[A-Z0-9]{3}\d{2}[A-Z0-9]{,2}'
#     try:
#         return re.findall(pattern, ref_no)[0]
#     except:
#         if return_blank:
#             return ''
#         else:
#             return ref_no

def seiko(ref_no, return_blank=False):
    # Modern Seiko pattern
    modern_pattern = r'S[A-Z0-9]{3}\d{2}[A-Z0-9]{,2}'
    # Caliber-Case pattern: 4 chars + hyphen + 4 chars
    # vintage_pattern = r'[A-Z0-9]{4}-[A-Z0-9]{4}'
    vintage_pattern = r'(?=.*\d)[A-Z0-9]{4}-[A-Z0-9]{4}'
    
    try:
        # First try to find modern pattern
        modern_match = re.findall(modern_pattern, ref_no)
        if modern_match:
            return modern_match[0]
        
        # If no modern pattern, try vintage pattern
        vintage_match = re.findall(vintage_pattern, ref_no)
        if vintage_match:
            return vintage_match[0]
            
        # If neither pattern matches
        if return_blank:
            return ''
        else:
            return ref_no
    except:
        if return_blank:
            return ''
        else:
            return ref_no
            
def longines(ref_no, return_blank=False):

    patterns = {
        r'L\d\.\d{3}\.\d\.\d{2}\.[0-9A-Z]': [
            (r'L\d[^A-Z0-9,]?\d{3}[^A-Z0-9,]?\d[^A-Z0-9,]?\d{2}[^A-Z0-9,]?[0-9A-Z]',
             're.sub(r"(L\d)(\d{3})(\d)(\d{2})([0-9A-Z])", r"\\1.\\2.\\3.\\4.\\5", re.sub(r"[^A-Z0-9]", "", extracted_str[0]))')
        ],
        r'L\d\.\d{3}\.\d': []  # New pattern added
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

def tag_heuer(ref_no, return_blank=False):
    
    patterns = {r'[A-Z]{2}[A-Z0-9]{4,6}\.[A-Z0-9]{2}\d{4}': [(r'[A-Z]{2}[^A-Z0-9,]{,2}[A-Z0-9][^A-Z0-9,]{,2}[A-Z0-9]{3,5}[^A-Z0-9,]{,2}[A-Z0-9]{2}[^A-Z0-9,]{,2}\d{4}',
                                                  're.sub(r"([A-Z]{2}[A-Z0-9]{4,6})([A-Z0-9]{2}\d{4})", r"\\1.\\2", re.sub(r"[^A-Z0-9]", "", extracted_str[0])).strip(".")')],
                r'[A-Z]{2}[A-Z0-9]{4,6}': [(r'[A-Z]{2}[A-Z0-9][^A-Z0-9,]{,2}[A-Z0-9]{3,5}',
                                            're.sub(r"[^A-Z0-9]", "", extracted_str[0])')]
               }
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            if not re.findall(r'\d', ref_no_cleaned[0].split('.')[0]): # If there's no digits in the left part before the period, this isn't right
                return '' if return_blank else ref_no
            else:
                return ref_no_cleaned[0]
        for alt_pattern in patterns[pattern]:
            extracted_str = re.findall(alt_pattern[0], ref_no)
            if extracted_str:
                ref_no_cleaned = eval(alt_pattern[1])
                if not re.findall(r'\d', ref_no_cleaned.split('.')[0]): # If there's no digits in the left part before the period, this isn't right
                    return '' if return_blank else ref_no
                else:
                    return ref_no_cleaned
        
    if return_blank:
        return ''
    else:
        return ref_no


def patek_philippe(ref_no, return_blank=False):
    
    # return_blank = True
    
    # Extracting allowed and alternate reference numbers from the listing title
    try:
        ref_no_extracted = re.findall(r'\d{3,4}(?:[^A-Z0-9,]{1,4}\d{,4})?[A-Z]{1,2}[^A-Z0-9,]{,4}\d{3}(?:\s?[TL])?', ref_no)[0]
    except:
        if return_blank:
            return ''
        else:
            return ref_no
    
    # Converting alternate formats to allowed formats
    
    # Rule 1: Replace non-alphanumeric with "/" if there is no letter preceding it followed by a number
    ref_no_cleaned = re.sub(r'(?<![a-zA-Z])[^A-Z0-9,]{1,4}(?=\d)', '/', ref_no_extracted)

    # Rule 2: Replace non-alphanumeric with "-" if there is a letter preceding it followed by a number
    ref_no_cleaned = re.sub(r'(?<=[a-zA-Z])[^A-Z0-9,]{,4}(?=\d)', '-', ref_no_cleaned)

    # Rule 3: Add a space before the "T" if there's no space before it and "T" is the last character in the string
    ref_no_cleaned = re.sub(r'(?<! )T$', ' T', ref_no_cleaned)

    # Rule 4: Remove the space before the "L" if "L" is the last character in the string
    ref_no_cleaned = re.sub(r' L$', 'L', ref_no_cleaned)
    
    # Rule 5: Replace non-alphanumeric with "" if there is a number preceding it and a letter following it and it is not the second to last character
    ref_no_cleaned = re.sub(r'(?<=\d)[^A-Z0-9,]{1,4}(?=[a-zA-Z])(?!.$)', '', ref_no_cleaned)
    
    # If there is a match with allowed formats, return the reference number
    if ref_no_cleaned:
        return ref_no_cleaned
    else:
        if return_blank:
            return ''
        else:
            return ref_no

def audemars_piguet(ref_no, return_blank=False):
    
    patterns = {r'\d{5}[A-Z]{2}\.[A-Z]{1,2}\.[0-9A-Z]{4,6}\.[0-9A-Z]{2,5}(?:[\-\.\s][0-9A-Z]{1,2})?':[
        (r'\d{5}[A-Z]{2}[^A-Z0-9,]?[A-Z]{1,2}[^A-Z0-9,]?[0-9A-Z]{6}[^A-Z0-9,]?[0-9A-Z]{2,5}(?:[\-\.\s][0-9A-Z]{1,2})?',
         're.sub(r"(\d{5}[A-Z]{2})([A-Z]{1,2})([0-9A-Z]{4,6})([0-9A-Z]{2,5})((?:[\-\.\s][0-9A-Z]{1,2})?)", r"\\1.\\2.\\3.\\4\\5", re.sub(r"[^A-Z0-9]", "", extracted_str[0]))'
        )
    ]}

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
    patterns = [r'T\d{3}\.\d{3}\.\d{2}\.\d{3}\.\d{2}[A-Za-z]*', 'T\d{2}\.\d{1}\.\d{3}\.\d{2}']
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            return ref_no_cleaned[0]
    if return_blank:
        return ''
    else:
        return ref_no

def tudor(ref_no, return_blank=False):
    
    def is_valid_year(year):
        try:
            year = int(year)
            return year < 1950 or year > 2025
        except ValueError:
            return True
    
    patterns = {r'[MT]?\d{4,5}[A-Z0-9\/]{,6}(?:\-[A-Z0-9]{4,6})?': []}
    
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            if is_valid_year(ref_no_cleaned[0]):
                return ref_no_cleaned[0]
            else:
                return ref_no if not return_blank else ''
        for alt_pattern in patterns[pattern]:
            extracted_str = re.findall(alt_pattern[0], ref_no)
            if extracted_str:
                ref_no_cleaned = eval(alt_pattern[1])
                if is_valid_year(ref_no_cleaned):
                    return ref_no_cleaned
                else:
                    return ref_no if not return_blank else ''
        
    return ref_no if not return_blank else ''
    
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
    # Specific for wcc_df and not for pre_wcc_df   

    if re.match(r'^\d{4}$', str(ref_no)):
        return f'IW{ref_no}'

    patterns = {r'IW\d{6,7}': [(r'IW\-?\d{4}\-?\d{2,3}', 'extracted_str[0].replace("-", "")')],
                r'IW\d{4}': [(r'IW\-?\d{4}', 'extracted_str[0].replace("-", "")')],
                r'\d{6}': [],
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
    patterns = [r'\d{2}\.\w?\d{3,4}\.\d{3,4}\-\d{1}\/\w*\d{2,4}\.\w+\d{3,4}', '\d{2}\.\w?\d{3,4}\.\d{3,4}\/\w*\d{2,4}\.\w+\d{3,4}']
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
    
    ref_no = ref_no.upper()
    
    patterns = {
        '\d{7}[A-Z]?': [],
        'VE\d{3}[A-Z]{2}': [],
        'VE\d{5}': [],
        '\d{6}[A-Z]': [],
        'V?\d{6}': [],
        '\d{5}[A-Z]\d': [],
    }

    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            return ref_no_cleaned[0]
        
    if return_blank:
        return ''
    else:
        return ref_no

def frederique_constant(ref_no, return_blank=False):
    
    patterns = {r'FC[\-]\d{3}[A-Z0-9]{4,10}(?:\-[A-Z]{2})?':[(r'FC[\-\s\.]?\d{3}[A-Z0-9]{4,10}(?:[\-\s\.]?[A-Z]{2})?',
                                                           're.sub(r"(FC)(\d{3})([A-Z0-9]{4,10})([A-Z]{,2})", r"\\1-\\2\\3-\\4", re.sub(r"[^A-Z0-9]", "", extracted_str[0])).strip("-")')]}
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

def oris(ref_no, return_blank=False):
    
    ref_no = ref_no.upper()
    
    patterns = {
        # Newly added based on library table
                r'01\s\d{3}\s\d{4}\s\d{4}\-SET\d?\s(?:\d\s)?\d{2}\s?\d{2,3}[A-Z]{,4}': 
        [(r'01[^A-Z0-9]?\d{3}[^A-Z0-9]?\d{4}[^A-Z0-9]?\d{4}\-SET\d?[^A-Z0-9]?(?:\d[^A-Z0-9]?)?\d{2}[^A-Z0-9]?\d{2,3}[A-Z]{,4}',
        're.sub(r"(01)(\d{3})(\d{4})(\d{4})(SET)(\d)(\d{2})(\d{2,3}[A-Z]{,4})", r"\\1 \\2 \\3 \\4-\\5 \\6 \\7 \\8", re.sub(r"[^\dA-Z]", "", extracted_str[0]))')],
        
                r'01\s\d{3}\s\d{4}\s\d{4}\-SET\d?\s(?:LS|MB|RS|TS)(?:\s[A-Z]{,5})?': 
        [(r'01[^A-Z0-9]?\d{3}[^A-Z0-9]?\d{4}[^A-Z0-9]?\d{4}[^A-Z0-9]?SET\d?[\s\-](?:LS|MB|RS|TS)[^A-Z0-9]?(?:\s[A-Z]{,5})?',
                're.sub(r"(01)(\d{3})(\d{4})(\d{4})(SET\d?)((?:LS|MB|RS|TS))([A-Z]{,5})", r"\\1 \\2 \\3 \\4-\\5 \\6\\7", re.sub(r"[^\dA-Z]", "", extracted_str[0]))')],
                
        r'01\s\d{3}\s\d{4}\s\d{4}\-\d{2}\s\d{1,2}\s\d{1,2}\s\d{,2}\s?[A-Z0-9]{,4}(?:\sX|\-\d)?': 
        [(r'01[^A-Z0-9]?\d{3}[^A-Z0-9]?\d{4}[^A-Z0-9]?\d{4}[^A-Z0-9,]{,3}\d{2}[^A-Z0-9]?\d{1,2}[^A-Z0-9]?\d{1,2}[^A-Z0-9]?\d{,2}[^A-Z0-9]?[A-Z0-9]{,4}(?:\sX|\-\d)?',
         're.sub(r"(01)(\d{3})(\d{4})(\d{4})(\d{2})(\d{1})(\d{2})(\d{,2}[A-Z0-9]{,4}(?:\sX|\-\d)?)", r"\\1 \\2 \\3 \\4-\\5 \\6 \\7 \\8", re.sub(r"[^\dA-Z]", "", extracted_str[0]))')],
        
                r'01\s\d{3}\s\d{4}\s\d{4}\-\d{1,2}[\s\.]\d{2,3}\s?\d{2}[A-Z]{,4}': 
        [],
        
                r'01\s\d{3}\s\d{4}\s\d{4}\s(?:LS|MB)': 
        [(r'01[^A-Z0-9]?\d{3}[^A-Z0-9]?\d{4}[^A-Z0-9]?\d{4}[^A-Z0-9]?(?:LS|MB)', 
          're.sub(r"(01)(\d{3})(\d{4})(\d{4})((?:LS|MB))", r"\\1 \\2 \\3 \\4 \\5", re.sub(r"[^\dA-Z]", "", extracted_str[0]))')],
        
        
        
        
        # Existing ones based on WCC rules
                r'01\s\d{3}\s\d{4}\s\d{4}\-SET\d?\s?(?:LS|MB|RS|TS)?': [
                    (r'01[^A-Z0-9]?\d{3}[^A-Z0-9]?\d{4}[^A-Z0-9]?\d{4}[^A-Z0-9]?SET\d?[^A-Z0-9]?(?:LS|MB|RS|TS)?',
                    're.sub(r"(01)(\d{3})(\d{4})(\d{4})(SET\d?)((?:LS|MB|RS|TS)?)", r"\\1 \\2 \\3 \\4-\\5 \\6", re.sub(r"[^\dA-Z]", "", extracted_str[0]))'),
                    (r'(?:01\s)?\d{3}\s\d{4}\s\d{4}\s?\-\s?SET\d?\s?(?:LS|MB|RS|TS)?', '("01 "+extracted_str[0].replace(" - ", "-")).replace("01 01", "01")')],
                
                r'01\s\d{3}\s\d{4}\s\d{4}\-\d{2}\s\d\s\d{2}\s\d{2}(?:PEB|EB)': [(r'(?:01\s)?\d{3}\s\d{4}\s\d{4}\s\d{2}\s\d\s\d{2}\s\d{2}(?:PEB|EB)', '("01 "+re.sub((r"(\\d{4}) (\\d{2} \\d{1})"), r"\\1-\\2", extracted_str[0])).replace("01 01", "01")'),
                                                                               (r'(?:01\s)?\d{3}\s\d{4}\s\d{4}\s?[\–\-]\s?\d{2}\s\d\s\d{2}\s\d{2}(?:PEB|EB)', '("01 "+re.sub(r"\s?[\–\-]\s?", "-", extracted_str[0])).replace("01 01", "01")')],
                r'01\s\d{3}\s\d{4}\s\d{4}\s(?:OSC|HB|LS|RS|TS)(?:\-[A-Z]{3}\-SET\d?)?': [(r'(?:01\s)?\d{3}\s\d{4}\s\d{4}\s?[\–\-]\s?(?:OSC|HB|LS|RS|TS)(?:\-[A-Z]{3}\-SET\d?)?', '("01 "+re.sub(r"(\d)\s?[\–\-]\s?(?=[a-zA-Z])", r"\\1 ", extracted_str[0])).replace("01 01", "01")')],
                r'01\s\d{3}\s\d{4}\s\d{4}': [(r'0?1\d{11}', "f'01 {extracted_str[0][-11:][:3]} {extracted_str[0][-11:][3:7]} {extracted_str[0][-11:][7:]}'")],
               }

    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            return ref_no_cleaned[0].strip()
        for alt_pattern in patterns[pattern]:
            extracted_str = re.findall(alt_pattern[0], ref_no)
            if extracted_str:
                ref_no_cleaned = eval(alt_pattern[1])
                return ref_no_cleaned.strip()

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
    
    patterns = {r'\d{4}[A-Z0-9]\/[A-Z0-9]{2}\d[A-Z][\-][A-Z0-9]\d{3}': [
        (r'\d{4}[A-Z0-9][^A-Z0-9,]?[A-Z0-9]{2}\d[A-Z][^A-Z0-9,]?[A-Z0-9]\d{3}',
        're.sub(r"(\d{4}[A-Z0-9])([A-Z0-9]{2}\d[A-Z])([A-Z0-9]\d{3})", r"\\1/\\2-\\3", re.sub(r"[^A-Z0-9]", "", extracted_str[0]))'
        )
    ]
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
    
    try:
        ref_no = re.findall('[A-Z]{2}[\.\s\/\-]?\d{4}[\.\s\/\-]?[A-Z0-9]{5}[\.\s\/\-]?[A-Z0-9]{3}[\.\/\-]?[A-Z0-9]{,4}', ref_no)[0]
        ref_no = re.sub('([\dA-Z]{6})([\dA-Z]{5})([\dA-Z]{3})([\dA-Z]{,4})', '\\1-\\2-\\3-\\4', re.sub(r'[^\dA-Z]', '', ref_no)).strip('-')
        return ref_no
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

# def panerai(ref_no, return_blank=False):
    
#     patterns = {r'(?:FER|PAM)\d{4}[A-Z0-9]{1,2}': [(r'(?:FER|PAM)[^A-Z0-9,]?\d{4}[A-Z0-9]{1,2}', 
#                                                     're.sub(r"[^A-Z0-9]", "", extracted_str[0])')],
#             r'\d{4}\-\d{3}(?:\/A)': [(r'\d{4}[^A-Z0-9,]?\d{3}[^A-Z0-9,]?A',
#                                       're.sub(r"(\d{4})(\d{3})(A)", r"\\1-\\2/\\3", re.sub(r"[^A-Z0-9]", "", extracted_str[0]))')],
#             r'\d{4}\-\d{3}': [(r'\d{4}[^A-Z0-9,]?\d{3}',
#                                're.sub(r"(\d{4})(\d{3})", r"\\1-\\2", re.sub(r"[^A-Z0-9]", "", extracted_str[0]))')],
#            }
    
#     for pattern in patterns:
#         ref_no_cleaned = re.findall(pattern, ref_no)
#         if ref_no_cleaned:
#             return ref_no_cleaned[0]
#         for alt_pattern in patterns[pattern]:
#             extracted_str = re.findall(alt_pattern[0], ref_no)
#             if extracted_str:
#                 ref_no_cleaned = eval(alt_pattern[1])
#                 return ref_no_cleaned
        
#     if return_blank:
#         return ''
#     else:
#         return ref_no

def fix_panerai_ref_num(ref_num):
    """
    To be used within the panerai() function that is defined below. Can be used independently as well, 
    but make sure is working with panerai reference numbers only.  
    The vast majority of Panerai reference numbers consits of the prefix PAM or FER, followed by a 
    5 digits number sequence. If is not 5 digits, the function will add zeros at the start of the numeric 
    sequence until it becomes 5 digits. Spaces, if any, between the prefix and the numeric will be removed. 
    """
    prefixes = ['PAM', 'FER']
    for prefix in prefixes:
        if ref_num.upper().startswith(prefix):
            # Remove prefix and any following spaces
            numeric_part = ref_num[len(prefix):].lstrip()
            # Remove any remaining spaces
            # numeric_part = numeric_part.replace(' ', '')
            if numeric_part.isdigit():  # Check if it is numeric
                # Format numeric part to be 5 digits
                formatted_numeric = numeric_part.zfill(5)
                return prefix + formatted_numeric
    return ref_num  # Return original if it does not match any prefix


def panerai(ref_no, return_blank=False):
    
    patterns = {r'(?:FER|PAM)\d{2,5}[A-Z0-9]?': [(r'(?:FER|PAM)[^A-Z0-9,]?\d{2,5}[A-Z0-9]?', 
                                                    're.sub(r"[^A-Z0-9]", "", extracted_str[0])')],
            r'\d{4}\-\d{3}(?:\/A)': [(r'\d{4}[^A-Z0-9,]?\d{3}[^A-Z0-9,]?A',
                                      're.sub(r"(\d{4})(\d{3})(A)", r"\\1-\\2/\\3", re.sub(r"[^A-Z0-9]", "", extracted_str[0]))')],
            r'\d{4}\-\d{3}': [(r'\d{4}[^A-Z0-9,]?\d{3}',
                               're.sub(r"(\d{4})(\d{3})", r"\\1-\\2", re.sub(r"[^A-Z0-9]", "", extracted_str[0]))')],
           }
    
    for pattern in patterns:
        ref_no_cleaned = re.findall(pattern, ref_no)
        if ref_no_cleaned:
            cleaned_value = ref_no_cleaned[0]
            # Only apply fix_panerai_ref_num if it's a PAM/FER pattern
            if pattern.startswith(r'(?:FER|PAM)'):
                return fix_panerai_ref_num(cleaned_value)
            return cleaned_value
            
        for alt_pattern in patterns[pattern]:
            extracted_str = re.findall(alt_pattern[0], ref_no)
            if extracted_str:
                cleaned_value = eval(alt_pattern[1])
                # Only apply fix_panerai_ref_num if it's a PAM/FER pattern
                if pattern.startswith(r'(?:FER|PAM)'):
                    return fix_panerai_ref_num(cleaned_value)
                return cleaned_value
    
    # Check if value is exactly 5 digits. If it does, add the PAM prefix to the value.
    if ref_no.isdigit() and len(ref_no) == 5:
        return 'PAM' + ref_no
    # Return blank or original if no pattern is found
    if return_blank:
        return ''
    else:
        return ref_no
        

try:
    engine = db.create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    connection = engine.connect()
    rec_accepted_ref_no = [x[0] for x in connection.execute(f"SELECT distinct reference_number from library_rec order by reference_number").fetchall()]
    connection.close()
except:
    rec_accepted_ref_no = []

finally:
    # Ensure the connection is closed
    try:
        connection.close()
    except:
        pass  # Ignore errors when closing the connection

def rec_watches(ref_no, return_blank=False):
    
    for arn in rec_accepted_ref_no:
        try:
            if re.sub(r'[^A-Z0-9]', '', arn.upper()) in re.sub(r'[^A-Z0-9]', '', ref_no.upper()):
                return arn
        except:
            pass
        
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
    'A. Lange & Söhne': a_lange_sohne,
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
    'Oris': oris,
    'Montblanc': montblanc,
    'Frederique Constant': frederique_constant,
    'Panerai': panerai,
    'REC Watches': rec_watches,
}

def reference_number_function(reference_number, listing_title, brand):
    
    # If reference number already exists we return the raw reference number if we can't find the pattern we're looking for
    if reference_number:
        
        if brand in reference_cleaning_map:
            reference_number_cleaned = reference_cleaning_map[brand](str(reference_number).upper(), True)
            # If there is a reference number match
            if reference_number_cleaned:
                return reference_number_cleaned
            # Otherwise, if there is no match in the reference number field, we try to extract it from listing title
            else:
                reference_number_cleaned = reference_cleaning_map[brand](str(listing_title).upper(), True)
                if reference_number_cleaned:
                    return reference_number_cleaned
                else:
                    return reference_number
        else:
            return reference_number
        
    # If reference number doesn't exist, we try to extract it from the listing_title field. If there is no match, we return blank
    if brand in reference_cleaning_map:
        return reference_cleaning_map[brand](str(listing_title).upper(), True)
    else:
        return ''

def reference_number_fx(df, brand_col='brand', ref_col='reference_number', title_col=None):
    """
    Clean and standardize reference numbers in a dataframe based on brand-specific patterns.

    This function applies brand-specific cleaning functions to reference numbers in a dataframe.
    It attempts to clean the reference number first, and if unsuccessful or if the reference number
    is missing, it tries to extract a valid reference number from the listing title (if provided).

    Parameters:
    -----------
    df : pandas.DataFrame
        The input dataframe containing the data to be cleaned.
    brand_col : str, optional
        The name of the column containing brand information (default is 'brand').
    ref_col : str, optional
        The name of the column containing reference numbers (default is 'reference_number').
    title_col : str or None, optional
        The name of the column containing listing titles. If None, listing titles won't be used (default is None).

    Returns:
    --------
    pandas.Series
        A series containing the cleaned reference numbers.

    Prints:
    -------
    The number of reference numbers that were cleaned or changed.

    Example:
    --------
    # Assuming 'df' is your dataframe with columns 'brand', 'reference_number', and 'listing_title'
    df['cleaned_reference'] = reference_number_fx(df, brand_col='brand', ref_col='reference_number', title_col='listing_title')

    Notes:
    ------
    - The function uses a predefined dictionary 'reference_cleaning_map' that maps brands to their respective cleaning functions.
    - If a brand is not found in the cleaning map, the original reference number is returned.
    - The function handles NaN values by converting them to strings before processing.
    """
    # Variables tha keep track of how many reference_numbers and listing titles are cleaned
    cleaned_count_from_title_col = 0
    cleaned_count_from_ref_col = 0 

    def apply_cleaning(row):
        nonlocal cleaned_count_from_title_col
        nonlocal cleaned_count_from_ref_col
        brand = row[brand_col]
        ref_num = row[ref_col]
        title = row[title_col] if title_col else ''
        
        if brand in reference_cleaning_map:
            clean_func = reference_cleaning_map[brand]
            
            # If reference number exists, try cleaning it
            if pd.notna(ref_num):
                cleaned = clean_func(str(ref_num).upper(), False)
                if cleaned != ref_num:
                    cleaned_count_from_ref_col += 1
                    return cleaned
                
                # If pattern not found in reference number, try listing title
                elif title_col and pd.notna(title):
                    cleaned_title = clean_func(str(title).upper(), True)
                    if cleaned_title:
                        cleaned_count_from_title_col += 1
                        return cleaned_title
                
                # If no pattern found in either, return original reference number
                return ref_num
            
            # If reference number is empty, try listing title
            elif title_col and pd.notna(title):
                cleaned = clean_func(str(title).upper(), True)
                if cleaned:
                    cleaned_count_from_title_col += 1
                    return cleaned
            
            # If reference number was empty and no pattern found in title, return empty
            return ref_num
        else:
            return ref_num
    
    result = df.apply(apply_cleaning, axis=1)
    
    print(f"Number of reference numbers cleaned from ref_num col: {cleaned_count_from_ref_col}")
    print(f"Number of reference numbers cleaned from title col: {cleaned_count_from_title_col}")
    return result


def group_reference_fx(df):
    """
    Generate group references for watch brands based on their reference numbers.

    This function processes a DataFrame containing watch brand and reference number information
    to create standardized group references. It uses vectorized operations for improved performance
    on large datasets.

    Currently supports specific rules for the following brands:
    - Rolex and Tudor: Extracts the first group of digits
    - Oris: Extracts the pattern '01 XXX XXXX XXXX'
    - Patek Philippe and Maurice Lacroix: Takes the part before the first hyphen

    For all other brands, it returns an empty string.

    Parameters:
    -----------
    df : pandas.DataFrame
        Input DataFrame containing at least two columns:
        - 'brand': The watch brand name
        - 'reference_number': The watch reference number

    Returns:
    --------
    pandas.Series
        A Series containing the generated group references, with the same index as the input DataFrame.

    Example:
    --------
    # Assuming 'df' is your DataFrame with 'brand' and 'reference_number' columns
    df['group_reference'] = group_reference_fx(df)

    Notes:
    ------
    - The function is case-insensitive for brand names.
    - For unsupported brands, an empty string is returned as the group reference.
    - This function does not modify the original DataFrame.
    """
    # Create a copy of the dataframe to avoid modifying the original
    df = df.copy()

    # Check if 'group_reference' column exists, if not, initialize it
    if 'group_reference' not in df.columns:
        df['group_reference'] = pd.NA

    # Convert brand to lowercase
    df['brand_lower'] = df['brand'].str.lower()

    # Initialize group_reference series
    group_reference = pd.Series('', index=df.index)
    
    # Rolex and Tudor
    rolex_tudor_mask = df['brand_lower'].isin(['rolex', 'tudor'])
    group_reference[rolex_tudor_mask] = df.loc[rolex_tudor_mask, 'reference_number'].str.extract(r'(\d+)', expand=False)
    
    # Oris
    oris_mask = df['brand_lower'] == 'oris'
    group_reference[oris_mask] = df.loc[oris_mask, 'reference_number'].str.extract(r'(01\s\d{3}\s\d{4}\s\d{4})', expand=False)
    
    # Maurice Lacroix
    ml_mask = df['brand_lower'] == 'maurice lacroix'
    group_reference[ml_mask] = df.loc[ml_mask, 'reference_number'].str.split('-').str[0]
   
    # Patek Philippe 
    # This pattern will extract the first 3 or 4 digits from the start of the reference number.
    # According to the Patek Philippe library, group reference numbers contain no alphabetical characters.
    # The number is a 4 numerical digits long, in rare ocassions is only 3, which is signaled by the appearence of the first 
    # alphabetical character right after it
    pp_mask = df['brand_lower'] == 'patek philippe'
    pp_refs = df.loc[pp_mask, 'reference_number']
    pp_extracted = pp_refs.str.extract(r'^(\d{3,4})', expand=False)  # Extract first 3 or 4 digits from the start
    group_reference[pp_mask] = pp_extracted

    return group_reference


# Clean group reference field
def group_reference_function(brand, reference_number):
    if not isinstance(reference_number, str):
        # logger.info(f"Warning: reference_number '{reference_number}' is not a string type")
        return ''
        
    if brand.lower() == 'longines':
        pattern1 = r'(L\d\.\d{3}\.\d)\.\d{2}\.[0-9A-Z]'
        pattern2 = r'L\d\.\d{3}\.\d'
        
        # Check first pattern
        match = re.match(pattern1, reference_number)
        if match:
            return match.group(1)
            
        # Check second pattern
        if re.match(pattern2, reference_number):
            return reference_number
            
        return ''
    
    elif brand.lower() in ['rolex', 'tudor']: # Rules are currently not complete yet as they only cover Rolex and Tudor
        try:
            group_reference = re.split(r'[-\s]', re.findall('\d.*', reference_number)[0])[0]
        except:
            group_reference = ''
    elif brand.lower() in ['oris']:
        try:
            group_reference = re.findall(r'01\s\d{3}\s\d{4}\s\d{4}', reference_number)[0]
        except:
            group_reference = ''
    elif brand.lower() in ['maurice lacroix']:
        try:
            group_reference = reference_number.split('-')[0]
        except:
            group_reference = ''
    elif brand.lower() in ['patek philippe']:
        try:
            group_reference = re.findall(r'^\d{3,4}', reference_number)[0]
        except:
            group_reference = ''
    elif brand.lower() in ['omega']:
        try:
            group_reference = re.findall(r'\d{3}\.\d{4}', reference_number)[0]
        except:
            group_reference = ''
    elif brand.lower() in ['audemars piguet']:
        try:
            group_reference = re.findall(r'\d{,5}', reference_number.split('.')[0])[0]
        except:
            group_reference = ''
    elif brand.lower() in ['tag heuer']:
        try:
            group_reference = reference_number.split('.')[0]
        except:
            group_reference = ''
    elif brand.lower() in ['a. lange & söhne']:
        try:
            group_reference = re.findall('\d{3}\.\d{3}', reference_number)[0]
        except:
            group_reference = ''
    else:
        group_reference = ''
    return group_reference


def modify_omega_ref_if_in_lib(df):
    """
    Modify Omega watch reference numbers in the DataFrame if they match a specific pattern
    and exist in the Omega library with an additional zero after the decimal point.

    This function performs the following operations:
    1. Identifies Omega watches with reference numbers matching the pattern: 
       three digits, a dot, three digits (optionally with a letter prefix).
    2. For matching reference numbers, it attempts to modify them by adding a zero after the dot.
    3. If the modified reference exists in the Omega library, it updates both the 'reference_number'
       and 'group_reference' columns with the modified value.
    4. Counts and reports on the number of modifications made.

    Parameters:
    -----------
    df : pandas.DataFrame
        The input DataFrame containing watch data. Must have columns:
        'brand', 'reference_number', and 'group_reference'.

    Returns:
    --------
    pandas.DataFrame
        The modified DataFrame with updated reference numbers and group references.

    Global Variables Used:
    ----------------------
    omega_library : pandas.DataFrame
        A DataFrame containing the Omega library reference numbers.
        Must have a 'group_reference' column.

    Notes:
    ------
    - The function only modifies Omega brand watches.
    - Reference numbers already containing four digits after the dot are not modified.
    - The function preserves any letter prefixes in the reference numbers (e.g., 'ST 145.022').
    - It prints statistics about the number of rows affected and the percentage of modifications.

    Example:
    --------
    >>> df = pd.DataFrame({
    ...     'brand': ['Omega', 'Omega', 'Rolex'],
    ...     'reference_number': ['145.022', 'ST 166.032', '116610'],
    ...     'group_reference': [None, None, None]
    ... })
    >>> modified_df = modify_omega_ref_if_in_lib(df)
    The mask is True for 2 out of 3 rows (66.67%).
    Number of rows affected: 2
    Percentage of rows affected: 100.00%
    """
    pattern = r'(?i)^(?:[A-Z]+\s+)?(\d{3}\.(?!0{3})\d{3})(?!\d)'
    mask = (df['brand'] == 'Omega') & (df['reference_number'].str.match(pattern))
    changed_count = 0
    changed_count = 0
    mask_true_count = mask.sum()
    total_rows = len(df)
    percentage = (mask_true_count / total_rows) * 100

    def modify_and_check(ref_num):
        # Extract the numeric part if there's a prefix
        # match = re.search(r'(?i)(?:[A-Z]+\s+)?(\d{3}\.\d{3})', ref_num)
        match = re.search(r'(?i)^(?:[A-Z]+\s+)?(\d{3}\.(?!0{3})\d{3})(?!\d)', ref_num)
        if match:
            numeric_part = match.group(1)
            modified_numeric = re.sub(r'\.', '.0', numeric_part, count=1)        
            if modified_numeric in omega_library['group_reference'].values:
                modified_ref = ref_num.replace(numeric_part, modified_numeric)
                return modified_ref, True
        return ref_num, False

    def modify_single_ref(ref_num):
        nonlocal changed_count
        modified_ref, was_changed = modify_and_check(ref_num)
        if was_changed:
            changed_count += 1
        return modified_ref

    df.loc[mask, 'reference_number'] = df.loc[mask, 'reference_number'].apply(modify_single_ref)    
    # Copy modified reference_number to group_reference
    df.loc[mask, 'group_reference'] = df.loc[mask, 'reference_number']


    print(f"The mask is True for {mask_true_count} out of {total_rows} rows ({percentage:.2f}%).")
    print(f'Number of rows affected: {changed_count}')
    print(f"Percentage of rows affected: {(changed_count / mask_true_count) * 100:.2f}%")

    return df