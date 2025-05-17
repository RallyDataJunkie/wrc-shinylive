from pandas import isna
import string
import re
from .rules_processor import nth, Nth

# Chars for encoding - we can do fifty cars safely
# The Z char is for nan
safe_chars = (
    string.ascii_lowercase  # 26
    + string.ascii_uppercase  # 26
    #+ string.digits  # 10 — unused here, extra room
)

# Build encoding and decoding maps
position_encode_map = {i: safe_chars[i] for i in range(51)}
position_decode_map = {
    v: k for k, v in position_encode_map.items()
}  # Optional for reverse


def encode_position_symbols(wide_df, cols, inplace=False):
    def position_encoder(row, cols):
        encoded = []
        for col in cols:
            val = row[col]
            if isna(val):
                encoded.append("Z")
            else:
                # Reindex position relative to 0
                encoded.append(position_encode_map[int(val)-1])
        return "".join(encoded)

    if not inplace:
        wide_df = wide_df.copy()

    wide_df["encoded"] = wide_df.apply(lambda row: position_encoder(row, cols), axis=1)

    return wide_df


## Remarks


def split_position_related_remarks(row):
    s=row["encoded"]
    if len(s)==1:
        return ""
    
    # Lead at every split
    match = re.fullmatch(r"^(a+)$", s)
    if len(s) and match:
        print("Lead at every split")
        return f"{row['driverName']} led at every split point and took the stage win."

    # 
    # Lost position
    match = re.fullmatch(r"^([ab]+)([^abc]+)$", s)
    if match:
        print("Lost position")
        lead_len = len(match.group(1))
        return f"{row['driverName']} started the stage well, but fell back at split {lead_len+1} and finally finished in {Nth(position_decode_map[s[-1]])}."
    
    # Started well, fell back, retook lead
    match = re.fullmatch(r"^([a]+)([^a]+)(a+)$", s)
    if match:
        print("Started well, fell back, retook lead")
        return f"{row['driverName']} started strongly, slipped back during the stage, then improved position to take the stage win."
    
    # Started poorly, finished well
    match = re.fullmatch(r"^([^abc]+)([abc]+)$", s)
    if match:
        print("Started poorly, finished well", s)
        return f"{row['driverName']} started poorly ({Nth(position_decode_map[s[0]])} at the first split) but improved position to finish in {Nth(position_decode_map[s[-1]])}."
    
    # Led first half-ish
    match = re.fullmatch(r"^([a]+)([^a]+)$", s)
    if match:
        print("Led first half-ish")
        lead_len = len(match.group(1))
        fall_back_len = len(match.group(2))

        if lead_len >= len(s)/2:
            return (
            f"{row['driverName']} led the split times over the first {" " if lead_len==1 else str(lead_len)+" "}split{'s' if lead_len != 1 else ''}, "
            f"then fell back over the last {fall_back_len} split section{'s' if fall_back_len != 1 else ''}."
            )
    
    # Lost podium
    match = re.fullmatch(r"^([abc]+)([^abc]+)$", s)
    if match:
        return f"{row['driverName']} was in a podium position for the first {p.number_to_words(len(match.group(1)))} splits, but then fell back to {Nth(position_decode_map[s[-1]])} by stage end."

    # Trailed then took lead
    match = re.fullmatch(r"^([^a]+)(a+)$", s)
    if match:
        print("Trailed then took lead")
        until_ = "the last split section, taking the stage lead, and the stage win, right at the end of the stage" if len(match.group(2))==1 else f"the {Nth(match.group(1)+1)}, but then continued in first position at each split as well as the stage win."
        return f"{row['driverName']} trailed in the splits until {until_}."

    print("no match",s )
    return ""


## TO DO the following is not used currently

# Via claude.ai
def bin_label(value, min_val, max_val, num_bins):
    """
    Assigns a label (A, B, C, etc.) based on which bin a value falls into within a range.

    Parameters:
    value: numeric (the value to be binned)
    min_val: numeric (the minimum value of the range)
    max_val: numeric (the maximum value of the range)
    num_bins: int (number of bins to divide the range into)

    Returns:
    str: A letter label corresponding to the appropriate bin
    """
    # Handle NaN values first, before any calculations
    if isna(value) or isna(min_val) or isna(max_val) or isna(num_bins):
        return "X"

    try:
        value = float(value)
        min_val = float(min_val)
        max_val = float(max_val)
        num_bins = int(num_bins)
    except (ValueError, TypeError):
        return "X"

    # Check if range is valid
    if max_val <= min_val:
        return "X"

    total_range = max_val - min_val
    if total_range == 0:
        return "A"  # If min=max, everything goes in first bin

    # Normalize value to range
    if value < min_val:
        return "X"
    if value > max_val:
        return chr(ord("A") + num_bins - 1)

    # Calculate bin
    bin_width = total_range / num_bins
    if value == max_val:
        return chr(ord("A") + num_bins - 1)

    bin_num = int((value - min_val) / bin_width)
    return chr(ord("A") + bin_num)


def encode_symbols(row, sym_cols, num_bins=5):
    """
    Encodes multiple columns into symbols based on their value ranges and combines them.

    Parameters:
    row: pandas Series (a single row of the dataframe)
    sym_cols: list of str (column names to encode)
    num_bins: int (number of bins for encoding)

    Returns:
    str: Combined symbols for all columns
    """
    symbols = []
    for col in sym_cols:
        value = row[col]
        min_val = row[f"min_{col}"]
        max_val = row[f"max_{col}"]
        symbol = bin_label(value, min_val, max_val, num_bins)
        symbols.append(symbol)

    return "".join(symbols)


def get_splits_symbols(split_times_wide, split_cols):
    """Generate symbolic encoding of split times."""
    split_times_wide = split_times_wide.copy()
    for col in split_cols:
        split_times_wide[f"min_{col}"] = split_times_wide[col].min()
        split_times_wide[f"max_{col}"] = split_times_wide[col].max()
    split_times_wide["allSyms"] = split_times_wide.apply(
        lambda row: encode_symbols(row, split_cols, 5),
        axis=1,
    )
    return split_times_wide[["carNo", "allSyms"]]
