import pandas as pd


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
    if pd.isna(value) or pd.isna(min_val) or pd.isna(max_val) or pd.isna(num_bins):
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


def get_splits_symbols(split_times_wide_numeric, split_cols):
    """Generate symbolic encoding of split times."""
    split_times_wide_numeric = split_times_wide_numeric.copy()
    for col in split_cols:
        split_times_wide_numeric[f"min_{col}"] = split_times_wide_numeric[col].min()
        split_times_wide_numeric[f"max_{col}"] = split_times_wide_numeric[col].max()
    split_times_wide_numeric["allSyms"] = split_times_wide_numeric.apply(
        lambda row: encode_symbols(row, split_cols, 5),
        axis=1,
    )
    return split_times_wide_numeric[["carNo", "allSyms"]]
