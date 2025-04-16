from pandas import DataFrame, isna


def df_color_gradient_styler(
    df,
    cols=None,
    within_cols_gradient=True,
    min_intensity=0.01,
    max_intensity=0.9,
    intensity_range=0.9,
    reverse_palette=False,
    pos_color=(255, 70, 70),
    neg_color=(40, 255, 40),
    max_delta=None,
):
    # Function to create color-coded background with rounded corners
    def color_by_value(val, pos_max, neg_max):
        # Define base styles for all cells
        base_style = "text-align: center; padding: 8px; border-radius: 8px; border: 2px solid white;"

        # Handle NaN values
        if isna(val):
            return f"{base_style} background-color: #d9d9d9;"  # Light gray for NaN
        elif val == 0:
            return f"{base_style} background-color: #f0f0f0;"

        # Check if value exceeds max_delta threshold
        if max_delta is not None and abs(val) >= max_delta:
            # Use max intensity if value exceeds threshold
            intensity = max_intensity
        else:
            # Apply color based on value and palette configuration
            if (val > 0 and not reverse_palette) or (val < 0 and reverse_palette):
                # Positive values (or negative if palette is reversed)
                ref_val = pos_max if pos_max != 0 else 1
                intensity = min(
                    min_intensity + (abs(val) / abs(ref_val)) * intensity_range,
                    max_intensity,
                )
            else:
                # Negative values (or positive if palette is reversed)
                ref_val = neg_max if neg_max != 0 else -1
                intensity = min(
                    min_intensity + (abs(val) / abs(ref_val)) * intensity_range,
                    max_intensity,
                )

        # Determine color based on value and palette configuration
        if (val > 0 and not reverse_palette) or (val < 0 and reverse_palette):
            color = pos_color
        else:
            color = neg_color

        return f"{base_style} background-color: rgba({color[0]}, {color[1]}, {color[2]}, {intensity});"

    if cols is None:
        return DataFrame()

    # Start with basic styling
    styler = df.style.format(
        {col: "{:.1f}" for col in cols},
        na_rep="",  # This will display empty string instead of 'nan'
    )

    # Add table styles for spacing between cells
    styler = styler.set_table_styles(
        [
            {"selector": "td", "props": [("text-align", "center"), ("padding", "5px")]},
            {"selector": "th", "props": [("text-align", "center"), ("padding", "5px")]},
            {
                "selector": "table",
                "props": [("border-collapse", "separate"), ("border-spacing", "5px")],
            },
        ]
    )

    # Calculate global max/min values if not using within-column gradients
    if not within_cols_gradient:
        all_values = df[cols].values.flatten()
        all_values = all_values[~isna(all_values)]  # Remove NaN values

        pos_vals = all_values[all_values > 0]
        neg_vals = all_values[all_values < 0]

        global_pos_max = pos_vals.max() if len(pos_vals) > 0 else 1
        global_neg_min = neg_vals.min() if len(neg_vals) > 0 else -1

    # Process each column
    for col in cols:
        if within_cols_gradient:
            # Calculate column-specific max values
            col_values = df[col].dropna()
            pos_vals = col_values[col_values > 0]
            neg_vals = col_values[col_values < 0]

            col_pos_max = pos_vals.max() if not pos_vals.empty else 1
            col_neg_min = neg_vals.min() if not neg_vals.empty else -1
        else:
            # Use global max/min values
            col_pos_max = global_pos_max
            col_neg_min = global_neg_min

        # Apply styling function to this column
        styler = styler.applymap(
            lambda x: color_by_value(x, col_pos_max, col_neg_min), subset=[col]
        )

    return styler
