from pandas import DataFrame, isna
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm
from seaborn.utils import relative_luminance


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
    max_delta=30,  # Accepts: None, 30 is 30s, so 1s/km pace diff on the longest stage
    use_linear_cmap=True,
    cmap_colors=None,
    balancer=False,
    drop_last_quantile=True,
    upper_limit=None,
    lower_limit=None,
):

    ##-- via chatGPT
    # This is chatGPT's estimate of what seaborn does
    # but seaborn heatmap changes the text to white for dark colours,
    # has better color ranges, etc. Reuse seaborn code?
    def color_by_value_with_cmap(val, vmax, vmin):
        colors = ["green", "white", "red"] if not cmap_colors else cmap_colors
        if reverse_palette:
            colors.reverse()

        cmap = LinearSegmentedColormap.from_list("custom_cmap", colors)
        base_style = (
            "text-align: center; padding: 8px; border-radius: 8px; border: 2px solid white;"
        )

        if isna(val):
            return f"{base_style} background-color: #d9d9d9;"  # Light gray for NaN
        elif val == 0:
            return f"{base_style} background-color: #f0f0f0;"
        else:
            normed = TwoSlopeNorm(vmin=vmin, vcenter=0, vmax=vmax)
            rgba = cmap(normed(val))  # norm maps val to 0-1, cmap maps 0-1 to color
            r, g, b, a = [int(255 * c) for c in rgba]
            # Pinched from seaborn heatmap
            lum = relative_luminance(rgba)
            text_color = (
                "rgba(0, 0, 0, 1)" if lum > 0.408 else "rgba(255, 255, 255, 1)"
            )
            style_ = f"background-color: rgba({r},{g},{b},{a}); color: {text_color}  !important;"
            return style_

    # --

    # Function to create color-coded background with rounded corners
    def color_by_value(val, pos_max, neg_max,):
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
    # Across the table, we need to get the global max pos and min neg
    if not within_cols_gradient:
        # all_values = df[cols].values.flatten()
        # all_values = all_values[~isna(all_values)]  # Remove NaN values
        all_values = df[cols].stack()

        pos_vals = all_values[all_values > 0]
        neg_vals = all_values[all_values < 0]

        if drop_last_quantile:
            global_pos_max = (
                pos_vals[pos_vals < pos_vals.quantile(0.9)].max()
                if len(pos_vals) > 0
                else 1
            )
            global_neg_min = (
                neg_vals[neg_vals > neg_vals.quantile(0.1)].min()
                if len(neg_vals) > 0
                else -1
            )
        else:
            global_pos_max = pos_vals.max() if len(pos_vals) else 1
            global_neg_min = neg_vals.min() if len(neg_vals) else -1

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
        # styler = styler.map(
        #    lambda x: color_by_value(x, col_pos_max, col_neg_min), subset=[col]
        # )

        # Override with user-provided limits if specified
        if upper_limit is not None:
            col_pos_max = upper_limit
        if lower_limit is not None:
            col_neg_min = lower_limit
            
        # Try to make the colours symmetrical -ish
        if balancer:
            multiplier_ = 5
            if abs(col_neg_min)>col_pos_max and abs(col_neg_min)< multiplier_ * col_pos_max:
                col_pos_max = abs(col_neg_min)
            elif abs(col_neg_min)<col_pos_max and abs(col_neg_min) * multiplier_ > col_pos_max:
                col_neg_min = -1 * col_pos_max

        # TO DO - consider pace bsed thresholds
        # Pass in sector/stage distances and set a nominal pace threshold (s/km)
        # Then set colour based on on maxing the color at the pace threshold
        if use_linear_cmap:
            color_func = lambda x, pos_max=col_pos_max, neg_max=col_neg_min: color_by_value_with_cmap(
                x, pos_max, neg_max
            )
        else:
            color_func = lambda x, pos_max=col_pos_max, neg_max=col_neg_min: color_by_value(
            x, pos_max, neg_max
        ) 
        styler = styler.map(color_func, subset=[col])

    return styler
