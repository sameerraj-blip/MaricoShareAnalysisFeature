"""Data operations using pandas"""
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Literal, Union
from datetime import datetime
import re


def round_numeric_values_to_2_decimals(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Round all numeric (float) values in the data to 2 decimal places.
    This ensures consistent decimal precision across all data operations.
    
    Args:
        data: List of dictionaries representing rows
    
    Returns:
        List of dictionaries with numeric values rounded to 2 decimal places
    """
    rounded_data = []
    for row in data:
        rounded_row = {}
        for key, value in row.items():
            if value is None or pd.isna(value):
                rounded_row[key] = None
            elif isinstance(value, (int, np.integer)):
                # Keep integers as-is
                rounded_row[key] = int(value) if isinstance(value, np.integer) else value
            elif isinstance(value, (float, np.floating)):
                # Round floats to 2 decimal places
                rounded_row[key] = round(float(value), 2)
            elif isinstance(value, str):
                # Try to convert string numbers to float and round
                try:
                    num_value = float(value)
                    if not np.isnan(num_value) and np.isfinite(num_value):
                        rounded_row[key] = round(num_value, 2)
                    else:
                        rounded_row[key] = value
                except (ValueError, TypeError):
                    # Not a number, keep as string
                    rounded_row[key] = value
            else:
                # Keep other types as-is (dates, booleans, etc.)
                rounded_row[key] = value
        rounded_data.append(rounded_row)
    return rounded_data


def is_id_column(column_name: str) -> bool:
    """
    Identify if a column is an ID column (identifier field).
    ID columns represent unique identifiers or entities (e.g., order_id, customer_id, item_id).
    
    Rules:
    - Columns ending with _id (e.g., order_id, customer_id, item_id)
    - Columns named "id" (case-insensitive)
    - Columns with _id_ in the name
    - Common ID patterns: order_id, item_id, customer_id, user_id, product_id, transaction_id, etc.
    
    Returns:
        True if the column is an identifier field, False otherwise
    """
    lower = column_name.lower().strip()
    
    # Match patterns: *_id, *_ID, or explicit patterns
    if re.search(r'_id$|^id$|_id_', column_name, re.IGNORECASE):
        return True
    
    # Common ID column names
    common_id_patterns = [
        'order_id', 'item_id', 'customer_id', 'user_id', 'product_id', 
        'transaction_id', 'invoice_id', 'payment_id', 'shipment_id',
        'employee_id', 'vendor_id', 'supplier_id', 'client_id',
        'account_id', 'contact_id', 'lead_id', 'opportunity_id',
        'case_id', 'ticket_id', 'request_id', 'record_id',
        'entity_id', 'object_id', 'reference_id', 'ref_id'
    ]
    
    if lower in common_id_patterns:
        return True
    
    # Check if column name contains "id" as a word (not just as part of another word)
    # e.g., "order id" (with space) should match, but "valid" should not
    if re.search(r'\bid\b', lower) and len(lower) <= 30:  # Reasonable length for ID columns
        # Additional check: if it's a short name with "id", likely an ID column
        if len(lower.split()) <= 3:  # e.g., "order id", "customer id"
            return True
    
    return False


def is_price_column(column_name: str) -> bool:
    """
    Identify if a column represents prices or monetary values.
    """
    lower = column_name.lower().strip()
    price_patterns = [
        'price', 'cost', 'amount', 'value', 'revenue', 'sales', 'income',
        'fee', 'charge', 'payment', 'total', 'subtotal', 'tax', 'discount_amount',
        'price_per_unit', 'unit_price', 'selling_price', 'purchase_price'
    ]
    return any(pattern in lower for pattern in price_patterns)


def is_percentage_or_rate_column(column_name: str) -> bool:
    """
    Identify if a column represents percentages, rates, or ratios.
    """
    lower = column_name.lower().strip()
    percentage_patterns = [
        'percent', 'percentage', 'rate', 'ratio', 'pct', '%',
        'discount_percent', 'conversion_rate', 'roi', 'margin',
        'efficiency', 'utilization', 'coverage', 'penetration'
    ]
    return any(pattern in lower for pattern in percentage_patterns) or lower.endswith('_rate') or lower.endswith('_ratio')


def is_boolean_column(column_name: str, series: pd.Series) -> bool:
    """
    Identify if a column represents boolean/flag values.
    """
    lower = column_name.lower().strip()
    boolean_patterns = [
        'is_', 'has_', 'can_', 'should_', 'will_', 'did_', 'was_',
        'active', 'enabled', 'completed', 'failed', 'success', 'valid',
        'flag', 'status_bool'
    ]
    
    # Check column name patterns
    if any(lower.startswith(pattern) for pattern in boolean_patterns):
        return True
    
    # Check if values are boolean-like
    if series.dtype == 'bool':
        return True
    
    # Check if values are 0/1 or True/False strings
    if series.dtype == 'object':
        unique_vals = series.dropna().unique()
        if len(unique_vals) <= 3:
            str_vals = [str(v).lower() for v in unique_vals]
            bool_like = {'true', 'false', '1', '0', 'yes', 'no', 'y', 'n', 't', 'f'}
            if all(v in bool_like for v in str_vals):
                return True
    
    return False


def is_date_column(column_name: str, series: pd.Series) -> bool:
    """
    Identify if a column represents dates or timestamps.
    """
    lower = column_name.lower().strip()
    date_patterns = [
        'date', 'time', 'timestamp', 'created_at', 'updated_at',
        'start_date', 'end_date', 'birth_date', 'join_date'
    ]
    
    if any(pattern in lower for pattern in date_patterns):
        return True
    
    # Check if pandas datetime type
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    
    return False


def is_derived_or_ratio_column(column_name: str, available_columns: List[str]) -> bool:
    """
    Identify if a column is likely a derived/calculated column (ratio, percentage, KPI).
    These should not be aggregated directly - base components should be aggregated first.
    """
    lower = column_name.lower().strip()
    
    # Common derived column patterns
    derived_patterns = [
        'rate', 'ratio', 'percent', 'percentage', 'pct', 'roi', 'margin',
        'efficiency', 'conversion', 'yield', 'utilization', 'coverage'
    ]
    
    if any(pattern in lower for pattern in derived_patterns):
        return True
    
    # Check if column name suggests it's calculated from others
    # e.g., "total" might be sum of components, "average" might be calculated
    if lower in ['total', 'average', 'avg', 'mean', 'median', 'sum']:
        # Check if there are base columns that could be summed
        base_patterns = ['qty', 'quantity', 'amount', 'value', 'price', 'cost']
        if any(base in col.lower() for col in available_columns for base in base_patterns):
            return True
    
    return False


def get_count_name_for_id_column(column_name: str) -> str:
    """
    Generate a meaningful count name for an ID column.
    
    Rules:
    - For columns ending with _id: remove _id and add meaningful suffix
    - Use "unique_" prefix for distinct counts (e.g., "unique_customers")
    - Use descriptive names like "order_count", "item_count", "unique_customers"
    
    Examples:
    - "order_id" -> "order_count"
    - "customer_id" -> "unique_customers"
    - "item_id" -> "item_count"
    - "user_id" -> "unique_users"
    """
    lower = column_name.lower().strip()
    
    # Special cases for better naming
    special_cases = {
        'customer_id': 'unique_customers',
        'user_id': 'unique_users',
        'client_id': 'unique_clients',
        'account_id': 'unique_accounts',
        'contact_id': 'unique_contacts',
        'lead_id': 'unique_leads',
        'employee_id': 'unique_employees',
        'vendor_id': 'unique_vendors',
        'supplier_id': 'unique_suppliers',
    }
    
    if lower in special_cases:
        return special_cases[lower]
    
    # Remove _id suffix and add meaningful suffix
    if lower.endswith('_id'):
        base_name = lower.replace('_id', '')
        # For entity names, use "unique_" prefix
        if base_name in ['customer', 'user', 'client', 'account', 'contact', 'lead', 
                         'employee', 'vendor', 'supplier', 'person', 'member']:
            return f"unique_{base_name}s"
        # For other IDs, use "_count" suffix
        return f"{base_name}_count"
    
    # For "id" or other patterns
    if lower == 'id':
        return 'record_count'
    
    # For columns with "id" in the name
    if 'id' in lower:
        # Try to extract the meaningful part
        parts = lower.split('_')
        if len(parts) > 1:
            # Take the part before "id"
            base = '_'.join(parts[:-1]) if parts[-1] == 'id' else '_'.join(parts)
            return f"{base}_count"
    
    # Fallback: generic count name
    return f"{lower}_count"


def remove_nulls(
    data: List[Dict[str, Any]],
    column: Optional[str] = None,
    method: Literal["delete", "mean", "median", "mode", "custom"] = "delete",
    custom_value: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Remove or impute null values in data.
    
    Args:
        data: List of dictionaries representing rows
        column: Column name to process (None for all columns)
        method: How to handle nulls - "delete" removes rows, others impute
        custom_value: Custom value for imputation (if method is "custom")
    
    Returns:
        Dictionary with:
            - data: Modified data
            - rows_before: Number of rows before operation
            - rows_after: Number of rows after operation
            - nulls_removed: Number of nulls handled
    """
    df = pd.DataFrame(data)
    rows_before = len(df)
    
    if column and column not in df.columns:
        raise ValueError(f"Column '{column}' not found in data")
    
    # Preprocess: Convert string "null" values to actual NaN BEFORE processing
    # This handles cases where data has string "null" instead of actual null/NaN
    for col in df.columns:
        if df[col].dtype == "object":
            # Replace string "null" (case-insensitive, exact match) with NaN
            df[col] = df[col].replace(['null', 'NULL', 'Null', 'None', 'NONE', 'none'], np.nan)
            # Also handle empty strings and whitespace-only strings as null
            df[col] = df[col].replace(r'^\s*$', np.nan, regex=True)
    
    # Determine which columns to process
    columns_to_process = [column] if column else df.columns.tolist()
    
    nulls_removed = 0
    
    if method == "delete":
        # Delete rows with nulls in specified column(s)
        if column:
            df = df.dropna(subset=[column])
        else:
            df = df.dropna()
        nulls_removed = rows_before - len(df)
    else:
        # Impute nulls
        for col in columns_to_process:
            if col not in df.columns:
                continue
            
            # When using mean/median, try to coerce string numbers and "-" placeholders to NaN/numeric
            if method in ("mean", "median") and df[col].dtype == "object":
                # Treat strings like "-", " -  ", empty strings as NaN
                coerced = df[col].replace(r"^\s*-\s*$", np.nan, regex=True)
                # Try to convert to numeric, coercing errors to NaN
                df[col] = pd.to_numeric(coerced, errors="coerce")
            
            null_count = df[col].isna().sum()
            if null_count == 0:
                continue
            
            if method == "mean":
                fill_value = df[col].mean()
            elif method == "median":
                fill_value = df[col].median()
            elif method == "mode":
                mode_values = df[col].mode()
                fill_value = mode_values.iloc[0] if len(mode_values) > 0 else None
            elif method == "custom":
                fill_value = custom_value
            else:
                fill_value = None
            
            if fill_value is not None and not pd.isna(fill_value):
                df[col] = df[col].fillna(fill_value)
                nulls_removed += null_count
    
    rows_after = len(df)
    
    # Convert back to list of dictionaries
    result_data = df.to_dict("records")
    
    # Convert numpy types to native Python types and handle NaN
    for row in result_data:
        for key, value in row.items():
            if pd.isna(value):
                row[key] = None
            elif isinstance(value, (np.integer, np.floating)):
                row[key] = value.item()
            elif isinstance(value, np.ndarray):
                row[key] = value.tolist()
            elif isinstance(value, datetime):
                row[key] = value.isoformat()
    
    # Round all numeric values to 2 decimal places
    result_data = round_numeric_values_to_2_decimals(result_data)
    
    # Ensure scalar fields are plain Python types (not numpy types) for JSON serialization
    rows_before_py = int(rows_before)
    rows_after_py = int(rows_after)
    nulls_removed_py = int(nulls_removed)
    
    return {
        "data": result_data,
        "rows_before": rows_before_py,
        "rows_after": rows_after_py,
        "nulls_removed": nulls_removed_py
    }


def get_preview(data: List[Dict[str, Any]], limit: int = 50) -> Dict[str, Any]:
    """
    Get preview of data (top N rows).
    
    Args:
        data: List of dictionaries representing rows
        limit: Maximum number of rows to return
    
    Returns:
        Dictionary with:
            - data: Preview data
            - total_rows: Total number of rows in dataset
            - returned_rows: Number of rows returned
    """
    total_rows = len(data)
    preview_data = data[:limit]
    
    return {
        "data": preview_data,
        "total_rows": total_rows,
        "returned_rows": len(preview_data)
    }


def get_summary(data: List[Dict[str, Any]], column: Optional[str] = None) -> Dict[str, Any]:
    """
    Generate summary statistics for each column or a specific column.
    
    Args:
        data: List of dictionaries representing rows
        column: Optional column name to summarize (if None, summarizes all columns)
    
    Returns:
        Dictionary with:
            - summary: List of column summaries with statistics
    """
    df = pd.DataFrame(data)
    
    summary = []
    
    # If specific column requested, only process that column
    if column and column in df.columns:
        columns_to_process = [column]
    elif column and column not in df.columns:
        # Column specified but not found, return empty summary
        return {
            "summary": []
        }
    else:
        # Process all columns
        columns_to_process = df.columns
    
    for col in columns_to_process:
        col_data = df[col]
        
        # Normalize placeholder values and empty strings to NaN so "null" logic
        # is consistent across summary and count_nulls:
        # - Empty strings: "", "   "
        # - Dash placeholders: "-", " - ", etc.
        if col_data.dtype == 'object':
            col_data = col_data.replace(r'^\s*$', np.nan, regex=True)
            col_data = col_data.replace(r'^\s*-\s*$', np.nan, regex=True)
        
        # Check if column contains dates before checking for numeric
        # This ensures date columns are properly identified
        is_date_column = False
        dtype = 'object'
        
        if col_data.dtype == 'object':
            # Try to convert to datetime first
            # Handle various date formats: "Apr-23", "2024-01-15", "01/15/2024", etc.
            
            # First, try to parse month-year formats like "Apr-22", "Apr-2022", "April 2022"
            # pandas to_datetime might not recognize these formats well, so we pre-process them
            def try_parse_date(value):
                """Try to parse a date value, handling various formats"""
                if pd.isna(value) or value is None or value == '':
                    return pd.NaT
                
                str_value = str(value).strip()
                if not str_value:
                    return pd.NaT
                
                # Try month-year formats: "Apr-22", "Apr-2022", "April 2022", "Apr/22", etc.
                # Pattern: 3+ letter month name, separator, 2-4 digit year
                month_year_pattern = r'^([A-Za-z]{3,})[-\s/](\d{2,4})$'
                match = re.match(month_year_pattern, str_value, re.IGNORECASE)
                if match:
                    month_name = match.group(1).lower()[:3]  # First 3 letters
                    year_str = match.group(2)
                    year = int(year_str)
                    
                    # Handle 2-digit years: assume 20xx if < 50, 19xx if >= 50
                    if len(year_str) == 2:
                        year = 2000 + year if year < 50 else 1900 + year
                    
                    # Map month names to numbers
                    month_map = {
                        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                    }
                    
                    if month_name in month_map and 1900 <= year <= 2100:
                        # Create a date (first day of month)
                        try:
                            return pd.Timestamp(year=year, month=month_map[month_name], day=1)
                        except:
                            return pd.NaT
                
                # Try pandas to_datetime for other formats
                try:
                    # Try common date formats (pandas will infer format)
                    parsed = pd.to_datetime(str_value, errors='coerce')
                    if pd.notna(parsed):
                        # Validate the parsed date is reasonable
                        if hasattr(parsed, 'year') and 1900 <= parsed.year <= 2100:
                            return parsed
                except:
                    pass
                
                return pd.NaT
            
            # Apply date parsing to the column
            date_converted = col_data.apply(try_parse_date)
            non_null_count_for_type = col_data.notna().sum()
            if non_null_count_for_type > 0:
                successful_date_conversions = date_converted.notna().sum()
                date_conversion_rate = successful_date_conversions / non_null_count_for_type
                
                # If at least 50% of non-null values are dates, treat as date column
                if date_conversion_rate >= 0.5:
                    is_date_column = True
                    col_data = date_converted
                    dtype = 'date'
                else:
                    # Not a date column, try numeric conversion
                    numeric_converted = pd.to_numeric(col_data, errors='coerce')
                    successful_numeric_conversions = numeric_converted.notna().sum()
                    numeric_conversion_rate = successful_numeric_conversions / non_null_count_for_type
                    
                    if numeric_conversion_rate >= 0.7:
                        # Column is numeric, use converted version
                        col_data = numeric_converted
                        if pd.api.types.is_integer_dtype(numeric_converted):
                            dtype = 'int64'
                        elif pd.api.types.is_float_dtype(numeric_converted):
                            dtype = 'float64'
                        else:
                            dtype = 'float64'
                    else:
                        dtype = 'object'
            else:
                # All values are NaN/None after normalization
                dtype = 'float64'
        else:
            # Already a non-object type
            dtype = str(col_data.dtype)
            # Check if it's already a datetime type
            if pd.api.types.is_datetime64_any_dtype(col_data):
                is_date_column = True
                dtype = 'date'
        
        # Basic counts
        total_values = len(col_data)
        null_count = col_data.isna().sum()
        non_null_count = total_values - null_count
        
        col_summary: Dict[str, Any] = {
            "variable": col,
            "datatype": dtype,
            "total_values": total_values,
            "null_values": int(null_count),
            "non_null_values": int(non_null_count)
        }
        
        # Date statistics (if date column)
        if is_date_column or pd.api.types.is_datetime64_any_dtype(col_data):
            # For date columns, we don't calculate mean/median/std, but we can provide min/max
            date_data = col_data.dropna()
            if len(date_data) > 0:
                col_summary["min"] = str(date_data.min())
                col_summary["max"] = str(date_data.max())
            else:
                col_summary["min"] = None
                col_summary["max"] = None
            # Date columns don't have mean/median/std
            col_summary["mean"] = None
            col_summary["median"] = None
            col_summary["std_dev"] = None
        # Numeric statistics
        elif pd.api.types.is_numeric_dtype(col_data):
            numeric_data = col_data.dropna()
            if len(numeric_data) > 0:
                col_summary["mean"] = float(numeric_data.mean())
                col_summary["median"] = float(numeric_data.median())
                col_summary["std_dev"] = float(numeric_data.std()) if len(numeric_data) > 1 else 0.0
                col_summary["min"] = float(numeric_data.min())
                col_summary["max"] = float(numeric_data.max())
            else:
                col_summary["mean"] = None
                col_summary["median"] = None
                col_summary["std_dev"] = None
                col_summary["min"] = None
                col_summary["max"] = None
        else:
            col_summary["mean"] = None
            col_summary["median"] = None
            col_summary["std_dev"] = None
            col_summary["min"] = None
            col_summary["max"] = None
        
        # Mode (most frequent value)
        mode_values = col_data.mode()
        if len(mode_values) > 0:
            mode_val = mode_values.iloc[0]
            # Convert numpy types
            if isinstance(mode_val, (np.integer, np.floating)):
                col_summary["mode"] = mode_val.item()
            elif pd.isna(mode_val):
                col_summary["mode"] = None
            else:
                col_summary["mode"] = str(mode_val)
        else:
            col_summary["mode"] = None
        
        # Convert numpy types in summary
        for key, value in col_summary.items():
            if isinstance(value, (np.integer, np.floating)):
                col_summary[key] = value.item()
            elif pd.isna(value):
                col_summary[key] = None
        
        summary.append(col_summary)
    
    return {
        "summary": summary
    }


def create_derived_column(
    data: List[Dict[str, Any]],
    new_column_name: str,
    expression: str
) -> Dict[str, Any]:
    """
    Create a new column from an expression.
    
    Args:
        data: List of dictionaries representing rows
        new_column_name: Name of the new column to create
        expression: Expression to evaluate (uses [ColumnName] format)
    
    Returns:
        Dictionary with:
            - data: Modified data with new column
            - errors: List of error messages (if any)
    """
    df = pd.DataFrame(data)
    errors = []
    
    def find_column_fuzzy(search_name: str) -> str | None:
        """Find column name using fuzzy matching (case-insensitive, handles spaces/underscores)"""
        if not search_name:
            return None
        
        search_normalized = search_name.strip().lower().replace(' ', '').replace('_', '').replace('-', '')
        
        # First try exact match (case-insensitive)
        for col in df.columns:
            if col.strip().lower() == search_name.strip().lower():
                return col
        
        # Then try normalized match (ignoring spaces, underscores, dashes)
        for col in df.columns:
            col_normalized = col.strip().lower().replace(' ', '').replace('_', '').replace('-', '')
            if col_normalized == search_normalized:
                return col
        
        # Then try prefix match
        for col in df.columns:
            col_normalized = col.strip().lower().replace(' ', '').replace('_', '').replace('-', '')
            if col_normalized.startswith(search_normalized) and len(search_normalized) >= 3:
                return col
        
        # Then try contains match
        for col in df.columns:
            col_normalized = col.strip().lower().replace(' ', '').replace('_', '').replace('-', '')
            if search_normalized in col_normalized and len(search_normalized) >= 3:
                return col
        
        return None
    
    # Replace [ColumnName] with df['ColumnName'] in expression
    # Pattern: [ColumnName] or [Column Name]
    # Handle nested brackets by replacing from innermost to outermost
    def replace_column_ref(match):
        col_name = match.group(1)
        # Try to find the column using fuzzy matching
        matched_col = find_column_fuzzy(col_name)
        
        if matched_col:
            # Escape single quotes in column name if present
            col_name_escaped = matched_col.replace("'", "\\'")
            return f"df['{col_name_escaped}']"
        else:
            # Show available columns in error message for debugging
            available_cols = list(df.columns)[:10]  # Show first 10 columns
            available_cols_str = ', '.join(available_cols)
            if len(df.columns) > 10:
                available_cols_str += f", ... (total: {len(df.columns)} columns)"
            errors.append(
                f"Column '{col_name}' not found. Available columns: {available_cols_str}"
            )
            return "None"
    
    # Replace [ColumnName] patterns - process multiple times to handle nested cases
    python_expr = expression
    max_iterations = 10  # Prevent infinite loops
    iteration = 0
    while '[' in python_expr and iteration < max_iterations:
        new_expr = re.sub(r'\[([^\]]+)\]', replace_column_ref, python_expr)
        if new_expr == python_expr:
            break  # No more replacements
        python_expr = new_expr
        iteration += 1
    
    if errors:
        return {
            "data": data,
            "errors": errors
        }
    
    # Debug: print the expression being evaluated and available columns
    print(f"Evaluating expression: {python_expr}")
    print(f"Available columns in DataFrame: {list(df.columns)}")
    print(f"DataFrame shape: {df.shape}")
    
    try:
        # Check if expression uses Python ternary (if...else) instead of np.where
        # This will cause "ambiguous truth value" error with boolean arrays
        if ' if ' in python_expr and ' else ' in python_expr and 'np.where' not in python_expr:
            # Try to convert simple Python ternary to np.where using AST parsing
            # Pattern: value_if_true if condition else value_if_false
            # Convert to: np.where(condition, value_if_true, value_if_false)
            try:
                # Use regex to match common ternary patterns
                # Pattern 1: "value1" if condition else "value2" (same quotes)
                ternary_pattern1 = r'(["\'])([^"\']+)\1\s+if\s+(.+?)\s+else\s+\1([^"\']+)\1'
                match = re.search(ternary_pattern1, python_expr)
                if match:
                    quote = match.group(1)
                    value_if_true = f"{quote}{match.group(2)}{quote}"
                    condition = match.group(3).strip()
                    value_if_false = f"{quote}{match.group(4)}{quote}"
                    python_expr = f"np.where({condition}, {value_if_true}, {value_if_false})"
                    print(f"Converted Python ternary to np.where (pattern 1): {python_expr}")
                else:
                    # Pattern 2: 'value1' if condition else 'value2' (different quotes or no quotes)
                    ternary_pattern2 = r'(["\']?)([^"\']+?)\1\s+if\s+(.+?)\s+else\s+(["\']?)([^"\']+?)\4'
                    match = re.search(ternary_pattern2, python_expr)
                    if match:
                        quote1 = match.group(1) if match.group(1) else ''
                        value_if_true = f"{quote1}{match.group(2)}{quote1}" if quote1 else match.group(2)
                        condition = match.group(3).strip()
                        quote2 = match.group(4) if match.group(4) else ''
                        value_if_false = f"{quote2}{match.group(5)}{quote2}" if quote2 else match.group(5)
                        python_expr = f"np.where({condition}, {value_if_true}, {value_if_false})"
                        print(f"Converted Python ternary to np.where (pattern 2): {python_expr}")
                    else:
                        # Couldn't auto-convert, provide helpful error
                        errors.append(
                            "Conditional expressions must use np.where() format, not Python 'if...else'. "
                            f"Found: {python_expr}. "
                            "Please use format: np.where(condition, value_if_true, value_if_false). "
                            "Example: np.where(df['qty_ordered'] > df['qty_ordered'].mean(), 'outperform', 'notperforming')"
                        )
                        return {
                            "data": data,
                            "errors": errors
                        }
            except Exception as e:
                # If AST parsing fails, try simple regex fallback
                try:
                    # Match: "value1" if condition else "value2"
                    ternary_pattern = r'(["\'])([^"\']+)\1\s+if\s+(.+?)\s+else\s+(["\'])([^"\']+)\4'
                    match = re.search(ternary_pattern, python_expr)
                    if match:
                        value_if_true = f"{match.group(1)}{match.group(2)}{match.group(1)}"
                        condition = match.group(3).strip()
                        value_if_false = f"{match.group(4)}{match.group(5)}{match.group(4)}"
                        python_expr = f"np.where({condition}, {value_if_true}, {value_if_false})"
                        print(f"Converted Python ternary to np.where (regex fallback): {python_expr}")
                    else:
                        # Couldn't auto-convert, provide helpful error
                        errors.append(
                            f"Error processing conditional expression: {str(e)}. "
                            "Conditional expressions must use np.where() format, not Python 'if...else'. "
                            f"Found: {python_expr}. "
                            "Please use format: np.where(condition, value_if_true, value_if_false). "
                            "Example: np.where(df['qty_ordered'] > df['qty_ordered'].mean(), 'outperform', 'notperforming')"
                        )
                        return {
                            "data": data,
                            "errors": errors
                        }
                except Exception as e2:
                    # If everything fails, provide helpful error
                    errors.append(
                        f"Error processing conditional expression: {str(e2)}. "
                        "Conditional expressions must use np.where() format. "
                        "Example: np.where(df['qty_ordered'] > df['qty_ordered'].mean(), 'outperform', 'notperforming')"
                    )
                    return {
                        "data": data,
                        "errors": errors
                    }
        
        # Evaluate the expression
        # Use safe evaluation context with additional numpy/pandas functions
        safe_dict = {
            "df": df, 
            "pd": pd, 
            "np": np, 
            "__builtins__": {},
            # Add common numpy/pandas functions that might be used
            "mean": lambda x: x.mean() if hasattr(x, 'mean') else np.mean(x),
            "std": lambda x: x.std() if hasattr(x, 'std') else np.std(x),
            "sum": lambda x: x.sum() if hasattr(x, 'sum') else np.sum(x),
            "max": lambda x: x.max() if hasattr(x, 'max') else np.max(x),
            "min": lambda x: x.min() if hasattr(x, 'min') else np.min(x),
        }
        
        result = eval(python_expr, safe_dict)
        
        # Handle scalar result (same value for all rows)
        if isinstance(result, (int, float, str, bool)) or (isinstance(result, float) and pd.isna(result)):
            df[new_column_name] = result
        elif isinstance(result, pd.Series):
            # Ensure the Series has the same index as df
            if len(result) == len(df) and result.index.equals(df.index):
                df[new_column_name] = result
            elif len(result) == len(df):
                # Same length but different index - reset index
                df[new_column_name] = result.values
            else:
                # Try to align
                df[new_column_name] = result
        elif isinstance(result, np.ndarray):
            # Convert numpy array to pandas Series
            if len(result) == len(df):
                df[new_column_name] = pd.Series(result, index=df.index)
            else:
                errors.append(f"Expression returned array of length {len(result)}, expected {len(df)}")
                return {
                    "data": data,
                    "errors": errors
                }
        else:
            errors.append(f"Expression returned unexpected type: {type(result)}")
            return {
                "data": data,
                "errors": errors
            }
        
        # Convert back to list of dictionaries
        result_data = df.to_dict("records")
        
        # Convert numpy types to native Python types
        for row in result_data:
            for key, value in row.items():
                if pd.isna(value):
                    row[key] = None
                elif isinstance(value, (np.integer, np.floating)):
                    row[key] = value.item()
                elif isinstance(value, np.ndarray):
                    row[key] = value.tolist()
                elif isinstance(value, datetime):
                    row[key] = value.isoformat()
        
        # Round all numeric values to 2 decimal places
        result_data = round_numeric_values_to_2_decimals(result_data)
        
        return {
            "data": result_data,
            "errors": []
        }
    except Exception as e:
        errors.append(f"Error evaluating expression: {str(e)}")
        return {
            "data": data,
            "errors": errors
        }


def convert_type(
    data: List[Dict[str, Any]],
    column: str,
    target_type: Literal["numeric", "string", "date", "percentage", "boolean"]
) -> Dict[str, Any]:
    """
    Convert column data type.
    
    Args:
        data: List of dictionaries representing rows
        column: Column name to convert
        target_type: Target data type
    
    Returns:
        Dictionary with:
            - data: Modified data
            - conversion_info: Information about the conversion
    """
    df = pd.DataFrame(data)
    
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in data")
    
    conversion_info = {
        "column": column,
        "original_type": str(df[column].dtype),
        "target_type": target_type,
        "converted_type": str(df[column].dtype),
        "success": True,
        "errors": []
    }
    
    try:
        if target_type == "numeric":
            # Convert to numeric, coercing errors to NaN
            df[column] = pd.to_numeric(df[column], errors="coerce")
            conversion_info["converted_type"] = "float64"
        
        elif target_type == "string":
            df[column] = df[column].astype(str)
            conversion_info["converted_type"] = "object"
        
        elif target_type == "date":
            df[column] = pd.to_datetime(df[column], errors="coerce")
            conversion_info["converted_type"] = "datetime64[ns]"
        
        elif target_type == "percentage":
            # Convert to numeric first, then divide by 100 if values > 1
            df[column] = pd.to_numeric(df[column], errors="coerce")
            # Check if values are already in 0-1 range or 0-100 range
            non_null = df[column].dropna()
            if len(non_null) > 0:
                max_val = non_null.max()
                if max_val > 1:
                    df[column] = df[column] / 100
            conversion_info["converted_type"] = "float64"
            conversion_info["note"] = "Values converted to 0-1 range (divide by 100 if > 1)"
        
        elif target_type == "boolean":
            df[column] = df[column].astype(bool)
            conversion_info["converted_type"] = "bool"
        
        # Count conversion errors (NaN values created)
        null_before = pd.DataFrame(data)[column].isna().sum()
        null_after = df[column].isna().sum()
        conversion_errors = null_after - null_before
        if conversion_errors > 0:
            conversion_info["errors"].append(
                f"{conversion_errors} values could not be converted and were set to null"
            )
    
    except Exception as e:
        conversion_info["success"] = False
        conversion_info["errors"].append(str(e))
        raise
    
    # Convert back to list of dictionaries
    result_data = df.to_dict("records")
    
    # Convert numpy types to native Python types
    for row in result_data:
        for key, value in row.items():
            if pd.isna(value):
                row[key] = None
            elif isinstance(value, (np.integer, np.floating)):
                # For string conversions, keep as string (don't convert back to number)
                if target_type == "string" and key == column:
                    row[key] = str(value.item())
                else:
                    row[key] = value.item()
            elif isinstance(value, np.ndarray):
                row[key] = value.tolist()
            elif isinstance(value, datetime):
                row[key] = value.isoformat()
            elif target_type == "string" and key == column and not isinstance(value, str):
                # Ensure the converted column is a string
                row[key] = str(value)
    
    # Round all numeric values to 2 decimal places, but skip columns converted to string
    # We need to preserve string values, so we'll round only non-string columns
    if target_type != "string":
        result_data = round_numeric_values_to_2_decimals(result_data)
    else:
        # For string conversions, only round other columns, not the converted column
        rounded_data = []
        for row in result_data:
            rounded_row = {}
            for key, value in row.items():
                if key == column:
                    # Keep the converted column as string
                    rounded_row[key] = str(value) if value is not None else None
                else:
                    # Round other numeric columns
                    if value is None or pd.isna(value):
                        rounded_row[key] = None
                    elif isinstance(value, (int, np.integer)):
                        rounded_row[key] = int(value) if isinstance(value, np.integer) else value
                    elif isinstance(value, (float, np.floating)):
                        rounded_row[key] = round(float(value), 2)
                    elif isinstance(value, str):
                        # Try to convert string numbers to float and round (for other columns)
                        try:
                            num_value = float(value)
                            if not np.isnan(num_value) and np.isfinite(num_value):
                                rounded_row[key] = round(num_value, 2)
                            else:
                                rounded_row[key] = value
                        except (ValueError, TypeError):
                            rounded_row[key] = value
                    else:
                        rounded_row[key] = value
            rounded_data.append(rounded_row)
        result_data = rounded_data
    
    return {
        "data": result_data,
        "conversion_info": conversion_info
    }


def aggregate_data(
    data: List[Dict[str, Any]],
    group_by_column: str,
    agg_columns: Optional[List[str]] = None,
    agg_funcs: Optional[Dict[str, Literal["sum", "avg", "mean", "min", "max", "count", "median", "std", "var", "p90", "p95", "p99", "any", "all"]]] = None,
    order_by_column: Optional[str] = None,
    order_by_direction: Literal["asc", "desc"] = "asc",
    user_intent: Optional[str] = None  # User's original message to detect intent
) -> Dict[str, Any]:
    """
    Aggregate data by grouping on a column and applying semantic aggregation functions.
    
    SEMANTIC AGGREGATION RULES:
    
    1. ID COLUMNS (order_id, customer_id, etc.):
       - ALWAYS use COUNT(DISTINCT) to count unique entities
       - NEVER use SUM, AVG, MIN, or MAX
       - Renamed to meaningful names (e.g., "order_id" -> "order_count", "customer_id" -> "unique_customers")
    
    2. PRICE/PERCENTAGE/RATE COLUMNS:
       - Default to AVG when user asks for "average", "typical", "mean"
       - Default to SUM otherwise (unless explicitly requested)
       - Labeled as "avg_<column>" or "<column> (Sum)"
    
    3. BOOLEAN COLUMNS (is_active, completed, etc.):
       - Use logical aggregations: ANY (at least one true) or ALL (all true)
       - Labeled as "any_<column>" or "all_<column>"
    
    4. DISTRIBUTION-BASED ANALYSIS:
       - Use MEDIAN for "median", "typical without outliers"
       - Use percentiles (P90, P95, P99) for tail analysis
       - Labeled as "median_<column>" or "p90_<column>"
    
    5. EXTREMES:
       - Use MAX for "highest", "maximum", "top"
       - Use MIN for "lowest", "minimum", "bottom"
       - Labeled as "max_<column>" or "min_<column>"
    
    6. VARIABILITY:
       - Use STDDEV or VARIANCE for "variability", "variance", "stability"
       - Labeled as "std_<column>" or "var_<column>"
    
    7. DERIVED/RATIO COLUMNS:
       - Warning issued - should aggregate base components first, then recompute ratio
       - Currently uses mean as fallback (limitation)
    
    VALIDATION:
    - Ensures at least one numeric measure exists
    - Ensures at least one grouping dimension is defined
    - Blocks aggregation of identifiers/text unless explicitly allowed
    - Prompts for clarification if intent is ambiguous
    
    Args:
        data: List of dictionaries representing rows
        group_by_column: Column to group by
        agg_columns: Optional list of columns to aggregate (if None, uses all numeric columns)
        agg_funcs: Optional dict mapping column names to aggregation functions
                  Supported: sum, avg, mean, min, max, count, median, std, var, p90, p95, p99, any, all
        order_by_column: Optional column to sort results by
        order_by_direction: Sort direction ("asc" or "desc")
        user_intent: User's original message for semantic intent detection
    
    Returns:
        Dictionary with:
            - data: Aggregated data
            - rows_before: Number of rows before aggregation
            - rows_after: Number of rows after aggregation
    """
    df = pd.DataFrame(data)
    rows_before = len(df)
    
    if group_by_column not in df.columns:
        raise ValueError(f"Column '{group_by_column}' not found in data")
    
    # Identify ID columns, string columns, and regular numeric columns
    all_columns = df.columns.tolist()
    numeric_cols = []
    id_columns = []
    string_cols = []
    
    # Debug: Log column types
    print(f"🔍 Aggregating by '{group_by_column}'. Available columns: {all_columns}")
    print(f"🔍 Column dtypes: {df.dtypes.to_dict()}")
    
    def is_numeric_column(col_name: str, series: pd.Series) -> bool:
        """Check if a column is numeric, including string numbers. Excludes ID columns and strings."""
        # Skip ID columns
        if is_id_column(col_name):
            return False
        
        # First check if already numeric dtype
        if pd.api.types.is_numeric_dtype(series):
            return True
        
        # Try to convert to numeric and check conversion rate
        if series.dtype == 'object':
            # Try converting to numeric
            numeric_converted = pd.to_numeric(series, errors='coerce')
            non_null_count = series.notna().sum()
            if non_null_count > 0:
                successful_conversions = numeric_converted.notna().sum()
                conversion_rate = successful_conversions / non_null_count
                # If at least 70% can be converted to numeric, treat as numeric
                if conversion_rate >= 0.7:
                    return True
        
        return False
    
    def is_string_column(col_name: str, series: pd.Series) -> bool:
        """Check if a column is a string/text column"""
        # Skip ID columns (they're handled separately)
        if is_id_column(col_name):
            return False
        
        # If it's object type and not numeric, it's likely a string
        if series.dtype == 'object':
            # Try to convert to numeric
            numeric_converted = pd.to_numeric(series, errors='coerce')
            non_null_count = series.notna().sum()
            if non_null_count > 0:
                successful_conversions = numeric_converted.notna().sum()
                conversion_rate = successful_conversions / non_null_count
                # If less than 70% can be converted to numeric, treat as string
                if conversion_rate < 0.7:
                    return True
        
        # Check if it's a string dtype
        if pd.api.types.is_string_dtype(series):
            return True
        
        return False
    
    # Determine which columns to aggregate
    # If agg_columns is None or empty list, auto-detect all numeric columns (except groupBy)
    if agg_columns and len(agg_columns) > 0:
        # User specified columns - only aggregate if they're numeric and not IDs
        for col in agg_columns:
            if col not in df.columns:
                continue
            if is_id_column(col):
                id_columns.append(col)
            elif is_string_column(col, df[col]):
                string_cols.append(col)
            elif is_numeric_column(col, df[col]):
                numeric_cols.append(col)
    else:
        # Auto-detect: Use all numeric columns except group_by, IDs, and strings
        # This handles cases where user says "aggregate all the other columns" or "aggregate all columns"
        print(f"🔍 Auto-detecting numeric columns (excluding '{group_by_column}')...")
        for col in all_columns:
            if col == group_by_column:
                continue
            if is_id_column(col):
                id_columns.append(col)
            elif is_string_column(col, df[col]):
                string_cols.append(col)
            elif is_numeric_column(col, df[col]):
                numeric_cols.append(col)
    
    print(f"🔍 Column classification - Numeric: {numeric_cols}, ID: {id_columns}, String: {string_cols}")
    
    # VALIDATION LAYER: Ensure we have columns to aggregate
    if not numeric_cols and not id_columns:
        available_cols = [c for c in all_columns if c != group_by_column]
        raise ValueError(
            f"No valid columns to aggregate. Found {len(numeric_cols)} numeric columns, "
            f"{len(id_columns)} ID columns, {len(string_cols)} string columns. "
            f"Available columns (excluding '{group_by_column}'): {', '.join(available_cols[:10])}"
            f"{'...' if len(available_cols) > 10 else ''}"
        )
    
    # Detect user intent from message (if provided)
    user_intent_lower = (user_intent or '').lower()
    wants_average = any(word in user_intent_lower for word in ['average', 'avg', 'mean', 'typical'])
    wants_median = any(word in user_intent_lower for word in ['median', 'typical without outliers', 'middle'])
    wants_percentile = any(word in user_intent_lower for word in ['p90', 'p95', 'p99', 'percentile', 'top 10%', 'top 5%', 'top 1%'])
    wants_max = any(word in user_intent_lower for word in ['highest', 'maximum', 'max', 'top'])
    wants_min = any(word in user_intent_lower for word in ['lowest', 'minimum', 'min', 'bottom'])
    wants_variability = any(word in user_intent_lower for word in ['variability', 'variance', 'std', 'standard deviation', 'stability', 'spread'])
    
    # Classify columns by semantic type
    price_cols = []
    percentage_cols = []
    boolean_cols = []
    date_cols = []
    derived_cols = []
    regular_numeric_cols = []
    
    for col in numeric_cols:
        if is_price_column(col):
            price_cols.append(col)
        elif is_percentage_or_rate_column(col):
            percentage_cols.append(col)
        elif is_boolean_column(col, df[col]):
            boolean_cols.append(col)
        elif is_date_column(col, df[col]):
            date_cols.append(col)
        elif is_derived_or_ratio_column(col, all_columns):
            derived_cols.append(col)
        else:
            regular_numeric_cols.append(col)
    
    print(f"🔍 Semantic column classification:")
    print(f"   Price columns: {price_cols}")
    print(f"   Percentage/Rate columns: {percentage_cols}")
    print(f"   Boolean columns: {boolean_cols}")
    print(f"   Date columns: {date_cols}")
    print(f"   Derived/Ratio columns: {derived_cols}")
    print(f"   Regular numeric columns: {regular_numeric_cols}")
    
    # Validate ID columns - ensure they NEVER use sum/avg/min/max
    # ID columns must use COUNT(DISTINCT) to count unique entities, not aggregate values
    if agg_funcs:
        for id_col in id_columns:
            if id_col in agg_funcs and agg_funcs[id_col] in ['sum', 'avg', 'mean', 'min', 'max']:
                print(f"⚠️ ID column '{id_col}' cannot use {agg_funcs[id_col]}. ID columns represent identifiers/entities.")
                print(f"   Automatically switching to COUNT(DISTINCT) (nunique) to count unique entities.")
                # Remove from agg_funcs - will use default nunique below
                del agg_funcs[id_col]
    
    # Build aggregation dictionary for pandas groupby
    agg_dict = {}
    rename_dict = {}  # For renaming aggregated columns
    
    # Convert numeric columns that are stored as objects to numeric type
    for col in numeric_cols:
        if df[col].dtype == 'object':
            # Convert to numeric
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Process columns based on semantic type and user intent
    def get_default_func_for_column(col: str, col_type: str) -> str:
        """Determine default aggregation function based on column type and user intent."""
        # Check if user explicitly specified a function for this column
        if agg_funcs and col in agg_funcs:
            return agg_funcs[col]
        
        # Apply semantic defaults based on column type
        if col_type == 'price':
            # For prices: use AVG if user wants average/typical, otherwise SUM
            return 'mean' if wants_average else 'sum'
        elif col_type == 'percentage':
            # For percentages/rates: use AVG if user wants average/typical, otherwise SUM
            return 'mean' if wants_average else 'sum'
        elif col_type == 'boolean':
            # For boolean: use 'any' to check if at least one true exists
            return 'any'
        elif col_type == 'derived':
            # Derived columns should not be aggregated directly - warn user
            print(f"⚠️ Warning: Column '{col}' appears to be a derived/ratio column. "
                  f"Consider aggregating base components instead.")
            return 'mean'  # Default to mean as fallback
        else:
            # Regular numeric: use user intent or default to sum
            if wants_average:
                return 'mean'
            elif wants_median:
                return 'median'
            elif wants_max:
                return 'max'
            elif wants_min:
                return 'min'
            else:
                return 'sum'
    
    # Process regular numeric columns
    for col in regular_numeric_cols:
        func = get_default_func_for_column(col, 'regular')
        if func == 'avg' or func == 'mean':
            agg_dict[col] = 'mean'
            rename_dict[col] = f"avg_{col}"
        elif func == 'median':
            agg_dict[col] = 'median'
            rename_dict[col] = f"median_{col}"
        elif func == 'count':
            agg_dict[col] = 'count'
            rename_dict[col] = f"{col} (Count)"
        elif func == 'std':
            agg_dict[col] = 'std'
            rename_dict[col] = f"std_{col}"
        elif func == 'var':
            agg_dict[col] = 'var'
            rename_dict[col] = f"var_{col}"
        elif func in ['p90', 'p95', 'p99']:
            percentile = int(func[1:])
            # Store percentile value - will handle separately
            # Use a named function that pandas can handle
            def make_percentile_func(p):
                def percentile_agg(series):
                    return series.quantile(p / 100.0)
                percentile_agg.__name__ = f'p{p}'
                return percentile_agg
            agg_dict[col] = make_percentile_func(percentile)
            rename_dict[col] = f"p{percentile}_{col}"
        else:
            agg_dict[col] = func
            if func == 'max':
                rename_dict[col] = f"max_{col}"
            elif func == 'min':
                rename_dict[col] = f"min_{col}"
            else:
                rename_dict[col] = f"{col} (Sum)"
    
    # Process price columns - default to AVG for average/typical, otherwise SUM
    for col in price_cols:
        func = get_default_func_for_column(col, 'price')
        if func == 'avg' or func == 'mean':
            agg_dict[col] = 'mean'
            rename_dict[col] = f"avg_{col}"
        elif func == 'median':
            agg_dict[col] = 'median'
            rename_dict[col] = f"median_{col}"
        else:
            agg_dict[col] = 'sum'
            rename_dict[col] = f"{col} (Sum)"
    
    # Process percentage/rate columns - default to AVG for average/typical, otherwise SUM
    for col in percentage_cols:
        func = get_default_func_for_column(col, 'percentage')
        if func == 'avg' or func == 'mean':
            agg_dict[col] = 'mean'
            rename_dict[col] = f"avg_{col}"
        elif func == 'median':
            agg_dict[col] = 'median'
            rename_dict[col] = f"median_{col}"
        else:
            agg_dict[col] = 'sum'
            rename_dict[col] = f"{col} (Sum)"
    
    # Process boolean columns - use logical aggregations
    for col in boolean_cols:
        func = (agg_funcs or {}).get(col, 'any')
        # Convert boolean column to numeric (True=1, False=0) for aggregation
        if df[col].dtype == 'bool' or df[col].dtype == 'object':
            # Convert boolean-like values to 0/1
            df[col] = df[col].astype(str).str.lower().isin(['true', '1', 'yes', 'y', 't']).astype(int)
        
        if func == 'any':
            # Check if at least one true exists (max > 0)
            agg_dict[col] = 'max'  # Will be > 0 if any True
            rename_dict[col] = f"any_{col}"
        elif func == 'all':
            # Check if all values are true (min == 1)
            agg_dict[col] = 'min'  # Will be 1 if all True
            rename_dict[col] = f"all_{col}"
        else:
            # Fallback to count
            agg_dict[col] = 'count'
            rename_dict[col] = f"{col} (Count)"
    
    # Process derived/ratio columns - warn but allow aggregation
    for col in derived_cols:
        func = get_default_func_for_column(col, 'derived')
        if func == 'avg' or func == 'mean':
            agg_dict[col] = 'mean'
            rename_dict[col] = f"avg_{col}"
        else:
            agg_dict[col] = func
            rename_dict[col] = f"{col} ({func.title()})"
    
    # Process ID columns - always use nunique (COUNT(DISTINCT)) for unique entity counts
    # ID columns represent identifiers/entities, so we count distinct values, not sum/average
    # This gives us the number of unique entities per group (e.g., unique customers, unique orders)
    for id_col in id_columns:
        # Use nunique for COUNT(DISTINCT) - this counts unique ID values per group
        # This is the correct aggregation for identifier fields
        agg_dict[id_col] = 'nunique'
        print(f"📊 ID column '{id_col}' will use COUNT(DISTINCT) -> will be renamed to '{get_count_name_for_id_column(id_col)}'")
    
    if not agg_dict:
        # Provide helpful error message
        available_cols = [c for c in all_columns if c != group_by_column]
        print(f"❌ No columns to aggregate. Numeric cols found: {numeric_cols}, ID cols found: {id_columns}, String cols: {string_cols}")
        print(f"❌ All columns: {all_columns}")
        raise ValueError(f"No numeric columns to aggregate (excluding IDs and strings). Available columns (excluding '{group_by_column}'): {', '.join(available_cols[:10])}{'...' if len(available_cols) > 10 else ''}. Found {len(numeric_cols)} numeric columns, {len(id_columns)} ID columns (excluded), {len(string_cols)} string columns (excluded).")
    
    print(f"✅ Aggregating {len(numeric_cols)} numeric columns and {len(id_columns)} ID columns (COUNT(DISTINCT))")
    if id_columns:
        print(f"   ID columns will be counted as unique entities: {', '.join(id_columns)}")
    
    # Perform aggregation
    # Handle percentile and custom functions separately
    percentile_cols = {}
    regular_agg_dict = {}
    
    for col, func in agg_dict.items():
        if callable(func) and hasattr(func, '__name__') and func.__name__.startswith('p'):
            # Percentile function - handle separately
            percentile_cols[col] = func
        else:
            regular_agg_dict[col] = func
    
    # First do regular aggregations
    if regular_agg_dict:
        grouped = df.groupby(group_by_column, as_index=False).agg(regular_agg_dict)
    else:
        grouped = df[[group_by_column]].drop_duplicates()
    
    # Then add percentile aggregations using apply
    for col, percentile_func in percentile_cols.items():
        percentile_result = df.groupby(group_by_column)[col].apply(percentile_func).reset_index(name=col)
        grouped = grouped.merge(percentile_result, on=group_by_column, how='left')
    
    # Rename ID column aggregations to meaningful count names
    # ID columns are aggregated using COUNT(DISTINCT), so rename to reflect unique entity counts
    for id_col in id_columns:
        count_name = get_count_name_for_id_column(id_col)
        if id_col in grouped.columns:
            grouped = grouped.rename(columns={id_col: count_name})
            print(f"✅ Renamed ID column aggregation: '{id_col}' -> '{count_name}' (COUNT(DISTINCT))")
    
    # Apply semantic renaming (rename_dict was built during column processing above)
    if rename_dict:
        # Only rename columns that exist in the grouped dataframe
        rename_dict_filtered = {k: v for k, v in rename_dict.items() if k in grouped.columns}
        grouped = grouped.rename(columns=rename_dict_filtered)
        print(f"✅ Applied semantic renaming: {len(rename_dict_filtered)} columns")
    
    # Apply sorting if specified
    if order_by_column:
        # Find the column in the grouped result (might be renamed)
        sort_col = None
        for col in grouped.columns:
            if col == order_by_column or col.lower().startswith(order_by_column.lower()):
                sort_col = col
                break
        
        if sort_col:
            ascending = order_by_direction == "asc"
            grouped = grouped.sort_values(by=sort_col, ascending=ascending)
    
    # The grouped dataframe already contains only:
    # - groupBy column
    # - Aggregated columns (numeric_cols and id_columns with their aggregation functions)
    # No need to preserve other columns - they are not relevant to the aggregation
    print(f"✅ Aggregation complete. Result columns: {list(grouped.columns)}")
    
    rows_after = len(grouped)
    
    # Convert back to list of dictionaries
    result_data = grouped.to_dict("records")
    
    # Convert numpy types to native Python types
    for row in result_data:
        for key, value in row.items():
            if pd.isna(value):
                row[key] = None
            elif isinstance(value, (np.integer, np.floating)):
                row[key] = value.item()
            elif isinstance(value, np.ndarray):
                row[key] = value.tolist()
            elif isinstance(value, datetime):
                row[key] = value.isoformat()
    
    # Round all numeric values to 2 decimal places
    result_data = round_numeric_values_to_2_decimals(result_data)
    
    return {
        "data": result_data,
        "rows_before": int(rows_before),
        "rows_after": int(rows_after)
    }


def pivot_table(
    data: List[Dict[str, Any]],
    index_column: str,
    value_columns: Optional[List[str]] = None,
    pivot_funcs: Optional[Dict[str, Literal["sum", "avg", "mean", "min", "max", "count"]]] = None
) -> Dict[str, Any]:
    """
    Create a pivot table where the index column's values become column headers.
    
    This creates a true pivot table where:
    - The index_column's unique values become new column headers
    - Value columns are aggregated for each index value
    - All other columns are preserved in the result
    
    Example:
        Input: Week | Status | Sales | Brand
               Week1|Complete| 100  | BrandA
               Week1|InProgress| 200 | BrandA
               Week2|Complete| 150  | BrandB
        
        Output: Week | Sales_Complete | Sales_InProgress | Brand
                Week1|     100        |       200        | BrandA
                Week2|     150        |       None       | BrandB
    
    ID COLUMN HANDLING:
    - ID columns (e.g., order_id, customer_id, item_id) are automatically identified
    - ID columns ALWAYS use COUNT(DISTINCT) to count unique entities per group
    - ID columns NEVER use SUM, AVG, MIN, or MAX (these are invalid for identifiers)
    - ID column outputs are renamed to meaningful names (e.g., "order_id" -> "order_count", "customer_id" -> "unique_customers")
    - This ensures ID columns represent entity counts, not aggregated values
    
    Args:
        data: List of dictionaries representing rows
        index_column: Column whose values will become column headers (e.g., "Status" -> "Complete", "In Progress", "Not Started")
        value_columns: Optional list of columns to aggregate (if None, uses all numeric columns except index)
        pivot_funcs: Optional dict mapping column names to aggregation functions
                    Note: ID columns will override any specified function and use COUNT(DISTINCT)
    
    Returns:
        Dictionary with:
            - data: Pivoted data with preserved columns
            - rows_before: Number of rows before pivot
            - rows_after: Number of rows after pivot
    """
    df = pd.DataFrame(data)
    rows_before = len(df)
    
    if index_column not in df.columns:
        raise ValueError(f"Column '{index_column}' not found in data")
    
    all_columns = df.columns.tolist()
    
    # Helper functions (same as in aggregate_data)
    def is_numeric_column(col_name: str, series: pd.Series) -> bool:
        """Check if a column is numeric, excluding ID columns and strings"""
        if is_id_column(col_name):
            return False
        if pd.api.types.is_numeric_dtype(series):
            return True
        if series.dtype == 'object':
            numeric_converted = pd.to_numeric(series, errors='coerce')
            non_null_count = series.notna().sum()
            if non_null_count > 0:
                conversion_rate = numeric_converted.notna().sum() / non_null_count
                if conversion_rate >= 0.7:
                    return True
        return False
    
    def is_string_column(col_name: str, series: pd.Series) -> bool:
        """Check if a column is a string/text column"""
        if is_id_column(col_name):
            return False
        if series.dtype == 'object':
            numeric_converted = pd.to_numeric(series, errors='coerce')
            non_null_count = series.notna().sum()
            if non_null_count > 0:
                conversion_rate = numeric_converted.notna().sum() / non_null_count
                if conversion_rate < 0.7:
                    return True
        return pd.api.types.is_string_dtype(series)
    
    # Get unique values from index column (these will become column headers)
    index_values = df[index_column].dropna().unique().tolist()
    print(f"🔍 Pivot - Index column '{index_column}' has {len(index_values)} unique values: {index_values[:10]}{'...' if len(index_values) > 10 else ''}")
    
    # No limit on unique values - allow any number of pivot values
    
    # Determine value columns to aggregate
    if value_columns:
        raw_value_columns = value_columns
    else:
        # Use all numeric columns except the index column
        raw_value_columns = [c for c in all_columns if c != index_column and is_numeric_column(c, df[c])]
    
    # Separate ID columns, string columns, and regular numeric columns
    regular_value_columns = []
    id_value_columns = []
    string_value_columns = []
    
    for col in raw_value_columns:
        if col not in df.columns:
            continue
        if is_id_column(col):
            id_value_columns.append(col)
        elif is_string_column(col, df[col]):
            string_value_columns.append(col)
        elif is_numeric_column(col, df[col]):
            regular_value_columns.append(col)
    
    print(f"🔍 Pivot - Numeric: {regular_value_columns}, ID (excluded): {id_value_columns}, String (excluded): {string_value_columns}")
    
    # Identify columns to preserve (all columns except index_column and value_columns)
    # NOTE: We will add the index_column back after pivoting so users can see the original status values
    # These will be kept in the result
    columns_to_preserve = [
        c for c in all_columns 
        if c != index_column 
        and c not in regular_value_columns 
        and c not in id_value_columns
    ]
    print(f"📋 Pivot - Preserving {len(columns_to_preserve)} columns: {columns_to_preserve}")
    print(f"📋 Pivot - Index column '{index_column}' will be added back to result after pivoting")
    
    # Estimate output size: number of columns = preserved columns + (value columns * unique index values)
    estimated_columns = len(columns_to_preserve) + (len(regular_value_columns) + len(id_value_columns)) * len(index_values)
    if estimated_columns > 1000:
        print(f"⚠️ Warning: Pivot will create approximately {estimated_columns} columns. This may result in a very large output.")
        # Still allow it, but warn the user
    
    # Validate ID columns - ensure they NEVER use sum/avg/min/max
    if pivot_funcs:
        for id_col in id_value_columns:
            if id_col in pivot_funcs and pivot_funcs[id_col] in ['sum', 'avg', 'mean', 'min', 'max']:
                print(f"⚠️ ID column '{id_col}' cannot use {pivot_funcs[id_col]} in pivot. ID columns represent identifiers/entities.")
                print(f"   Automatically switching to COUNT(DISTINCT) (nunique) to count unique entities.")
                del pivot_funcs[id_col]
    
    if not regular_value_columns and not id_value_columns:
        raise ValueError("No columns to aggregate in pivot")
    
    # Create pivot tables for each value column
    # We'll combine all value columns into a single pivot operation for efficiency
    all_value_cols = regular_value_columns + id_value_columns
    
    if not all_value_cols:
        raise ValueError("No columns to aggregate in pivot")
    
    # Build aggregation dictionary for all value columns at once
    agg_dict = {}
    
    # Process regular numeric columns
    for col in regular_value_columns:
        func = (pivot_funcs or {}).get(col, 'sum')
        if func == 'avg' or func == 'mean':
            agg_dict[col] = 'mean'
        elif func == 'count':
            agg_dict[col] = 'count'
        else:
            agg_dict[col] = func
    
    # Process ID columns - always use nunique (COUNT(DISTINCT))
    for id_col in id_value_columns:
        agg_dict[id_col] = 'nunique'
    
    print(f"✅ Pivoting {len(all_value_cols)} value column(s) using aggregation functions: {agg_dict}")
    
    # Create pivot table: index_column values become columns
    # Group by preserved columns (if any), pivot on index_column, aggregate value columns
    if columns_to_preserve:
        # Group by preserved columns, pivot index_column values into columns
        # Use agg_dict if we have multiple functions, otherwise use the single function
        aggfunc_param = agg_dict if len(set(agg_dict.values())) > 1 else list(agg_dict.values())[0]
        
        pivot_df = df.pivot_table(
            index=columns_to_preserve,
            columns=index_column,
            values=all_value_cols,
            aggfunc=aggfunc_param,
            fill_value=None
        )
        
        # Flatten MultiIndex columns: create columns like "Sales_Complete", "Sales_In Progress"
        if isinstance(pivot_df.columns, pd.MultiIndex):
            # MultiIndex: (value_col, index_value)
            new_columns = []
            for col_tuple in pivot_df.columns:
                value_col = col_tuple[0]
                index_val = str(col_tuple[1]).replace(' ', '_')
                # Handle ID columns - use count name
                if value_col in id_value_columns:
                    count_name = get_count_name_for_id_column(value_col)
                    new_columns.append(f"{count_name}_{index_val}")
                else:
                    new_columns.append(f"{value_col}_{index_val}")
            pivot_df.columns = new_columns
        else:
            # Single value column
            new_columns = []
            for col in pivot_df.columns:
                index_val = str(col).replace(' ', '_')
                if len(regular_value_columns) == 1:
                    new_columns.append(f"{regular_value_columns[0]}_{index_val}")
                elif len(id_value_columns) == 1:
                    count_name = get_count_name_for_id_column(id_value_columns[0])
                    new_columns.append(f"{count_name}_{index_val}")
                else:
                    new_columns.append(str(col))
            pivot_df.columns = new_columns
        
        pivot_df = pivot_df.reset_index()
        result_df = pivot_df
        
        # Ensure we have all unique combinations of preserved columns
        unique_combinations = df[columns_to_preserve].drop_duplicates()
        result_df = unique_combinations.merge(result_df, on=columns_to_preserve, how='left')
        
    else:
        # No preserved columns - aggregate everything into a single row
        # This creates a summary pivot table
        aggfunc_param = agg_dict if len(set(agg_dict.values())) > 1 else list(agg_dict.values())[0]
        
        pivot_df = df.pivot_table(
            columns=index_column,
            values=all_value_cols,
            aggfunc=aggfunc_param,
            fill_value=None
        )
        
        # Flatten MultiIndex columns
        if isinstance(pivot_df.columns, pd.MultiIndex):
            new_columns = []
            for col_tuple in pivot_df.columns:
                value_col = col_tuple[0]
                index_val = str(col_tuple[1]).replace(' ', '_')
                if value_col in id_value_columns:
                    count_name = get_count_name_for_id_column(value_col)
                    new_columns.append(f"{count_name}_{index_val}")
                else:
                    new_columns.append(f"{value_col}_{index_val}")
            pivot_df.columns = new_columns
        
        pivot_df = pivot_df.reset_index(drop=True)
        result_df = pivot_df
        
        print(f"⚠️ No preserved columns - created summary pivot with {len(result_df)} row(s)")
    
    # Add the index_column back to the result by finding the status value with the highest aggregated value
    # This gives a "primary status" for each row
    if index_column not in result_df.columns and columns_to_preserve:
        status_column_data = []
        
        for idx, row in result_df.iterrows():
            # Find which pivot columns have the highest non-null values
            # The pivot columns are named like "ValueColumn_StatusValue"
            max_value = None
            max_status = None
            
            for col in result_df.columns:
                if col not in columns_to_preserve and '_' in col:
                    # This is likely a pivot column
                    value = row[col]
                    if pd.notna(value) and value != 0:
                        # Extract status value from column name
                        # Try to match against known index_values
                        col_lower = col.lower()
                        for status_val in index_values:
                            status_val_clean = str(status_val).replace(' ', '_').lower()
                            # Check if status value appears in column name
                            if status_val_clean in col_lower or col_lower.endswith('_' + status_val_clean):
                                # Use the status value with the highest aggregated value
                                if max_value is None or (isinstance(value, (int, float)) and isinstance(max_value, (int, float)) and value > max_value):
                                    max_value = value
                                    max_status = status_val
                                break
                        # If no match found, try extracting from column name directly
                        if max_status is None:
                            parts = col.split('_')
                            if len(parts) > 1:
                                # Try to match the last part or combination
                                potential_status = '_'.join(parts[-2:]) if len(parts) > 2 else parts[-1]
                                if potential_status in [str(v).replace(' ', '_') for v in index_values]:
                                    if max_value is None or (isinstance(value, (int, float)) and isinstance(max_value, (int, float)) and value > max_value):
                                        max_value = value
                                        max_status = potential_status
            
            # If we found a status, use it; otherwise try to get from original data
            if max_status is not None:
                status_column_data.append(max_status)
            else:
                # Try to find a matching row in original data to get status
                matching_status = None
                for orig_idx, orig_row in df.iterrows():
                    match = True
                    for preserve_col in columns_to_preserve:
                        if preserve_col in result_df.columns and preserve_col in orig_row:
                            if str(orig_row[preserve_col]) != str(row[preserve_col]):
                                match = False
                                break
                    if match and index_column in orig_row:
                        matching_status = orig_row[index_column]
                        break
                
                status_column_data.append(matching_status if matching_status is not None else index_values[0] if index_values else None)
        
        result_df[index_column] = status_column_data
        print(f"✅ Added '{index_column}' column back to pivot result (using primary status based on highest values)")
    
    # Sort by index_column (status) so rows are grouped by status category
    if index_column in result_df.columns:
        result_df = result_df.sort_values(by=index_column, na_position='last')
        print(f"✅ Sorted result by '{index_column}' column")
    
    print(f"✅ Pivot complete. Result columns: {list(result_df.columns)}")
    print(f"✅ Result shape: {result_df.shape}")
    
    rows_after = len(result_df)
    
    # Convert back to list of dictionaries
    result_data = result_df.to_dict("records")
    
    # Convert numpy types to native Python types
    for row in result_data:
        for key, value in row.items():
            if pd.isna(value):
                row[key] = None
            elif isinstance(value, (np.integer, np.floating)):
                row[key] = value.item()
            elif isinstance(value, np.ndarray):
                row[key] = value.tolist()
            elif isinstance(value, datetime):
                row[key] = value.isoformat()
    
    # Round all numeric values to 2 decimal places
    result_data = round_numeric_values_to_2_decimals(result_data)
    
    return {
        "data": result_data,
        "rows_before": int(rows_before),
        "rows_after": int(rows_after)
    }


def identify_outliers(
    data: List[Dict[str, Any]],
    column: Optional[str] = None,
    method: Literal["iqr", "zscore", "isolation_forest", "local_outlier_factor"] = "iqr",
    threshold: Optional[float] = None
) -> Dict[str, Any]:
    """
    Identify outliers in numeric columns.
    
    Args:
        data: List of dictionaries representing rows
        column: Optional specific column to analyze (if None, analyzes all numeric columns)
        method: Detection method - "iqr" (default), "zscore", "isolation_forest", or "local_outlier_factor"
        threshold: Optional threshold (default: 3 for zscore, 1.5 for IQR)
    
    Returns:
        Dictionary with:
            - outliers: List of outlier records with row_index, column, value, and method details
            - summary: Summary statistics including total outliers and counts by column
            - statistics: Statistical information for each column analyzed
    """
    df = pd.DataFrame(data)
    
    # Determine columns to analyze
    if column:
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in data")
        columns_to_analyze = [column]
    else:
        # Analyze all numeric columns
        columns_to_analyze = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
        if not columns_to_analyze:
            raise ValueError("No numeric columns found in data")
    
    outliers = []
    outliers_by_column: Dict[str, int] = {}
    statistics: Dict[str, Dict[str, float]] = {}
    
    # Set default threshold
    if threshold is None:
        threshold = 3.0 if method == "zscore" else 1.5
    
    for col in columns_to_analyze:
        series = df[col].dropna()
        if len(series) == 0:
            continue
        
        # Calculate statistics
        mean_val = float(series.mean())
        median_val = float(series.median())
        std_val = float(series.std())
        q1 = float(series.quantile(0.25))
        q3 = float(series.quantile(0.75))
        iqr = q3 - q1
        
        statistics[col] = {
            "mean": mean_val,
            "median": median_val,
            "std_dev": std_val,
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "lower_bound": q1 - threshold * iqr,
            "upper_bound": q3 + threshold * iqr
        }
        
        column_outliers = []
        
        if method == "iqr":
            lower_bound = q1 - threshold * iqr
            upper_bound = q3 + threshold * iqr
            
            for idx, value in enumerate(df[col]):
                if pd.isna(value):
                    continue
                if value < lower_bound or value > upper_bound:
                    column_outliers.append({
                        "row_index": idx,
                        "column": col,
                        "value": float(value),
                        "iqr_lower": lower_bound,
                        "iqr_upper": upper_bound,
                        "method": "iqr"
                    })
        
        elif method == "zscore":
            for idx, value in enumerate(df[col]):
                if pd.isna(value):
                    continue
                z_score = abs((value - mean_val) / std_val) if std_val > 0 else 0
                if z_score > threshold:
                    column_outliers.append({
                        "row_index": idx,
                        "column": col,
                        "value": float(value),
                        "z_score": float(z_score),
                        "method": "zscore"
                    })
        
        elif method == "isolation_forest":
            try:
                from sklearn.ensemble import IsolationForest
                # Use only this column for isolation forest
                X = series.values.reshape(-1, 1)
                contamination = min(0.1, max(0.01, len(column_outliers) / len(series)) if column_outliers else 0.1)
                iso_forest = IsolationForest(contamination=contamination, random_state=42)
                predictions = iso_forest.fit_predict(X)
                
                for idx, (value, pred) in enumerate(zip(df[col], predictions)):
                    if pd.isna(value):
                        continue
                    if pred == -1:  # Outlier
                        column_outliers.append({
                            "row_index": idx,
                            "column": col,
                            "value": float(value),
                            "method": "isolation_forest"
                        })
            except ImportError:
                raise ValueError("scikit-learn is required for isolation_forest method")
        
        elif method == "local_outlier_factor":
            try:
                from sklearn.neighbors import LocalOutlierFactor
                # Use only this column for LOF
                X = series.values.reshape(-1, 1)
                n_neighbors = min(20, len(series) - 1)
                if n_neighbors < 2:
                    continue
                lof = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=0.1)
                predictions = lof.fit_predict(X)
                
                for idx, (value, pred) in enumerate(zip(df[col], predictions)):
                    if pd.isna(value):
                        continue
                    if pred == -1:  # Outlier
                        column_outliers.append({
                            "row_index": idx,
                            "column": col,
                            "value": float(value),
                            "method": "local_outlier_factor"
                        })
            except ImportError:
                raise ValueError("scikit-learn is required for local_outlier_factor method")
        
        outliers.extend(column_outliers)
        outliers_by_column[col] = len(column_outliers)
    
    return {
        "outliers": outliers,
        "summary": {
            "total_outliers": len(outliers),
            "columns_analyzed": columns_to_analyze,
            "outliers_by_column": outliers_by_column
        },
        "statistics": statistics
    }


def treat_outliers(
    data: List[Dict[str, Any]],
    column: Optional[str] = None,
    method: Literal["iqr", "zscore", "isolation_forest", "local_outlier_factor"] = "iqr",
    threshold: Optional[float] = None,
    treatment: Literal["remove", "cap", "winsorize", "transform", "impute"] = "remove",
    treatment_value: Optional[Union[Literal["mean", "median", "mode", "min", "max"], float]] = None
) -> Dict[str, Any]:
    """
    Treat outliers in numeric columns.
    
    Args:
        data: List of dictionaries representing rows
        column: Optional specific column to treat (if None, treats all numeric columns)
        method: Detection method - "iqr" (default), "zscore", "isolation_forest", or "local_outlier_factor"
        threshold: Optional threshold (default: 3 for zscore, 1.5 for IQR)
        treatment: How to treat outliers - "remove" (default), "cap", "winsorize", "transform", or "impute"
        treatment_value: For impute/cap - "mean", "median", "mode", "min", "max", or a numeric value
    
    Returns:
        Dictionary with:
            - data: Treated data
            - rows_before: Number of rows before treatment
            - rows_after: Number of rows after treatment
            - outliers_treated: Number of outliers treated
            - treatment_applied: Description of treatment
            - summary: Summary of treatment by column
    """
    df = pd.DataFrame(data)
    rows_before = len(df)
    
    # First identify outliers
    outlier_result = identify_outliers(data, column, method, threshold)
    outlier_indices_by_column: Dict[str, set] = {}
    
    for outlier in outlier_result["outliers"]:
        col = outlier["column"]
        idx = outlier["row_index"]
        if col not in outlier_indices_by_column:
            outlier_indices_by_column[col] = set()
        outlier_indices_by_column[col].add(idx)
    
    total_outliers_treated = 0
    columns_treated = list(outlier_indices_by_column.keys())
    outliers_by_column: Dict[str, int] = {}
    
    # Determine columns to treat
    if column:
        columns_to_treat = [column] if column in df.columns else []
    else:
        columns_to_treat = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
    
    # If treatment is "remove", collect all unique row indices to remove first
    # This prevents index issues when removing rows from multiple columns
    if treatment == "remove":
        all_indices_to_remove = set()
        for col in columns_to_treat:
            if col in outlier_indices_by_column:
                all_indices_to_remove.update(outlier_indices_by_column[col])
                outliers_by_column[col] = len(outlier_indices_by_column[col])
                total_outliers_treated += len(outlier_indices_by_column[col])
        
        # Remove all rows with outliers at once
        valid_indices = [idx for idx in all_indices_to_remove if idx < len(df)]
        if valid_indices:
            df = df.drop(index=valid_indices)
            # Reset index after removal to avoid issues
            df = df.reset_index(drop=True)
    else:
        # For other treatments, process columns one by one
        for col in columns_to_treat:
            if col not in outlier_indices_by_column:
                continue
            
            outlier_indices = outlier_indices_by_column[col]
            outliers_by_column[col] = len(outlier_indices)
            total_outliers_treated += len(outlier_indices)
            
            series = df[col]
            
            if treatment == "cap":
                # Cap outliers at bounds
                stats = outlier_result["statistics"][col]
                if method == "iqr":
                    lower_bound = stats["lower_bound"]
                    upper_bound = stats["upper_bound"]
                else:
                    # For zscore, use percentile-based bounds
                    lower_bound = float(series.quantile(0.01))
                    upper_bound = float(series.quantile(0.99))
                
                if treatment_value:
                    if isinstance(treatment_value, (int, float)):
                        if treatment_value < 0:  # Negative means lower percentile
                            lower_bound = float(series.quantile(abs(treatment_value) / 100))
                        else:  # Positive means upper percentile
                            upper_bound = float(series.quantile(treatment_value / 100))
                
                for idx in outlier_indices:
                    if idx < len(df):
                        if df.loc[idx, col] < lower_bound:
                            df.loc[idx, col] = lower_bound
                        elif df.loc[idx, col] > upper_bound:
                            df.loc[idx, col] = upper_bound
            
            elif treatment == "winsorize":
                # Winsorize at 1st and 99th percentiles
                lower_bound = float(series.quantile(0.01))
                upper_bound = float(series.quantile(0.99))
                
                for idx in outlier_indices:
                    if idx < len(df):
                        if df.loc[idx, col] < lower_bound:
                            df.loc[idx, col] = lower_bound
                        elif df.loc[idx, col] > upper_bound:
                            df.loc[idx, col] = upper_bound
            
            elif treatment == "transform":
                # Log transform (only for positive values)
                if (series > 0).all():
                    df[col] = np.log1p(df[col])
                else:
                    # Use square root for values that might be negative
                    df[col] = np.sign(df[col]) * np.sqrt(np.abs(df[col]))
            
            elif treatment == "impute":
                # Replace outliers with imputed value
                # Use dropna() to exclude NaN values when calculating statistics
                series_clean = series.dropna()
                
                if len(series_clean) == 0:
                    # If no valid values, skip this column
                    print(f"Warning: Column '{col}' has no valid numeric values for imputation. Skipping.")
                    continue
                
                if treatment_value == "mean":
                    mean_val = series_clean.mean()
                    if pd.isna(mean_val):
                        # Fallback to median if mean is NaN
                        impute_value = float(series_clean.median())
                    else:
                        impute_value = float(mean_val)
                elif treatment_value == "median":
                    median_val = series_clean.median()
                    if pd.isna(median_val):
                        # Fallback to 0 if median is also NaN (shouldn't happen with dropna, but just in case)
                        impute_value = 0.0
                    else:
                        impute_value = float(median_val)
                elif treatment_value == "mode":
                    mode_values = series_clean.mode()
                    if len(mode_values) > 0:
                        impute_value = float(mode_values.iloc[0])
                    else:
                        # Fallback to median
                        impute_value = float(series_clean.median())
                elif treatment_value == "min":
                    min_val = series_clean.min()
                    if pd.isna(min_val):
                        impute_value = 0.0
                    else:
                        impute_value = float(min_val)
                elif treatment_value == "max":
                    max_val = series_clean.max()
                    if pd.isna(max_val):
                        impute_value = 0.0
                    else:
                        impute_value = float(max_val)
                elif isinstance(treatment_value, (int, float)):
                    impute_value = float(treatment_value)
                else:
                    # Default to median
                    median_val = series_clean.median()
                    if pd.isna(median_val):
                        impute_value = 0.0
                    else:
                        impute_value = float(median_val)
                
                # Impute outliers with the calculated value
                for idx in outlier_indices:
                    if idx < len(df):
                        df.loc[idx, col] = impute_value
    
    rows_after = len(df)
    
    # Convert back to list of dictionaries
    result_data = df.to_dict("records")
    
    # Convert numpy types to native Python types
    for row in result_data:
        for key, value in row.items():
            if pd.isna(value):
                row[key] = None
            elif isinstance(value, (np.integer, np.floating)):
                row[key] = value.item()
            elif isinstance(value, np.ndarray):
                row[key] = value.tolist()
            elif isinstance(value, datetime):
                row[key] = value.isoformat()
    
    # Round all numeric values to 2 decimal places
    result_data = round_numeric_values_to_2_decimals(result_data)
    
    treatment_desc = f"{treatment} using {method} method"
    if treatment_value:
        treatment_desc += f" with {treatment_value}"
    
    return {
        "data": result_data,
        "rows_before": int(rows_before),
        "rows_after": int(rows_after),
        "outliers_treated": int(total_outliers_treated),
        "treatment_applied": treatment_desc,
        "summary": {
            "columns_treated": columns_treated,
            "outliers_by_column": outliers_by_column
        }
    }
