"""Data operations using pandas"""
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Literal
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
                numeric_converted = pd.to_numeric(coerced, errors="coerce")
                
                # If we successfully converted a good portion of the data, use numeric column
                non_null_original = df[col].notna().sum()
                if non_null_original > 0:
                    successful_conversions = numeric_converted.notna().sum()
                    conversion_rate = successful_conversions / non_null_original
                    if conversion_rate >= 0.7:
                        df[col] = numeric_converted
            
            null_count = df[col].isna().sum()
            if null_count == 0:
                continue
            
            if method == "mean":
                if pd.api.types.is_numeric_dtype(df[col]):
                    mean_value = df[col].mean()
                    # Check if mean is NaN (happens when all values are null)
                    if pd.isna(mean_value):
                        # If all values are null, use 0 as default for numeric columns
                        fill_value = 0.0
                    else:
                        # Round mean to 2 decimal places
                        fill_value = round(float(mean_value), 2)
                    df[col] = df[col].fillna(fill_value)
                else:
                    # For non-numeric, fall back to mode
                    mode_value = df[col].mode()
                    fill_value = mode_value[0] if len(mode_value) > 0 else ""
                    df[col] = df[col].fillna(fill_value)
            elif method == "median":
                if pd.api.types.is_numeric_dtype(df[col]):
                    median_value = df[col].median()
                    # Check if median is NaN (happens when all values are null)
                    if pd.isna(median_value):
                        # If all values are null, use 0 as default for numeric columns
                        fill_value = 0.0
                    else:
                        # Round median to 2 decimal places
                        fill_value = round(float(median_value), 2)
                    df[col] = df[col].fillna(fill_value)
                else:
                    mode_value = df[col].mode()
                    fill_value = mode_value[0] if len(mode_value) > 0 else ""
                    df[col] = df[col].fillna(fill_value)
            elif method == "mode":
                mode_value = df[col].mode()
                if len(mode_value) > 0:
                    fill_value = mode_value[0]
                    # If mode is numeric, round to 2 decimal places
                    if pd.api.types.is_numeric_dtype(df[col]):
                        try:
                            fill_value = round(float(fill_value), 2)
                        except (ValueError, TypeError):
                            # If conversion fails, use original value
                            pass
                    df[col] = df[col].fillna(fill_value)
                else:
                    # If mode is empty (all values are null), use 0 for numeric, empty string for non-numeric
                    if pd.api.types.is_numeric_dtype(df[col]):
                        fill_value = 0.0
                    else:
                        fill_value = ""
                    df[col] = df[col].fillna(fill_value)
            elif method == "custom":
                # Round custom value to 2 decimal places if it's numeric
                if custom_value is not None:
                    try:
                        if isinstance(custom_value, (int, float)) or (isinstance(custom_value, str) and custom_value.replace('.', '', 1).replace('-', '', 1).isdigit()):
                            custom_value_rounded = round(float(custom_value), 2)
                            df[col] = df[col].fillna(custom_value_rounded)
                        else:
                            df[col] = df[col].fillna(custom_value)
                    except (ValueError, TypeError):
                        # If conversion fails, use original value
                        df[col] = df[col].fillna(custom_value)
                else:
                    df[col] = df[col].fillna(custom_value)
            
            nulls_removed += int(null_count)
    
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
    Create a new column from an expression involving existing columns.
    
    Args:
        data: List of dictionaries representing rows
        new_column_name: Name of the new column to create
        expression: Expression like "[Column1] + [Column2]" or "[Column1] * [Column2]"
    
    Returns:
        Dictionary with:
            - data: Modified data with new column
            - errors: List of error messages if any
    """
    df = pd.DataFrame(data)
    errors = []
    
    # Parse expression - replace [ColumnName] with df['ColumnName']
    # Handle basic operations: +, -, *, /, and parentheses
    try:
        # Extract column names from expression (format: [ColumnName])
        import re
        column_pattern = r'\[([^\]]+)\]'
        column_matches = re.findall(column_pattern, expression)
        
        if not column_matches:
            errors.append(f"No column references found in expression. Use format [ColumnName]")
            return {
                "data": data,
                "errors": errors
            }
        
        # Verify all columns exist
        missing_columns = [col for col in column_matches if col not in df.columns]
        if missing_columns:
            errors.append(f"Columns not found: {', '.join(missing_columns)}")
            return {
                "data": data,
                "errors": errors
            }
        
        # Convert columns to numeric first (handle strings with commas, percentages, etc.)
        for col in column_matches:
            if col in df.columns:
                # If already numeric, skip conversion
                if pd.api.types.is_numeric_dtype(df[col]):
                    continue
                
                # Convert to string first, then clean and convert to numeric
                # Handle commas, dollar signs, percentages, and other formatting
                df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                df[col] = df[col].str.replace('$', '', regex=False)
                df[col] = df[col].str.replace('%', '', regex=False)
                df[col] = df[col].str.strip()
                
                # Convert to numeric, coercing errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Replace [ColumnName] with df['ColumnName'] for evaluation
        eval_expression = expression
        for col in column_matches:
            # Escape column name for pandas (handle special characters)
            safe_col = f"df['{col}']"
            eval_expression = eval_expression.replace(f'[{col}]', safe_col)
        
        # Evaluate the expression
        try:
            df[new_column_name] = pd.eval(eval_expression)
        except Exception as e:
            # Fallback: try with numpy operations
            try:
                # Replace df['Column'] with just the column reference for numpy
                numpy_expr = expression
                for col in column_matches:
                    numpy_expr = numpy_expr.replace(f'[{col}]', f"df['{col}']")
                df[new_column_name] = eval(numpy_expr)
            except Exception as e2:
                errors.append(f"Error evaluating expression: {str(e2)}")
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
            "errors": errors
        }
        
    except Exception as e:
        errors.append(f"Error creating derived column: {str(e)}")
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
    Convert column to specified data type.
    
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
    
    original_dtype = str(df[column].dtype)
    conversion_info = {
        "column": column,
        "original_type": original_dtype,
        "target_type": target_type,
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

