"""Data operations using pandas"""
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Literal
from datetime import datetime


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
                
            null_count = df[col].isna().sum()
            if null_count == 0:
                continue
            
            if method == "mean":
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].mean())
                else:
                    # For non-numeric, use mode
                    mode_value = df[col].mode()
                    df[col] = df[col].fillna(mode_value[0] if len(mode_value) > 0 else "")
            elif method == "median":
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].median())
                else:
                    mode_value = df[col].mode()
                    df[col] = df[col].fillna(mode_value[0] if len(mode_value) > 0 else "")
            elif method == "mode":
                mode_value = df[col].mode()
                df[col] = df[col].fillna(mode_value[0] if len(mode_value) > 0 else "")
            elif method == "custom":
                df[col] = df[col].fillna(custom_value)
            
            nulls_removed += null_count
    
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
    
    return {
        "data": result_data,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "nulls_removed": nulls_removed
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
        dtype = str(col_data.dtype)
        
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
        
        # Numeric statistics
        if pd.api.types.is_numeric_dtype(col_data):
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
                row[key] = value.item()
            elif isinstance(value, np.ndarray):
                row[key] = value.tolist()
            elif isinstance(value, datetime):
                row[key] = value.isoformat()
    
    return {
        "data": result_data,
        "conversion_info": conversion_info
    }

