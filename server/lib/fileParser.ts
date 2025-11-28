import { parse } from 'csv-parse/sync';
import * as XLSX from 'xlsx';
import { DataSummary } from '../shared/schema.js';

// Month name mapping for date detection
const MONTH_MAP: Record<string, number> = {
  january: 0,
  jan: 0,
  february: 1,
  feb: 1,
  march: 2,
  mar: 2,
  april: 3,
  apr: 3,
  may: 4,
  june: 5,
  jun: 5,
  july: 6,
  jul: 6,
  august: 7,
  aug: 7,
  september: 8,
  sept: 8,
  sep: 8,
  october: 9,
  oct: 9,
  november: 10,
  nov: 10,
  december: 11,
  dec: 11,
};

export async function parseFile(buffer: Buffer, filename: string): Promise<Record<string, any>[]> {
  const ext = filename.split('.').pop()?.toLowerCase();

  if (ext === 'csv') {
    return parseCsv(buffer);
  } else if (ext === 'xlsx' || ext === 'xls') {
    return parseExcel(buffer);
  } else {
    throw new Error('Unsupported file format. Please upload CSV or Excel files.');
  }
}

function parseCsv(buffer: Buffer): Record<string, any>[] {
  const content = buffer.toString('utf-8');
  const records = parse(content, {
    columns: true,
    skip_empty_lines: true,
    cast: true,
    cast_date: true,
  });
  
  // Normalize column names: trim whitespace from all column names
  return normalizeColumnNames(records as Record<string, any>[]);
}

function parseExcel(buffer: Buffer): Record<string, any>[] {
  const workbook = XLSX.read(buffer, { type: 'buffer' });
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];
  const data = XLSX.utils.sheet_to_json(worksheet, { raw: false, defval: null });
  
  // Normalize column names: trim whitespace from all column names
  return normalizeColumnNames(data as Record<string, any>[]);
}

/**
 * Normalizes column names by trimming whitespace from all keys
 * This ensures consistent column name handling throughout the application
 */
function normalizeColumnNames(data: Record<string, any>[]): Record<string, any>[] {
  if (!data || data.length === 0) {
    return data;
  }
  
  // Create a mapping of old column names to normalized (trimmed) names
  const firstRow = data[0];
  const columnMapping: Record<string, string> = {};
  
  for (const oldKey of Object.keys(firstRow)) {
    const normalizedKey = oldKey.trim();
    if (oldKey !== normalizedKey) {
      columnMapping[oldKey] = normalizedKey;
    }
  }
  
  // If no normalization needed, return as-is
  if (Object.keys(columnMapping).length === 0) {
    return data;
  }
  
  // Remap all rows to use normalized column names
  return data.map(row => {
    const normalizedRow: Record<string, any> = {};
    for (const [oldKey, value] of Object.entries(row)) {
      const newKey = columnMapping[oldKey] || oldKey.trim();
      normalizedRow[newKey] = value;
    }
    return normalizedRow;
  });
}

/**
 * Comprehensive date detection function that handles multiple date formats:
 * - Month-Year: "Apr-24", "April 2024", "Jan-2024", "Mar/24"
 * - Standard dates: "DD-MM-YYYY", "MM-DD-YYYY", "YYYY-MM-DD", "DD/MM/YYYY"
 * - Dot separators: "DD.MM.YYYY", "MM.DD.YYYY"
 * - Month names with day: "April 15, 2024", "15 April 2024"
 * - Date objects and timestamps
 */
function isDateValue(value: any): boolean {
  if (value === null || value === undefined || value === '') return false;
  
  // If it's already a Date object, it's a date
  if (value instanceof Date && !isNaN(value.getTime())) return true;
  
  const str = String(value).trim();
  if (!str) return false;
  
  // Check for month-year formats: "Apr-24", "April 2024", "Jan-2024", "Mar/24", etc.
  const mmmYyMatch = str.match(/^([A-Za-z]{3,})[-\s/]?(\d{2,4})$/i);
  if (mmmYyMatch) {
    const monthName = mmmYyMatch[1].toLowerCase().substring(0, 3);
    if (MONTH_MAP[monthName] !== undefined) {
      const year = parseInt(mmmYyMatch[2], 10);
      // Validate year is reasonable (1900-2100)
      if (year >= 0 && year <= 2100) {
        return true;
      }
    }
  }
  
  // Check for date formats with separators: "DD-MM-YYYY", "MM-DD-YYYY", "YYYY-MM-DD", etc.
  const dateWithSeparators = str.match(/^\d{1,4}[-/]\d{1,2}[-/]\d{1,4}$/);
  if (dateWithSeparators) {
    const date = new Date(str);
    if (!isNaN(date.getTime())) {
      // Additional validation: check if the parsed date components make sense
      const parts = str.split(/[-/]/);
      if (parts.length === 3) {
        const [part1, part2, part3] = parts.map(p => parseInt(p, 10));
        // Check if month is valid (1-12) and day is valid (1-31)
        if ((part1 >= 1 && part1 <= 12 && part2 >= 1 && part2 <= 31) ||
            (part2 >= 1 && part2 <= 12 && part1 >= 1 && part1 <= 31)) {
          return true;
        }
        // Check if it's YYYY-MM-DD format
        if (part1 >= 1900 && part1 <= 2100 && part2 >= 1 && part2 <= 12 && part3 >= 1 && part3 <= 31) {
          return true;
        }
      }
    }
  }
  
  // Check for formats like "DD.MM.YYYY" or "MM.DD.YYYY"
  const dateWithDots = str.match(/^\d{1,2}\.\d{1,2}\.\d{4}$/);
  if (dateWithDots) {
    const date = new Date(str.replace(/\./g, '-'));
    if (!isNaN(date.getTime())) return true;
  }
  
  // Check for month name with day and year: "April 15, 2024", "15 April 2024", "Apr 15 2024", etc.
  const monthNameWithDay = str.match(/\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b/i);
  if (monthNameWithDay) {
    const date = new Date(str);
    if (!isNaN(date.getTime())) {
      // Additional check: make sure it actually contains a year
      if (str.match(/\d{4}/)) {
        return true;
      }
    }
  }
  
  // Check for formats like "YYYYMMDD" (8 digits)
  const compactDate = str.match(/^\d{8}$/);
  if (compactDate) {
    const year = parseInt(str.substring(0, 4), 10);
    const month = parseInt(str.substring(4, 6), 10);
    const day = parseInt(str.substring(6, 8), 10);
    if (year >= 1900 && year <= 2100 && month >= 1 && month <= 12 && day >= 1 && day <= 31) {
      return true;
    }
  }
  
  // Try native Date parsing as fallback
  const date = new Date(str);
  if (!isNaN(date.getTime())) {
    // Additional validation: reject if it's just a number that happens to parse as a date
    if (str.match(/^\d+$/)) {
      // If it's just digits, be more strict - only accept if it's a reasonable timestamp
      const num = parseInt(str, 10);
      // Accept if it's a reasonable Unix timestamp (between 1970 and 2100)
      // Milliseconds: 0 to 4102444800000 (Jan 1, 2100)
      // Seconds: 0 to 4102444800
      if (num > 0) {
        // Check if it's milliseconds (13+ digits) or seconds (10 digits)
        if (str.length >= 13) {
          return num < 4102444800000; // Max timestamp for year 2100 in milliseconds
        } else if (str.length === 10) {
          return num < 4102444800; // Max timestamp for year 2100 in seconds
        }
      }
      return false;
    }
    // For non-numeric strings, check if the parsed date is reasonable
    const year = date.getFullYear();
    if (year >= 1900 && year <= 2100) {
      return true;
    }
  }
  
  return false;
}

export function createDataSummary(data: Record<string, any>[]): DataSummary {
  if (data.length === 0) {
    throw new Error('No data found in file');
  }

  const columns = Object.keys(data[0]);
  const numericColumns: string[] = [];
  const dateColumns: string[] = [];

  const columnInfo = columns.map((col) => {
    // Check more rows for better date detection (up to 1000 rows or all rows if less)
    // This ensures we catch date columns even if they're not in the first 100 rows
    const sampleSize = Math.min(data.length, 1000);
    const values = data.slice(0, sampleSize).map((row) => row[col]);
    const nonNullValues = values.filter((v) => v !== null && v !== undefined && v !== '');

    // Determine column type
    let type = 'string';
    
    // Check if numeric (handle percentages by stripping % symbol)
    // Only consider numeric if we have values and all are numeric
    const isNumeric = nonNullValues.length > 0 && nonNullValues.every((v) => {
      if (v === '') return false;
      // Strip % symbol and commas for numeric check
      const cleaned = String(v).replace(/[%,]/g, '').trim();
      return !isNaN(Number(cleaned)) && cleaned !== '';
    });
    
    // Use comprehensive date detection
    // Consider it a date column if at least 50% of non-null values are dates
    // This handles cases where some rows might have invalid dates or mixed data
    const dateMatches = nonNullValues.filter((v) => isDateValue(v)).length;
    const dateThreshold = Math.max(1, Math.ceil(nonNullValues.length * 0.5));
    const isDate = nonNullValues.length > 0 && dateMatches >= dateThreshold;

    if (isNumeric) {
      type = 'number';
      numericColumns.push(col);
    } else if (isDate) {
      type = 'date';
      dateColumns.push(col);
    }

    // Serialize sample values to primitives (convert Date objects to strings)
    const sampleValues = values.slice(0, 3).map((v) => {
      if (v instanceof Date) {
        return v.toISOString();
      }
      return v;
    });

    return {
      name: col,
      type,
      sampleValues,
    };
  });

  return {
    rowCount: data.length,
    columnCount: columns.length,
    columns: columnInfo,
    numericColumns,
    dateColumns,
  };
}
