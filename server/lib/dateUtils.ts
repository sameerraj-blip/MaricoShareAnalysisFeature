/**
 * Date normalization utilities for handling flexible date formats
 * and period-based aggregations (monthly, yearly, quarterly, daily)
 */

export type DatePeriod = 'day' | 'month' | 'monthOnly' | 'quarter' | 'year';

export interface NormalizedDate {
  original: string;
  date: Date;
  period: DatePeriod;
  normalizedKey: string; // e.g., "2024-01" for month, "2024" for year
  displayLabel: string;  // e.g., "Jan 2024", "2024"
}

const MONTH_NAMES: { [key: string]: number } = {
  'jan': 0, 'january': 0,
  'feb': 1, 'february': 1,
  'mar': 2, 'march': 2,
  'apr': 3, 'april': 3,
  'may': 4,
  'jun': 5, 'june': 5,
  'jul': 6, 'july': 6,
  'aug': 7, 'august': 7,
  'sep': 8, 'september': 8, 'sept': 8,
  'oct': 9, 'october': 9,
  'nov': 10, 'november': 10,
  'dec': 11, 'december': 11
};

const MONTH_SHORT_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/**
 * Enhanced date parser that handles multiple formats:
 * - "Jan-24", "January 2024", "2024-01-15", "15/01/2024", etc.
 */
export function parseFlexibleDate(dateStr: string | Date): Date | null {
  if (!dateStr) return null;
  
  // If it's already a Date object
  if (dateStr instanceof Date && !isNaN(dateStr.getTime())) {
    return dateStr;
  }
  
  const str = String(dateStr).trim();
  if (!str) return null;
  
  // Try numeric month-year formats: "11-2020", "11/2020", "11 2020" (MM-YYYY)
  const numericMonthYearMatch = str.match(/^(\d{1,2})[-/](\d{2,4})$/);
  if (numericMonthYearMatch) {
    const part1 = parseInt(numericMonthYearMatch[1], 10);
    const part2 = parseInt(numericMonthYearMatch[2], 10);
    
    // If part2 is 2-4 digits, it's likely a year (MM-YYYY format)
    // If part2 is 2 digits and part1 <= 12, it could be MM-YY or MM-YYYY
    if (part1 >= 1 && part1 <= 12) {
      let year: number;
      if (part2 >= 100) {
        // 4-digit year (e.g., 11-2020)
        year = part2;
      } else {
        // 2-digit year (e.g., 11-20)
        // Common convention: 00-30 = 2000-2030, 31-99 = 1931-1999
        year = part2 <= 30 ? 2000 + part2 : 1900 + part2;
      }
      
      if (year >= 1900 && year <= 2100) {
        return new Date(year, part1 - 1, 1); // month is 0-indexed
      }
    }
  }
  
  // Try month-year formats: "Jan-24", "January 2024", "Jan/24", "Jan 2024", "Jan24"
  const monthYearMatch = str.match(/^([A-Za-z]{3,})[-\s/]?(\d{2,4})$/i);
  if (monthYearMatch) {
    const monthName = monthYearMatch[1].toLowerCase();
    const month = MONTH_NAMES[monthName];
    if (month !== undefined) {
      let year = parseInt(monthYearMatch[2], 10);
      if (year < 100) {
        // Common convention: 00-30 = 2000-2030, 31-99 = 1931-1999
        year = year <= 30 ? 2000 + year : 1900 + year;
      }
      if (year >= 1900 && year <= 2100) {
        return new Date(year, month, 1);
      }
    }
  }
  
  // Try standard date formats: "DD-MM-YYYY", "MM-DD-YYYY", "YYYY-MM-DD", "DD/MM/YYYY", etc.
  const dateWithSeparators = str.match(/^(\d{1,4})[-/](\d{1,2})[-/](\d{1,4})$/);
  if (dateWithSeparators) {
    const parts = dateWithSeparators;
    let year: number, month: number, day: number;
    
    // If first part is 4 digits, assume YYYY-MM-DD or YYYY/MM/DD
    if (parts[1].length === 4) {
      year = parseInt(parts[1], 10);
      month = parseInt(parts[2], 10) - 1;
      day = parseInt(parts[3], 10);
    } else {
      // Try DD-MM-YYYY or MM-DD-YYYY based on which makes sense
      const part1 = parseInt(parts[1], 10);
      const part2 = parseInt(parts[2], 10);
      const part3 = parseInt(parts[3], 10);
      
      if (part1 > 12) {
        // DD-MM-YYYY (day is > 12, so first part must be day)
        day = part1;
        month = part2 - 1;
        year = part3 < 100 ? (part3 <= 30 ? 2000 + part3 : 1900 + part3) : part3;
      } else if (part2 > 12) {
        // MM-DD-YYYY (second part > 12, so it must be day)
        month = part1 - 1;
        day = part2;
        year = part3 < 100 ? (part3 <= 30 ? 2000 + part3 : 1900 + part3) : part3;
      } else {
        // Ambiguous - default to DD-MM-YYYY (common in many regions)
        day = part1;
        month = part2 - 1;
        year = part3 < 100 ? (part3 <= 30 ? 2000 + part3 : 1900 + part3) : part3;
      }
    }
    
    if (year >= 1900 && year <= 2100 && month >= 0 && month < 12 && day >= 1 && day <= 31) {
      return new Date(year, month, day);
    }
  }
  
  // Try dot separators: "DD.MM.YYYY", "MM.DD.YYYY"
  const dateWithDots = str.match(/^(\d{1,2})\.(\d{1,2})\.(\d{2,4})$/);
  if (dateWithDots) {
    const part1 = parseInt(dateWithDots[1], 10);
    const part2 = parseInt(dateWithDots[2], 10);
    const part3 = parseInt(dateWithDots[3], 10);
    const year = part3 < 100 ? (part3 <= 30 ? 2000 + part3 : 1900 + part3) : part3;
    
    if (part1 > 12) {
      // DD.MM.YYYY
      return new Date(year, part2 - 1, part1);
    } else {
      // MM.DD.YYYY
      return new Date(year, part1 - 1, part2);
    }
  }
  
  // Try ISO format or standard Date constructor
  const date = new Date(str);
  if (!isNaN(date.getTime()) && date.getFullYear() >= 1900 && date.getFullYear() <= 2100) {
    return date;
  }
  
  return null;
}

/**
 * Normalize a date to a specific period (month, quarter, year, day)
 */
export function normalizeDateToPeriod(
  dateStr: string | Date,
  period: DatePeriod
): NormalizedDate | null {
  const date = parseFlexibleDate(dateStr);
  if (!date) return null;
  
  const originalStr = dateStr instanceof Date ? dateStr.toISOString() : String(dateStr);
  
  const year = date.getFullYear();
  const month = date.getMonth();
  const quarter = Math.floor(month / 3) + 1;
  const day = date.getDate();
  
  let normalizedKey: string;
  let displayLabel: string;
  
  switch (period) {
    case 'year':
      normalizedKey = `${year}`;
      displayLabel = `${year}`;
      break;
    case 'quarter':
      normalizedKey = `${year}-Q${quarter}`;
      displayLabel = `Q${quarter} ${year}`;
      break;
    case 'month':
      normalizedKey = `${year}-${String(month + 1).padStart(2, '0')}`;
      displayLabel = `${MONTH_SHORT_NAMES[month]} ${year}`;
      break;
    case 'monthOnly':
      // For month-only aggregation, combine all years (e.g., all Jan values)
      normalizedKey = `${String(month + 1).padStart(2, '0')}`;
      displayLabel = MONTH_SHORT_NAMES[month];
      break;
    case 'day':
      normalizedKey = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
      displayLabel = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
      break;
    default:
      return null;
  }
  
  return {
    original: originalStr,
    date,
    period,
    normalizedKey,
    displayLabel
  };
}

/**
 * Detect the period from user query (monthly, yearly, etc.)
 */
export function detectPeriodFromQuery(query: string): DatePeriod | null {
  const lowerQuery = query.toLowerCase();
  
  // Check for monthly patterns - distinguish between month-only aggregation and month-year
  // If user says "aggregated months" or "across months" without specific dates, use monthOnly
  // If user mentions specific month-year (like "Apr-24"), that's handled by extractDatesFromQuery
  const hasSpecificMonthYear = /\b([A-Za-z]{3,}[-/]?\d{2,4}|\d{1,4}[-/]\d{1,2}[-/]\d{1,4})\b/.test(lowerQuery);
  const wantsMonthOnly = /\b(aggregated?\s+months?|across\s+months?|by\s+month\s+only|month\s+only|group\s+by\s+month\s+name|months?\s+aggregated?)\b/.test(lowerQuery);
  
  // Check for month aggregation patterns first (before general monthly patterns)
  if (wantsMonthOnly && !hasSpecificMonthYear) {
    // User wants month-only aggregation (combining all years)
    return 'monthOnly';
  }
  
  if (/\b(monthly|by month|per month|each month|month-by-month|aggregate.*month|group.*month|across.*month|different month|month.*breakdown|breakdown.*month)\b/.test(lowerQuery)) {
    // General monthly pattern - use month (month-year grouping)
    return 'month';
  }
  
  // Check for yearly patterns
  if (/\b(yearly|by year|per year|each year|year-by-year|annual|annually|aggregate.*year|group.*year|across.*year|different year|year.*breakdown|breakdown.*year|aggregated.*over.*year|over.*year)\b/.test(lowerQuery)) {
    return 'year';
  }
  
  // Check for quarterly patterns
  if (/\b(quarterly|by quarter|per quarter|each quarter|quarter-by-quarter|aggregate.*quarter|group.*quarter|across.*quarter|different quarter|quarter.*breakdown|breakdown.*quarter)\b/.test(lowerQuery)) {
    return 'quarter';
  }
  
  // Check for daily patterns
  if (/\b(daily|by day|per day|each day|day-by-day|aggregate.*day|group.*day|across.*day|different day|day.*breakdown|breakdown.*day)\b/.test(lowerQuery)) {
    return 'day';
  }
  
  return null;
}

/**
 * Check if a column name suggests it's a date column
 */
export function isDateColumnName(columnName: string): boolean {
  const lower = columnName.toLowerCase();
  return /\b(date|month|week|year|time|period|day|quarter)\b/i.test(lower);
}

/**
 * Extracted date information from query
 */
export interface ExtractedDate {
  type: 'year' | 'month' | 'date' | 'dateRange';
  year?: number;
  month?: number; // 0-11 (JavaScript month index)
  monthName?: string; // Full month name
  date?: Date;
  startDate?: Date;
  endDate?: Date;
  originalText: string;
}

/**
 * Extract specific dates, months, or years from a query string
 * Returns parsed dates that can be used for filtering
 */
export function extractDatesFromQuery(query: string): ExtractedDate[] {
  const results: ExtractedDate[] = [];
  const seen = new Set<string>(); // Track duplicates
  
  // Pattern 1: Month-Year formats like "Apr-24", "Mar-22", "April 2024", "March 2022"
  const monthYearPatterns = [
    /\b([A-Za-z]{3,})[-/](\d{2,4})\b/gi,  // "Apr-24", "Mar/22"
    /\b([A-Za-z]{3,})\s+(\d{4})\b/gi,     // "April 2024", "March 2022"
  ];
  
  for (const pattern of monthYearPatterns) {
    let match;
    while ((match = pattern.exec(query)) !== null) {
      const matchText = match[0];
      if (seen.has(matchText.toLowerCase())) continue;
      seen.add(matchText.toLowerCase());
      
      const parsed = parseFlexibleDate(matchText);
      if (parsed) {
        results.push({
          type: 'month',
          year: parsed.getFullYear(),
          month: parsed.getMonth(),
          monthName: MONTH_SHORT_NAMES[parsed.getMonth()],
          date: parsed,
          originalText: matchText,
        });
      }
    }
  }
  
  // Pattern 2: Year only like "2024", "2022" (but not as part of month-year)
  const yearPattern = /\b(19|20)\d{2}\b/g;
  let match;
  while ((match = yearPattern.exec(query)) !== null) {
    const yearText = match[0];
    // Skip if it's part of a month-year pattern we already captured
    const before = query.substring(Math.max(0, match.index - 10), match.index);
    const after = query.substring(match.index + match[0].length, match.index + match[0].length + 10);
    const isPartOfMonthYear = /[A-Za-z]{3,}[-/\s]/.test(before) || /[-/\s]/.test(after);
    
    if (isPartOfMonthYear) continue;
    if (seen.has(yearText)) continue;
    seen.add(yearText);
    
    const year = parseInt(yearText, 10);
    if (year >= 1900 && year <= 2100) {
      results.push({
        type: 'year',
        year,
        originalText: yearText,
      });
    }
  }
  
  // Pattern 3: Full dates like "2024-01-15", "15/01/2024", "15-01-2024"
  const fullDatePattern = /\b(\d{1,4}[-/]\d{1,2}[-/]\d{1,4})\b/g;
  while ((match = fullDatePattern.exec(query)) !== null) {
    const matchText = match[0];
    if (seen.has(matchText)) continue;
    seen.add(matchText);
    
    const parsed = parseFlexibleDate(matchText);
    if (parsed) {
      results.push({
        type: 'date',
        date: parsed,
        year: parsed.getFullYear(),
        month: parsed.getMonth(),
        originalText: matchText,
      });
    }
  }
  
  // Pattern 4: Date ranges like "from Apr-24 to Jun-24", "between March 2022 and April 2022"
  const rangePattern = /\b(?:from|between)\s+([A-Za-z]{3,}[-/]?\d{2,4}|\d{1,4}[-/]\d{1,2}[-/]\d{1,4})\s+(?:to|and)\s+([A-Za-z]{3,}[-/]?\d{2,4}|\d{1,4}[-/]\d{1,2}[-/]\d{1,4})\b/gi;
  while ((match = rangePattern.exec(query)) !== null) {
    const rangeText = `${match[1]} to ${match[2]}`;
    if (seen.has(rangeText.toLowerCase())) continue;
    seen.add(rangeText.toLowerCase());
    
    const start = parseFlexibleDate(match[1]);
    const end = parseFlexibleDate(match[2]);
    if (start && end) {
      results.push({
        type: 'dateRange',
        startDate: start,
        endDate: end,
        originalText: rangeText,
      });
    }
  }
  
  return results;
}

