/**
 * Column Matching Utility
 * Finds matching column names using fuzzy matching
 */

/**
 * Find matching column name (case-insensitive, handles spaces/underscores, partial matching)
 */
export function findMatchingColumn(searchName: string, availableColumns: string[]): string | null {
  if (!searchName) return null;
  
  // Trim whitespace from search name
  const trimmedSearch = searchName.trim();
  const normalized = trimmedSearch.toLowerCase().replace(/[\s_-]/g, '');
  
  // First try exact match (case-insensitive, ignoring spaces/underscores/dashes)
  for (const col of availableColumns) {
    const colTrimmed = col.trim(); // Trim actual column names too
    const colNormalized = colTrimmed.toLowerCase().replace(/[\s_-]/g, '');
    if (colNormalized === normalized) {
      return colTrimmed; // Return trimmed version
    }
  }
  
  // Try exact match with original column name (preserving spaces)
  for (const col of availableColumns) {
    const colTrimmed = col.trim();
    if (colTrimmed.toLowerCase() === trimmedSearch.toLowerCase()) {
      return colTrimmed;
    }
  }
  
  // Then try prefix match (search term is prefix of column name)
  for (const col of availableColumns) {
    const colTrimmed = col.trim();
    const colNormalized = colTrimmed.toLowerCase().replace(/[\s_-]/g, '');
    if (colNormalized.startsWith(normalized) && normalized.length >= 3) {
      return colTrimmed;
    }
  }
  
  // Then try partial match (search term contained in column name)
  for (const col of availableColumns) {
    const colTrimmed = col.trim();
    const colNormalized = colTrimmed.toLowerCase().replace(/[\s_-]/g, '');
    if (colNormalized.includes(normalized) && normalized.length >= 3) {
      return colTrimmed;
    }
  }
  
  // Try word-boundary matching (search term matches as a word in column name)
  const searchWords = trimmedSearch.toLowerCase().split(/\s+/).filter(w => w.length > 0);
  for (const col of availableColumns) {
    const colTrimmed = col.trim();
    const colLower = colTrimmed.toLowerCase();
    let allWordsMatch = true;
    for (const word of searchWords) {
      // Skip very short words (1-2 chars) unless they're important (like "PA", "nGRP")
      if (word.length < 2) continue;
      const wordRegex = new RegExp(`\\b${word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
      if (!wordRegex.test(colLower)) {
        allWordsMatch = false;
        break;
      }
    }
    if (allWordsMatch && searchWords.length > 0) {
      return colTrimmed;
    }
  }
  
  // Try phrase matching (exact phrase appears in column name, case-insensitive)
  // This handles cases like "PAB nGRP" matching "PAB nGRP Adstocked"
  const searchPhrase = trimmedSearch.toLowerCase();
  for (const col of availableColumns) {
    const colTrimmed = col.trim();
    const colLower = colTrimmed.toLowerCase();
    // Check if the search phrase appears as a contiguous substring
    if (colLower.includes(searchPhrase) && searchPhrase.length >= 3) {
      return colTrimmed;
    }
  }
  
  // Finally try reverse partial match (column name contained in search term)
  for (const col of availableColumns) {
    const colTrimmed = col.trim();
    const colNormalized = colTrimmed.toLowerCase().replace(/[\s_-]/g, '');
    if (normalized.includes(colNormalized) && colNormalized.length >= 3) {
      return colTrimmed;
    }
  }
  
  return null;
}

