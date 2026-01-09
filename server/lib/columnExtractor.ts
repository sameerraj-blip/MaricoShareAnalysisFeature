/**
 * Column Extractor
 * Uses RegEx ONLY to extract column names from chat messages
 * This is the ONLY place RegEx should be used for column extraction
 */

/**
 * Extracts column names from a chat message using RegEx patterns
 * Matches against available columns from the dataset
 * 
 * @param message - The chat message to extract columns from
 * @param availableColumns - Array of available column names from the dataset
 * @returns Array of extracted column names that match available columns
 */
export function extractColumnsFromMessage(
  message: string,
  availableColumns: string[]
): string[] {
  if (!message || !availableColumns || availableColumns.length === 0) {
    return [];
  }

  const extractedColumns: string[] = [];
  const normalizedMessage = message.toLowerCase();

  // Create a map of normalized column names to original column names
  const columnMap = new Map<string, string>();
  for (const col of availableColumns) {
    const normalized = col.toLowerCase().trim();
    columnMap.set(normalized, col);
  }

  // RegEx patterns to match column names in the message
  // Pattern 1: Match quoted column names (e.g., "PA TOM", 'Revenue', `Sales`)
  const quotedPattern = /["'`]([^"'`]+)["'`]/g;
  let match;
  while ((match = quotedPattern.exec(message)) !== null) {
    const quotedText = match[1].trim();
    const normalized = quotedText.toLowerCase();
    
    // Check if it matches any available column
    if (columnMap.has(normalized)) {
      const originalColumn = columnMap.get(normalized)!;
      if (!extractedColumns.includes(originalColumn)) {
        extractedColumns.push(originalColumn);
      }
    }
  }

  // Pattern 2: Match column names that appear as standalone words
  // Look for words that match column names (case-insensitive, word boundaries)
  for (const col of availableColumns) {
    const normalizedCol = col.toLowerCase().trim();
    
    // Escape special regex characters in column name
    const escapedCol = normalizedCol.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    
    // Create regex pattern with word boundaries
    // This matches the column name as a complete word or phrase
    const wordBoundaryPattern = new RegExp(`\\b${escapedCol}\\b`, 'gi');
    
    if (wordBoundaryPattern.test(message)) {
      if (!extractedColumns.includes(col)) {
        extractedColumns.push(col);
      }
    }
  }

  // Pattern 3: Match column names that contain spaces or special characters
  // Some column names might be referenced without quotes but with specific formatting
  // Match multi-word column names that appear in the message
  for (const col of availableColumns) {
    // Skip if already extracted
    if (extractedColumns.includes(col)) {
      continue;
    }

    const normalizedCol = col.toLowerCase().trim();
    
    // For multi-word columns, try to match them as phrases
    if (normalizedCol.includes(' ') || normalizedCol.includes('_') || normalizedCol.includes('-')) {
      // Replace spaces, underscores, hyphens with optional whitespace pattern
      const flexiblePattern = normalizedCol
        .replace(/\s+/g, '\\s+')
        .replace(/_/g, '[\\s_]+')
        .replace(/-/g, '[\\s-]+');
      
      // Escape special regex characters
      const escapedPattern = flexiblePattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      
      const phrasePattern = new RegExp(escapedPattern, 'gi');
      if (phrasePattern.test(message)) {
        if (!extractedColumns.includes(col)) {
          extractedColumns.push(col);
        }
      }
    }
  }

  // Pattern 4: Match partial column names (for abbreviations or partial references)
  // Only match if the partial name is at least 3 characters and appears as a word
  for (const col of availableColumns) {
    if (extractedColumns.includes(col)) {
      continue;
    }

    const normalizedCol = col.toLowerCase().trim();
    
    // Try matching first word or abbreviation
    const firstWord = normalizedCol.split(/[\s_-]+/)[0];
    if (firstWord.length >= 3) {
      const abbreviationPattern = new RegExp(`\\b${firstWord}\\b`, 'gi');
      if (abbreviationPattern.test(message)) {
        // Additional check: make sure it's not a common word
        const commonWords = ['the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can', 'her', 'was', 'one', 'our', 'out', 'day', 'get', 'has', 'him', 'his', 'how', 'its', 'may', 'new', 'now', 'old', 'see', 'two', 'who', 'way', 'use', 'her', 'she', 'him', 'has', 'had', 'did', 'get', 'may', 'new', 'now', 'old', 'see', 'two', 'who', 'way', 'use'];
        if (!commonWords.includes(firstWord.toLowerCase())) {
          if (!extractedColumns.includes(col)) {
            extractedColumns.push(col);
          }
        }
      }
    }
  }

  return extractedColumns;
}

