import React from 'react';

/**
 * Simple markdown renderer for chat messages
 * Handles **bold**, *italic*, and line breaks
 * Removes orphaned asterisks that aren't part of markdown formatting
 */
export function MarkdownRenderer({ content }: { content: string }) {
  // Clean up orphaned asterisks (standalone * that aren't part of **bold** or *italic*)
  const cleanedContent = cleanOrphanedAsterisks(content);
  
  // Split by lines to handle line breaks
  const lines = cleanedContent.split('\n');
  
  return (
    <div className="markdown-content">
      {lines.map((line, lineIndex) => {
        const parts = parseMarkdownLine(line, lineIndex);
        
        return (
          <React.Fragment key={lineIndex}>
            {parts}
            {lineIndex < lines.length - 1 && <br />}
          </React.Fragment>
        );
      })}
    </div>
  );
}

/**
 * Remove orphaned characters (asterisks and hyphens) that aren't part of markdown formatting
 * Orphaned characters are:
 * - Single * at the end of lines/sentences (not part of **bold** or *italic*)
 * - Single - at the end of lines/sentences (not part of ranges like "24.0-41.0")
 */
function cleanOrphanedAsterisks(text: string): string {
  // First, protect valid markdown patterns by replacing them with placeholders
  const placeholders: { [key: string]: string } = {};
  let placeholderCounter = 0;
  
  // Protect **bold** patterns
  let cleaned = text.replace(/\*\*(.*?)\*\*/g, (match) => {
    const key = `__BOLD_${placeholderCounter}__`;
    placeholders[key] = match;
    placeholderCounter++;
    return key;
  });
  
  // Protect *italic* patterns (but only if they're not part of **bold**)
  cleaned = cleaned.replace(/\*([^*\n]+?)\*/g, (match) => {
    const key = `__ITALIC_${placeholderCounter}__`;
    placeholders[key] = match;
    placeholderCounter++;
    return key;
  });
  
  // Protect number ranges (e.g., "24.0-41.0", "907-1258") to avoid removing valid hyphens
  cleaned = cleaned.replace(/(\d+\.?\d*)\s*-\s*(\d+\.?\d*)/g, (match) => {
    const key = `__RANGE_${placeholderCounter}__`;
    placeholders[key] = match;
    placeholderCounter++;
    return key;
  });
  
  // Now remove orphaned asterisks:
  // 1. Remove asterisks at the end of lines (with optional whitespace before)
  cleaned = cleaned.replace(/\s*\*\s*$/gm, '');
  // 2. Remove asterisks after periods/full stops
  cleaned = cleaned.replace(/\.\s*\*\s+/g, '. ');
  // 3. Remove asterisks that are standalone (surrounded by spaces or at line end)
  cleaned = cleaned.replace(/\s+\*\s+/g, ' ');
  cleaned = cleaned.replace(/\s+\*$/gm, '');
  
  // Remove orphaned hyphens:
  // 1. Remove hyphens at the end of lines (with whitespace before) - handles "text -" at end
  cleaned = cleaned.replace(/\s+-\s*$/gm, '');
  // 2. Remove hyphens after periods/full stops followed by space(s) - handles "text. -"
  cleaned = cleaned.replace(/\.\s+-\s*/g, '. ');
  // 3. Remove hyphens that appear standalone after sentences (period + space + hyphen at end)
  cleaned = cleaned.replace(/\.\s+-\s*$/gm, '.');
  // 4. Remove hyphens followed by space and newline or end of string
  cleaned = cleaned.replace(/-\s+$/gm, '');
  
  // Restore protected markdown patterns and ranges
  Object.keys(placeholders).forEach((key) => {
    cleaned = cleaned.replace(key, placeholders[key]);
  });
  
  return cleaned;
}

/**
 * Parse a line of markdown and return React nodes
 * Handles **bold** and *italic* (but prioritizes **bold** over *italic*)
 */
function parseMarkdownLine(line: string, baseKey: number): React.ReactNode[] {
  const parts: React.ReactNode[] = [];
  let keyCounter = baseKey * 1000;
  let remaining = line;
  let position = 0;
  
  // First, process bold text (**text**) - this takes priority
  const boldRegex = /\*\*(.*?)\*\*/g;
  const boldMatches: Array<{ start: number; end: number; text: string }> = [];
  let match;
  
  while ((match = boldRegex.exec(line)) !== null) {
    boldMatches.push({
      start: match.index,
      end: match.index + match[0].length,
      text: match[1],
    });
  }
  
  // Process the line, handling bold sections and regular text
  let lastIndex = 0;
  
  for (const boldMatch of boldMatches) {
    // Add text before the bold
    if (boldMatch.start > lastIndex) {
      const beforeText = line.substring(lastIndex, boldMatch.start);
      if (beforeText) {
        parts.push(...parseInlineMarkdown(beforeText, keyCounter++));
      }
    }
    
    // Add bold text
    parts.push(
      <strong key={keyCounter++} className="font-semibold">
        {boldMatch.text}
      </strong>
    );
    
    lastIndex = boldMatch.end;
  }
  
  // Add remaining text after last bold
  if (lastIndex < line.length) {
    const afterText = line.substring(lastIndex);
    if (afterText) {
      parts.push(...parseInlineMarkdown(afterText, keyCounter++));
    }
  }
  
  // If no bold was found, process the whole line for italic
  if (parts.length === 0) {
    parts.push(...parseInlineMarkdown(line, keyCounter++));
  }
  
  return parts;
}

/**
 * Parse inline markdown (italic) - only processes text that's not already bold
 * Since bold (**text**) is processed first, we can safely look for single asterisks
 */
function parseInlineMarkdown(text: string, baseKey: number): React.ReactNode[] {
  // If text is empty, return empty array
  if (!text) {
    return [];
  }
  
  // If text contains **, it means there might be bold markers we missed - skip italic processing
  // (This shouldn't happen since we process bold first, but just in case)
  if (text.includes('**')) {
    return [text];
  }
  
  const parts: React.ReactNode[] = [];
  let keyCounter = baseKey * 1000;
  
  // Process italic text (*text*) - find single asterisks
  // Since we've already processed bold, remaining asterisks should be italic
  const italicRegex = /\*([^*\n]+?)\*/g;
  let lastIndex = 0;
  let match;
  
  while ((match = italicRegex.exec(text)) !== null) {
    // Add text before the italic
    if (match.index > lastIndex) {
      const beforeText = text.substring(lastIndex, match.index);
      if (beforeText) {
        parts.push(beforeText);
      }
    }
    
    // Add italic text
    parts.push(
      <em key={keyCounter++} className="italic">
        {match[1]}
      </em>
    );
    
    lastIndex = italicRegex.lastIndex;
  }
  
  // Add remaining text
  if (lastIndex < text.length) {
    parts.push(text.substring(lastIndex));
  }
  
  // If no italic was found, return the text as-is
  if (parts.length === 0) {
    return [text];
  }
  
  return parts;
}

