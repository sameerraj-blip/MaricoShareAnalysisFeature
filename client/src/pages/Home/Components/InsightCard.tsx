import { useState } from 'react';
import { Insight } from '@/shared/schema';
import { Card } from '@/components/ui/card';
import { Lightbulb, ChevronDown, ChevronUp } from 'lucide-react';

interface InsightCardProps {
  insights: Insight[];
}

// Function to clean orphaned characters (asterisks and hyphens) that aren't part of markdown
const cleanOrphanedAsterisks = (text: string): string => {
  // Protect valid markdown patterns
  const placeholders: { [key: string]: string } = {};
  let placeholderCounter = 0;
  
  // Protect **bold** patterns
  let cleaned = text.replace(/\*\*(.*?)\*\*/g, (match) => {
    const key = `__BOLD_${placeholderCounter}__`;
    placeholders[key] = match;
    placeholderCounter++;
    return key;
  });
  
  // Protect *italic* patterns
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
  
  // Remove orphaned asterisks:
  // 1. Remove asterisks at the end of lines
  cleaned = cleaned.replace(/\s*\*\s*$/gm, '');
  // 2. Remove asterisks after periods/full stops
  cleaned = cleaned.replace(/\.\s*\*\s+/g, '. ');
  // 3. Remove standalone asterisks (surrounded by spaces)
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
};

// Function to parse text and format bold sections
const parseInsightText = (text: string) => {
  // First clean orphaned asterisks
  const cleanedText = cleanOrphanedAsterisks(text);
  
  // Split by ** to identify bold sections
  const parts = cleanedText.split(/(\*\*[^*]+\*\*)/g);
  
  return parts.map((part, index) => {
    if (part.startsWith('**') && part.endsWith('**')) {
      // This is a bold section - remove the ** and make it darker
      const boldText = part.slice(2, -2);
      return (
        <span key={index} className="font-semibold text-gray-800 dark:text-gray-200">
          {boldText}
        </span>
      );
    }
    return part;
  });
};

// Function to break down insight into sub-points
const parseInsightSubPoints = (text: string) => {
  // Split by ** sections to create sub-points
  const sections = text.split(/(\*\*[^*]+\*\*)/g);
  const subPoints: string[] = [];
  
  let currentPoint = '';
  
  sections.forEach((section) => {
    if (section.startsWith('**') && section.endsWith('**')) {
      // If we have accumulated text, add it as a sub-point
      if (currentPoint.trim()) {
        subPoints.push(currentPoint.trim());
        currentPoint = '';
      }
      // Start new sub-point with the bold section
      currentPoint = section;
    } else {
      // Add to current sub-point
      currentPoint += section;
    }
  });
  
  // Add the last sub-point if there's any content
  if (currentPoint.trim()) {
    subPoints.push(currentPoint.trim());
  }
  
  return subPoints;
};

export function InsightCard({ insights }: InsightCardProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  
  if (!insights || insights.length === 0) return null;

  const INITIAL_DISPLAY_COUNT = 3;
  const hasMoreInsights = insights.length > INITIAL_DISPLAY_COUNT;
  const displayedInsights = isExpanded ? insights : insights.slice(0, INITIAL_DISPLAY_COUNT);
  const hiddenCount = insights.length - INITIAL_DISPLAY_COUNT;

  return (
    <Card className="bg-primary/5 border-l-4 border-l-primary shadow-sm" data-testid="insight-card">
      <div className="p-6">
        <div className="flex items-center gap-2 mb-4">
          <Lightbulb className="w-5 h-5 text-primary" />
          <h3 className="text-lg font-semibold text-foreground">Key Insights</h3>
          {hasMoreInsights && (
            <span className="text-xs text-muted-foreground ml-auto">
              {insights.length} insights
            </span>
          )}
        </div>
        <ul className="space-y-4">
          {displayedInsights.map((insight) => {
            const subPoints = parseInsightSubPoints(insight.text);
            
            return (
              <li key={insight.id} className="space-y-2" data-testid={`insight-${insight.id}`}>
                <div className="flex gap-3">
                  <span className="flex-shrink-0 w-6 h-6 rounded-full bg-primary text-primary-foreground text-sm font-medium flex items-center justify-center mt-0.5">
                    {insight.id}
                  </span>
                  <div className="flex-1 space-y-2">
                    {subPoints.map((subPoint, subIndex) => (
                      <div key={subIndex} className="text-sm text-foreground leading-relaxed">
                        {parseInsightText(subPoint)}
                      </div>
                    ))}
                  </div>
                </div>
              </li>
            );
          })}
        </ul>
        {hasMoreInsights && (
          <button
            onClick={() => setIsExpanded(!isExpanded)}
            className="mt-4 w-full flex items-center justify-center gap-2 text-sm text-primary hover:text-primary/80 transition-colors py-2 border-t border-primary/10"
          >
            {isExpanded ? (
              <>
                <ChevronUp className="w-4 h-4" />
                Show less
              </>
            ) : (
              <>
                <ChevronDown className="w-4 h-4" />
                Show {hiddenCount} more insight{hiddenCount > 1 ? 's' : ''}
              </>
            )}
          </button>
        )}
      </div>
    </Card>
  );
}
