import { openai, MODEL } from './openai.js';
import { Message, DataSummary } from '../shared/schema.js';

export async function generateAISuggestions(
  chatHistory: Message[],
  dataSummary: DataSummary,
  lastAnswer?: string
): Promise<string[]> {
  const lastMessages = chatHistory.slice(-4); // Get last 4 messages for context
  const conversationContext = lastMessages
    .map(m => `${m.role === 'user' ? 'User' : 'Assistant'}: ${m.content.substring(0, 200)}`)
    .join('\n');

  const prompt = `You are a helpful data analyst assistant. Based on the conversation history and data context, generate 3-4 concise, actionable follow-up questions that would be natural next steps for the user to ask.

CONVERSATION CONTEXT:
${conversationContext || 'This is the initial analysis of a newly uploaded dataset.'}

${lastAnswer ? `LAST ASSISTANT RESPONSE:\n${lastAnswer.substring(0, 500)}\n` : ''}

AVAILABLE DATA COLUMNS:
- Numeric columns: ${dataSummary.numericColumns.slice(0, 15).join(', ')}${dataSummary.numericColumns.length > 15 ? ` (and ${dataSummary.numericColumns.length - 15} more)` : ''}
- Date columns: ${dataSummary.dateColumns.slice(0, 5).join(', ')}${dataSummary.dateColumns.length > 5 ? ` (and ${dataSummary.dateColumns.length - 5} more)` : ''}
- All columns: ${dataSummary.columns.map(c => c.name).slice(0, 20).join(', ')}${dataSummary.columns.length > 20 ? ` (and ${dataSummary.columns.length - 20} more)` : ''}
- Total: ${dataSummary.rowCount} rows, ${dataSummary.columnCount} columns

GUIDELINES:
- Generate questions that are relevant to the current conversation OR the dataset structure
- Make them specific and actionable using actual column names from the dataset (e.g., "What affects ${dataSummary.numericColumns[0] || 'sales'}?" not "What affects revenue?")
- Vary the question types (correlation, trends, comparisons, top performers, etc.)
- Keep each question under 12 words
- If no conversation history (initial upload), suggest exploratory questions based on the actual column names
- Use the actual column names from the dataset - be specific!
- Focus on the most interesting or relevant columns from the dataset

Output JSON only:
{
  "suggestions": ["question 1", "question 2", "question 3", "question 4"]
}`;

  try {
    const response = await openai.chat.completions.create({
      model: MODEL as string,
      messages: [
        {
          role: 'system',
          content: 'You are a helpful data analyst assistant. Generate concise, actionable follow-up questions based on conversation context. Output valid JSON only.',
        },
        {
          role: 'user',
          content: prompt,
        },
      ],
      response_format: { type: 'json_object' },
      temperature: 0.7,
      max_tokens: 200,
    });

    const content = response.choices[0].message.content || '{}';
    const parsed = JSON.parse(content);
    
    if (Array.isArray(parsed.suggestions) && parsed.suggestions.length > 0) {
      return parsed.suggestions.slice(0, 4); // Return max 4 suggestions
    }
  } catch (error) {
    console.error('Failed to generate AI suggestions:', error);
  }

  // Fallback to default suggestions
  return getDefaultSuggestions(dataSummary);
}

function getDefaultSuggestions(summary: DataSummary): string[] {
  if (summary.numericColumns.length > 0) {
    return [
      `What affects ${summary.numericColumns[0]}?`,
      `Show me trends for ${summary.numericColumns[0]}`,
      `What are the top performers?`,
      'Analyze correlations in the data'
    ];
  }
  return [
    'Show me trends over time',
    'What are the top performers?',
    'Analyze the data',
    'What patterns do you see?'
  ];
}

