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

  // Build column context for better suggestions
  const allColumnNames = dataSummary.columns.map(c => c.name).slice(0, 20).join(', ');
  const columnTypes = dataSummary.columns.slice(0, 10).map(c => `- ${c.name} (${c.datatype})`).join('\n');
  
  const prompt = `You are a helpful data analyst assistant. Based on the conversation history and data context, generate 3-4 concise, actionable follow-up questions that would be natural next steps for the user to ask.

${conversationContext ? `CONVERSATION CONTEXT:\n${conversationContext}\n` : 'NO CONVERSATION HISTORY - This is a new dataset upload. Generate initial exploratory questions based on the data structure.\n'}

${lastAnswer ? `LAST ASSISTANT RESPONSE:\n${lastAnswer.substring(0, 500)}\n` : ''}

AVAILABLE DATA COLUMNS:
${columnTypes}
- Total columns: ${dataSummary.columns.length}
- Numeric columns: ${dataSummary.numericColumns.slice(0, 10).join(', ')}
- Date columns: ${dataSummary.dateColumns.slice(0, 5).join(', ')}

${!conversationContext ? `IMPORTANT: Since this is a new dataset, generate questions that:
- Are specific to the actual column names in the dataset (use exact column names from the list above)
- Explore relationships between columns (e.g., "What affects [column name]?" where [column name] is from the numeric columns)
- Ask about trends over time if date columns exist
- Compare different columns or ask about top performers
- Make questions relevant to the domain (e.g., if columns contain "nGRP", "TOM", "Adstocked", these might be marketing/media metrics)
- Avoid generic questions like "What affects revenue?" if "revenue" is not in the column names
` : ''}

GUIDELINES:
- Generate questions that are relevant to the current conversation${!conversationContext ? ' and the actual data structure' : ''}
- Make them specific and actionable (e.g., "What affects ${dataSummary.numericColumns[0] || 'the data'}?" not "Tell me more")
- Use actual column names from the dataset when possible
- Vary the question types (correlation, trends, comparisons, etc.)
- Keep each question under 10 words
- Focus on the most interesting or relevant columns mentioned

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

