import { AnalysisIntent } from '../intentClassifier.js';
import { DataSummary } from '../../../shared/schema.js';
import { openai } from '../../openai.js';
import { getModelForTask } from '../models.js';

const MIN_CONFIDENCE = 0.5;

/**
 * Check if clarification is needed
 */
export function shouldAskClarification(intent: AnalysisIntent): boolean {
  return intent.confidence < MIN_CONFIDENCE || intent.requiresClarification === true;
}

/**
 * Ask clarifying question to user
 */
export async function askClarifyingQuestion(
  intent: AnalysisIntent,
  summary: DataSummary
): Promise<{ answer: string; charts?: never[]; insights?: never[] }> {
  const allColumns = summary.columns.map(c => c.name).join(', ');
  const numericColumns = summary.numericColumns.slice(0, 10).join(', ');
  
  const prompt = `The user asked a question that I'm not entirely sure about. Generate a helpful clarifying question.

USER QUESTION: ${intent.customRequest || 'Unknown'}
INTENT TYPE: ${intent.type}
CONFIDENCE: ${intent.confidence.toFixed(2)}

AVAILABLE DATA:
- ${summary.rowCount} rows, ${summary.columnCount} columns
- Numeric columns: ${numericColumns}${summary.numericColumns.length > 10 ? '...' : ''}

Generate a friendly, helpful clarifying question that:
1. Acknowledges uncertainty
2. Suggests specific things they can ask about
3. Shows available columns/options
4. Is conversational and helpful

Keep it SHORT (2-3 sentences max).`;

  try {
    const model = getModelForTask('generation');
    
    const response = await openai.chat.completions.create({
      model,
      messages: [
        {
          role: 'system',
          content: 'You are a helpful data analyst assistant. Generate friendly clarifying questions when you need more information.',
        },
        {
          role: 'user',
          content: prompt,
        },
      ],
      temperature: 0.7,
      max_tokens: 150,
    });

    const answer = response.choices[0].message.content?.trim() || 
      `I'm not entirely sure what you're asking. Could you rephrase? I can help you analyze: ${numericColumns}${summary.numericColumns.length > 10 ? '...' : ''}`;

    return { answer };
  } catch (error) {
    console.error('Error generating clarifying question:', error);
    
    // Fallback clarifying question
    const suggestions = [
      `What affects ${summary.numericColumns[0] || 'the data'}?`,
      `Show me trends in the data`,
      `Analyze correlations`,
    ];
    
    return {
      answer: `I'm not entirely sure what you're asking. Could you rephrase? Here are some things I can help with:\n\n${suggestions.map(s => `- ${s}`).join('\n')}`,
    };
  }
}

