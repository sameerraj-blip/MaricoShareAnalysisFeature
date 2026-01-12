import { BaseHandler, HandlerContext, HandlerResponse } from './baseHandler.js';
import { AnalysisIntent } from '../intentClassifier.js';
import { openai } from '../../openai.js';
import { getModelForTask } from '../models.js';

/**
 * Conversational Handler
 * Handles casual chat, greetings, thanks, general questions, and non-data questions
 * Enhanced to be more ChatGPT-like with comprehensive responses
 */
export class ConversationalHandler extends BaseHandler {
  canHandle(intent: AnalysisIntent): boolean {
    return intent.type === 'conversational';
  }

  async handle(intent: AnalysisIntent, context: HandlerContext): Promise<HandlerResponse> {
    console.log('💬 ConversationalHandler processing intent:', intent.type);
    
    // Validate data (not really needed for conversational, but good practice)
    const validation = this.validateData(intent, context);
    if (!validation.valid && validation.errors.length > 0) {
      // For conversational, we can still proceed even with validation errors
      console.log('⚠️ Validation warnings (continuing anyway):', validation.warnings);
    }

    // Build comprehensive conversation history context (last 10 messages for better context)
    const recentHistory = context.chatHistory
      .slice(-10)
      .filter(msg => msg.content && msg.content.length < 1000)
      .map((msg) => `${msg.role === 'user' ? 'User' : 'Assistant'}: ${msg.content}`)
      .join('\n');

    const historyContext = recentHistory ? `\n\nCONVERSATION HISTORY:\n${recentHistory}\n\nUse this conversation history to provide context-aware, natural responses. Reference previous topics when relevant.` : '';

    // Use original question if available, otherwise customRequest, otherwise fallback
    const userMessage = intent.originalQuestion || intent.customRequest || 'something';
    console.log('💬 User message:', userMessage);
    
    // Check if this is a simple greeting/casual chat vs a general knowledge question
    const isSimpleChat = /^(hi|hello|hey|thanks|thank you|bye|goodbye|ok|okay|sure|yes|no)$/i.test(userMessage.trim());
    
    // Build response guidelines based on question type
    let responseGuidelines = '';
    if (isSimpleChat) {
      responseGuidelines = '- For simple greetings/casual responses: Keep it warm, friendly, and brief (1-2 sentences). Be enthusiastic and engaging.';
    } else {
      responseGuidelines = `- For general questions or discussions: Provide comprehensive, detailed, and helpful responses. Explain concepts clearly, provide examples when relevant, and be thorough but not overwhelming.
- Structure longer responses with clear paragraphs if needed
- Use natural, conversational language - like talking to a knowledgeable friend
- If the question relates to data analysis, you can mention your data analysis capabilities
- If it's a general knowledge question, answer it fully and accurately
- Be helpful, accurate, and engaging in all responses`;
    }
    
    // Enhanced prompt for ChatGPT-like responses
    const prompt = `You are an intelligent, helpful, and friendly AI assistant with expertise in data analysis. You're having a natural conversation with the user.

USER'S MESSAGE: "${userMessage}"
${historyContext}

YOUR CAPABILITIES:
- You're primarily a data analyst assistant, but you can also answer general questions, explain concepts, provide insights, and engage in natural conversation
- You can discuss data analysis, statistics, machine learning, business intelligence, and related topics
- You can answer general knowledge questions, explain how things work, provide advice, and have meaningful conversations
- You maintain context from previous messages and reference them naturally when relevant

RESPONSE GUIDELINES:
${responseGuidelines}

- Maintain a warm, friendly, and professional tone
- Use emojis sparingly (1-2 max, only when appropriate)
- Be conversational and natural, not robotic
- If you don't know something, say so honestly and offer to help with what you can do

Respond naturally and helpfully to the user's message.`;

    try {
      const model = getModelForTask('generation');
      
      const response = await openai.chat.completions.create({
        model,
        messages: [
          {
            role: 'system',
            content: 'You are an intelligent, helpful, and friendly AI assistant with expertise in data analysis. You can answer general questions, explain concepts, provide insights, and engage in natural, flowing conversations. You maintain context from previous messages and provide comprehensive, helpful responses when needed.',
          },
          {
            role: 'user',
            content: prompt,
          },
        ],
        temperature: 0.8, // Balanced temperature for natural but coherent responses
        max_tokens: isSimpleChat ? 150 : 800, // Longer responses for general questions
      });

      const answer = response.choices[0].message.content?.trim() || 
        "Hi! I'm here to help you. What would you like to know?";

      console.log('💬 Generated conversational response:', answer.substring(0, 200));
      
      if (!answer || answer.trim().length === 0) {
        console.error('❌ Empty answer from OpenAI, using fallback');
        throw new Error('Empty answer from OpenAI');
      }

      return {
        answer,
      };
    } catch (error) {
      console.error('Conversational response error:', error);
      
      // Fallback responses
      const userMessage = intent.originalQuestion || intent.customRequest || '';
      const questionLower = userMessage.toLowerCase();
      if (questionLower.match(/\b(hi|hello|hey)\b/)) {
        return { answer: "Hi there! 👋 I'm here to help you explore your data and answer your questions. What would you like to know?" };
      } else if (questionLower.match(/\b(thanks|thank you)\b/)) {
        return { answer: "You're welcome! Happy to help. Anything else you'd like to explore or discuss?" };
      } else if (questionLower.match(/\b(bye|goodbye)\b/)) {
        return { answer: "Goodbye! Feel free to come back if you have more questions. I'm here to help!" };
      }
      
      return {
        answer: "I'm here to help! I can assist with data analysis, answer general questions, explain concepts, and have meaningful conversations. What would you like to know?",
      };
    }
  }
}

