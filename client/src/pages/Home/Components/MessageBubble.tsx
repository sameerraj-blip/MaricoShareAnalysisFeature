import { forwardRef, useState } from 'react';
import { Message, ThinkingStep } from '@/shared/schema';
import { User, Bot, Edit2, Check, X as XIcon } from 'lucide-react';
import { ChartRenderer } from './ChartRenderer';
import { InsightCard } from './InsightCard';
import { DataPreview } from './DataPreview';
import { DataPreviewTable, DataSummaryTable } from './DataPreviewTable';
import { ThinkingDisplay } from './ThinkingDisplay';
import { Textarea } from '@/components/ui/textarea';
import { Button } from '@/components/ui/button';
import { getUserEmail } from '@/utils/userStorage';

interface MessageBubbleProps {
  message: Message;
  sampleRows?: Record<string, any>[];
  columns?: string[];
  numericColumns?: string[];
  dateColumns?: string[];
  totalRows?: number;
  totalColumns?: number;
  onEditMessage?: (messageIndex: number, newContent: string) => void;
  messageIndex?: number;
  isLastUserMessage?: boolean;
  thinkingSteps?: ThinkingStep[]; // Thinking steps to display below user messages
}

export const MessageBubble = forwardRef<HTMLDivElement, MessageBubbleProps>(({
  message,
  sampleRows,
  columns,
  numericColumns,
  dateColumns,
  totalRows,
  totalColumns,
  onEditMessage,
  messageIndex,
  isLastUserMessage = false,
  thinkingSteps,
}, ref) => {
  const isUser = message.role === 'user';
  const currentUserEmail = getUserEmail()?.toLowerCase();
  const messageUserEmail = message.userEmail?.toLowerCase();
  
  // Show name if it's a user message and has a different email (shared analysis)
  const showUserName = isUser && messageUserEmail && messageUserEmail !== currentUserEmail;
  const displayName = message.userEmail ? message.userEmail.split('@')[0] : 'You';
  
  const [isEditing, setIsEditing] = useState(false);
  const [editValue, setEditValue] = useState(message.content);

  const handleSaveEdit = () => {
    if (editValue.trim() && onEditMessage && messageIndex !== undefined) {
      onEditMessage(messageIndex, editValue.trim());
      setIsEditing(false);
    }
  };

  const handleCancelEdit = () => {
    setEditValue(message.content);
    setIsEditing(false);
  };

  return (
    <div
      ref={ref}
      className={`flex gap-3 ${isUser ? 'justify-end' : 'justify-start'} mb-4`}
      data-testid={`message-${message.role}`}
    >
      {!isUser && (
        <div className="flex-shrink-0 w-8 h-8 rounded-full bg-gradient-to-br from-primary to-primary/80 flex items-center justify-center shadow-sm">
          <Bot className="w-4 h-4 text-primary-foreground" />
        </div>
      )}
      
      <div className={`flex-1 max-w-[90%] ${isUser ? 'ml-auto' : 'mr-0'}`}>
        {isUser && (
          <div className="relative group">
            {/* Display user name and edit button in a flex container to avoid overlap */}
            {(showUserName || (isLastUserMessage && onEditMessage)) && (
              <div className="flex items-center justify-end gap-2 mb-1 mr-2">
                {showUserName && (
                  <span className="text-xs text-gray-500">{displayName}</span>
                )}
                {isLastUserMessage && onEditMessage && !isEditing && (
                  <button
                    onClick={() => setIsEditing(true)}
                    className="opacity-0 group-hover:opacity-100 transition-opacity p-1.5 rounded-md hover:bg-gray-100 text-gray-600 hover:text-gray-900 text-xs font-medium flex items-center gap-1"
                    title="Edit message"
                  >
                    <Edit2 className="h-3 w-3" />
                    <span>Edit</span>
                  </button>
                )}
              </div>
            )}
            {isEditing ? (
              <div className="rounded-xl px-4 py-3 shadow-sm bg-primary text-primary-foreground ml-auto">
                <Textarea
                  value={editValue}
                  onChange={(e) => setEditValue(e.target.value)}
                  className="bg-transparent border-none text-primary-foreground resize-none min-h-[60px] max-h-[200px] focus-visible:ring-0 focus-visible:ring-offset-0 p-0"
                  autoFocus
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && !e.shiftKey) {
                      e.preventDefault();
                      handleSaveEdit();
                    } else if (e.key === 'Escape') {
                      handleCancelEdit();
                    }
                  }}
                />
                <div className="flex gap-2 mt-3">
                  <Button
                    size="sm"
                    onClick={handleSaveEdit}
                    className="h-7 text-xs bg-white/20 hover:bg-white/30 text-white border border-white/30"
                  >
                    <Check className="h-3 w-3 mr-1" />
                    Save & Submit
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={handleCancelEdit}
                    className="h-7 text-xs text-white/80 hover:text-white hover:bg-white/10"
                  >
                    <XIcon className="h-3 w-3 mr-1" />
                    Cancel
                  </Button>
                </div>
              </div>
            ) : (
              <div
                className={`rounded-xl px-4 py-3 shadow-sm bg-primary text-primary-foreground ml-auto relative`}
                data-testid={`message-content-${message.role}`}
              >
                <p className="text-sm leading-relaxed whitespace-pre-wrap">
                  {message.content}
                </p>
              </div>
            )}
          </div>
        )}

        {/* Display thinking steps below user messages */}
        {isUser && thinkingSteps && thinkingSteps.length > 0 && (
          <ThinkingDisplay steps={thinkingSteps} />
        )}

        {!isUser && message.content && (
          <div
            className="rounded-xl px-4 py-3 shadow-sm bg-white border border-gray-100"
            data-testid={`message-content-${message.role}`}
          >
            <p className="text-sm text-gray-800 leading-relaxed whitespace-pre-wrap">
              {message.content}
            </p>
          </div>
        )}

        {!isUser && sampleRows && columns && sampleRows.length > 0 && (
          <div className="mt-3">
            <DataPreview 
              data={sampleRows} 
              columns={columns}
              numericColumns={numericColumns}
              dateColumns={dateColumns}
              totalRows={totalRows}
              totalColumns={totalColumns}
              defaultExpanded={true}
            />
          </div>
        )}

        {!isUser && message.charts && message.charts.length > 0 && (
          <div className={`mt-3 grid gap-4 ${
            message.charts.length === 1 
              ? 'grid-cols-1' 
              : 'grid-cols-1 md:grid-cols-2 lg:grid-cols-3'
          }`}>
            {message.charts.map((chart, idx) => (
              <ChartRenderer 
                key={idx} 
                chart={chart} 
                index={idx}
                isSingleChart={message.charts!.length === 1}
                enableFilters
              />
            ))}
          </div>
        )}

        {!isUser && message.insights && message.insights.length > 0 && (
          <div className="mt-3">
            <InsightCard insights={message.insights} />
          </div>
        )}

        {/* Display data preview for Data Ops responses */}
        {!isUser && (message as any).preview && Array.isArray((message as any).preview) && (message as any).preview.length > 0 && (
          <div className="mt-3">
            <DataPreviewTable data={(message as any).preview} />
          </div>
        )}

        {/* Display data summary for Data Ops responses */}
        {!isUser && (message as any).summary && Array.isArray((message as any).summary) && (message as any).summary.length > 0 && (
          <div className="mt-3">
            <DataSummaryTable summary={(message as any).summary} />
          </div>
        )}
      </div>

      {isUser && (
        <div className="flex-shrink-0 w-8 h-8 rounded-full bg-primary flex items-center justify-center shadow-sm">
          <User className="w-4 h-4 text-primary-foreground" />
        </div>
      )}
    </div>
  );
});

MessageBubble.displayName = 'MessageBubble';
