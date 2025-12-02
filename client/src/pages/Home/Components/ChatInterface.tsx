import { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import { Message, ThinkingStep } from '@/shared/schema';
import { MessageBubble } from '@/pages/Home/Components/MessageBubble';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { Send, Upload as UploadIcon, Square, Filter, Database } from 'lucide-react';
import { Switch } from '@/components/ui/switch';
import { Label } from '@/components/ui/label';
import { Card } from '@/components/ui/card';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { getUserEmail } from '@/utils/userStorage';
import { useToast } from '@/hooks/use-toast';

interface ChatInterfaceProps {
  messages: Message[];
  onSendMessage: (message: string) => void;
  onUploadNew: () => void;
  isLoading: boolean;
  onLoadHistory?: () => void;
  canLoadHistory?: boolean;
  loadingHistory?: boolean;
  sampleRows?: Record<string, any>[];
  columns?: string[];
  numericColumns?: string[];
  dateColumns?: string[];
  totalRows?: number;
  totalColumns?: number;
  onStopGeneration?: () => void;
  onEditMessage?: (messageIndex: number, newContent: string) => void;
  thinkingSteps?: ThinkingStep[]; // Thinking steps to display during loading
  thinkingTargetTimestamp?: number | null;
  aiSuggestions?: string[]; // AI-generated suggestions
  collaborators?: string[]; // List of all collaborators in the session
  dataOpsMode?: boolean; // Data Ops mode toggle state
  onDataOpsModeChange?: (enabled: boolean) => void; // Callback for mode change
}

// Dynamic suggestions based on conversation context
const getSuggestions = (
  messages: Message[], 
  columns?: string[], 
  numericColumns?: string[],
  aiSuggestions?: string[]
) => {
  // If AI suggestions are provided, use them first (they're contextually relevant)
  if (aiSuggestions && aiSuggestions.length > 0) {
    return aiSuggestions;
  }

  // If there's conversation history, suggest follow-ups
  if (messages.length > 1) {
    const lastMessage = messages[messages.length - 1];
    if (lastMessage.role === 'assistant' && lastMessage.content) {
      // Extract mentioned columns from last response
      const mentionedCols = (columns || []).filter(col => 
        lastMessage.content.toLowerCase().includes(col.toLowerCase())
      );
      
      if (mentionedCols.length > 0) {
        return [
          `Tell me more about ${mentionedCols[0]}`,
          `What affects ${mentionedCols[0]}?`,
          `Show me trends for ${mentionedCols[0]}`,
          "What else can you show me?"
        ];
      }
    }
  }
  
  // Default suggestions
  return [
    "What affects revenue?",
    "Show me trends over time",
    "What are the top performers?",
    "Analyze correlations in the data"
  ];
};

export function ChatInterface({ 
  messages, 
  onSendMessage, 
  onUploadNew, 
  isLoading, 
  onLoadHistory,
  canLoadHistory = false,
  loadingHistory = false,
  sampleRows, 
  columns,
  numericColumns,
  dateColumns,
  totalRows,
  totalColumns,
  onStopGeneration,
  onEditMessage,
  thinkingSteps,
  thinkingTargetTimestamp,
  aiSuggestions,
  collaborators: propCollaborators,
  dataOpsMode = false,
  onDataOpsModeChange,
}: ChatInterfaceProps) {
  const [inputValue, setInputValue] = useState('');
  const [selectedCollaborator, setSelectedCollaborator] = useState<string>('all');
  const { toast } = useToast();
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const lastMessageRef = useRef<HTMLDivElement | null>(null);
  const previousLastTimestampRef = useRef<number | null>(null);
  const [mentionState, setMentionState] = useState<{
    active: boolean;
    query: string;
    start: number | null;
    options: string[];
    selectedIndex: number;
  }>({
    active: false,
    query: '',
    start: null,
    options: [],
    selectedIndex: 0
  });

  const currentUserEmail = getUserEmail()?.toLowerCase();

  // Get all collaborators: from prop, or extract from messages, and always include current user
  const collaborators = useMemo(() => {
    const collaboratorSet = new Set<string>();
    
    // Add collaborators from prop (session data)
    if (propCollaborators && propCollaborators.length > 0) {
      propCollaborators.forEach((email) => {
        if (email) collaboratorSet.add(email.toLowerCase());
      });
    }
    
    // Also extract from messages (in case some collaborators haven't sent messages yet)
    messages.forEach((message) => {
      if (message.role === 'user' && message.userEmail) {
        collaboratorSet.add(message.userEmail.toLowerCase());
      }
    });
    
    // Always include current user
    if (currentUserEmail) {
      collaboratorSet.add(currentUserEmail);
    }
    
    return Array.from(collaboratorSet).sort();
  }, [propCollaborators, messages, currentUserEmail]);

  // Handle filter change with toast notification
  const handleFilterChange = (value: string) => {
    setSelectedCollaborator(value);
    const displayName = value === 'all' 
      ? 'All Messages' 
      : collaborators.find(c => c.toLowerCase() === value.toLowerCase())?.split('@')[0] || value.split('@')[0];
    
    toast({
      title: "Filter applied",
      description: `Showing messages from ${displayName}`,
    });
  };

  // Filter messages based on selected collaborator
  const filteredMessages = useMemo(() => {
    if (selectedCollaborator === 'all') {
      return messages;
    }
    return messages.filter((message) => {
      // Always show assistant messages
      if (message.role === 'assistant') {
        return true;
      }
      // For user messages, filter by selected collaborator
      return message.userEmail?.toLowerCase() === selectedCollaborator.toLowerCase();
    });
  }, [messages, selectedCollaborator]);

  useEffect(() => {
    if (!filteredMessages.length || !lastMessageRef.current) return;

    const lastMessage = filteredMessages[filteredMessages.length - 1];
    if (!lastMessage) return;

    if (previousLastTimestampRef.current === lastMessage.timestamp) {
      return;
    }

    const behavior: ScrollBehavior =
      previousLastTimestampRef.current === null ? 'auto' : 'smooth';

    lastMessageRef.current.scrollIntoView({
      behavior,
      block: lastMessage.role === 'assistant' ? 'start' : 'end'
    });

    previousLastTimestampRef.current = lastMessage.timestamp;
  }, [filteredMessages]);

  useEffect(() => {
    if (isLoading && thinkingSteps && thinkingSteps.length > 0 && lastMessageRef.current) {
      lastMessageRef.current.scrollIntoView({
        behavior: 'smooth',
        block: 'nearest',
      });
    }
  }, [thinkingSteps, isLoading]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (inputValue.trim() && !isLoading) {
      onSendMessage(inputValue.trim());
      setInputValue('');
      inputRef.current?.focus();
    }
  };

  const updateMentionState = useCallback(
    (value: string, selectionStart: number | null) => {
      if (selectionStart === null) {
        setMentionState(prev => ({
          ...prev,
          active: false,
          query: '',
          start: null,
          options: [],
          selectedIndex: 0
        }));
        return;
      }

      const textUntilCaret = value.slice(0, selectionStart);
      const mentionMatch = textUntilCaret.match(/@([A-Za-z0-9 _-]*)$/);
      const availableColumns = columns ?? [];

      if (mentionMatch && availableColumns.length > 0) {
        const query = mentionMatch[1];
        const start = selectionStart - mentionMatch[0].length;
        const normalizedQuery = query.trim().toLowerCase();
        const options = availableColumns.filter(column =>
          normalizedQuery === '' ? true : column.toLowerCase().includes(normalizedQuery)
        );

        setMentionState(prev => ({
          active: options.length > 0,
          query,
          start,
          options,
          selectedIndex: options.length > 0 ? Math.min(prev.selectedIndex, options.length - 1) : 0
        }));
      } else {
        setMentionState(prev => ({
          ...prev,
          active: false,
          query: '',
          start: null,
          options: [],
          selectedIndex: 0
        }));
      }
    },
    [columns]
  );

  const handleInputChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const { value, selectionStart } = e.target;
    setInputValue(value);
    updateMentionState(value, selectionStart);
  };

  const selectMention = useCallback(
    (column: string) => {
      const textarea = inputRef.current;
      if (!textarea) return;

      const selectionStart = textarea.selectionStart ?? inputValue.length;
      const selectionEnd = textarea.selectionEnd ?? selectionStart;
      const mentionStart = mentionState.start ?? selectionStart;
      const currentValue = textarea.value;
      const before = currentValue.slice(0, mentionStart);
      const after = currentValue.slice(selectionEnd);
      const insertion = `${column} `;
      const nextValue = `${before}${insertion}${after}`;
      const nextCaretPosition = before.length + insertion.length;

      setInputValue(nextValue);
      setMentionState({
        active: false,
        query: '',
        start: null,
        options: [],
        selectedIndex: 0
      });

      requestAnimationFrame(() => {
        textarea.focus();
        textarea.setSelectionRange(nextCaretPosition, nextCaretPosition);
        updateMentionState(nextValue, nextCaretPosition);
      });
    },
    [inputValue.length, mentionState.start, updateMentionState]
  );

  const handleSuggestionClick = (suggestion: string) => {
    if (!isLoading) {
      onSendMessage(suggestion);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (mentionState.active && mentionState.options.length > 0) {
      if (e.key === 'ArrowDown' || (e.key === 'Tab' && !e.shiftKey)) {
        e.preventDefault();
        setMentionState(prev => ({
          ...prev,
          selectedIndex: (prev.selectedIndex + 1) % prev.options.length
        }));
        return;
      }

      if (e.key === 'ArrowUp' || (e.key === 'Tab' && e.shiftKey)) {
        e.preventDefault();
        setMentionState(prev => ({
          ...prev,
          selectedIndex: (prev.selectedIndex - 1 + prev.options.length) % prev.options.length
        }));
        return;
      }

      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        const selectedColumn =
          mentionState.options[mentionState.selectedIndex] ?? mentionState.options[0];
        if (selectedColumn) {
          selectMention(selectedColumn);
        }
        return;
      }

      if (e.key === 'Escape') {
        e.preventDefault();
        setMentionState(prev => ({
          ...prev,
          active: false,
          query: '',
          start: null,
          options: [],
          selectedIndex: 0
        }));
        return;
      }
    }

    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      if (inputValue.trim() && !isLoading) {
        onSendMessage(inputValue.trim());
        setInputValue('');
        requestAnimationFrame(() => inputRef.current?.focus());
      }
      return;
    }

    requestAnimationFrame(() => {
      const textarea = inputRef.current;
      if (textarea) {
        updateMentionState(textarea.value, textarea.selectionStart);
      }
    });
  };

  const handleTextareaBlur = () => {
    setMentionState(prev => ({
      ...prev,
      active: false,
      query: '',
      start: null,
      options: [],
      selectedIndex: 0
    }));
  };

  return (
    <div className="flex flex-col bg-gradient-to-br from-slate-50 to-white h-[calc(100vh-80px)] relative">
      {/* Messages Area */}
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-6xl mx-auto px-4 py-4 space-y-4">
          {collaborators.length > 0 && (
            <div className="flex justify-end items-center mb-2">
              <div className="inline-flex items-center gap-2 rounded-full border border-gray-200 bg-white px-3 py-1.5 shadow-sm hover:shadow-md transition-all duration-200">
                <Filter className="w-3.5 h-3.5 text-gray-600" />
                <span className="text-xs font-medium text-gray-700">Filter messages</span>
                <Select value={selectedCollaborator} onValueChange={handleFilterChange}>
                  <SelectTrigger className="h-6 px-2 text-xs font-semibold border-none bg-transparent shadow-none focus:ring-0 focus:ring-offset-0 hover:bg-transparent text-gray-900 min-w-[120px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Messages</SelectItem>
                    {collaborators.map((email) => {
                      const displayName = email.split('@')[0];
                      const isCurrentUser = email.toLowerCase() === currentUserEmail;
                      return (
                        <SelectItem key={email} value={email}>
                          {isCurrentUser ? `${displayName} (You)` : displayName}
                        </SelectItem>
                      );
                    })}
                  </SelectContent>
                </Select>
              </div>
            </div>
          )}
          {filteredMessages.map((message, idx) => {
            // Find the original index in the full messages array for edit functionality
            const originalIndex = messages.findIndex((m) => m.timestamp === message.timestamp && m.role === message.role);
            const isLastMessage = idx === filteredMessages.length - 1;
            // Check if this is the last user message (for edit button and thinking steps)
            const isLastUserMessage = message.role === 'user' && 
              (idx === filteredMessages.length - 1 || 
               (idx < filteredMessages.length - 1 && filteredMessages[idx + 1].role === 'assistant'));
            const isThinkingTarget = thinkingTargetTimestamp != null && message.timestamp === thinkingTargetTimestamp;
            const showThinkingSteps = isThinkingTarget && isLoading && thinkingSteps && thinkingSteps.length > 0;
            return (
              <MessageBubble
                key={`${message.timestamp}-${message.role}-${idx}`}
                message={message}
                sampleRows={originalIndex === 0 ? sampleRows : undefined}
                columns={originalIndex === 0 ? columns : undefined}
                numericColumns={originalIndex === 0 ? numericColumns : undefined}
                dateColumns={originalIndex === 0 ? dateColumns : undefined}
                totalRows={originalIndex === 0 ? totalRows : undefined}
                totalColumns={originalIndex === 0 ? totalColumns : undefined}
                onEditMessage={onEditMessage}
                messageIndex={originalIndex >= 0 ? originalIndex : idx}
                isLastUserMessage={isLastUserMessage}
                thinkingSteps={showThinkingSteps ? thinkingSteps : undefined}
                ref={isLastMessage ? lastMessageRef : undefined}
              />
            );
          })}
        </div>
      </div>

      {/* Input Area */}
      <div className="sticky bottom-0 bg-white/95 backdrop-blur-sm border-t border-gray-100">
        <div className="max-w-6xl mx-auto px-4 py-4">
          {/* Data Ops Mode Toggle */}
          {onDataOpsModeChange && (
            <div className="mb-3 flex items-center justify-center gap-2">
              <Label htmlFor="data-ops-toggle" className="text-sm font-medium text-gray-700 cursor-pointer flex items-center gap-2">
                <Database className="w-4 h-4" />
                <span>Analysis</span>
              </Label>
              <Switch
                id="data-ops-toggle"
                checked={dataOpsMode}
                onCheckedChange={onDataOpsModeChange}
                className="data-[state=checked]:bg-primary"
              />
              <Label htmlFor="data-ops-toggle" className="text-sm font-medium text-gray-700 cursor-pointer flex items-center gap-2">
                <span>Data Ops</span>
                <Database className="w-4 h-4" />
              </Label>
            </div>
          )}
          {filteredMessages.length === 0 && messages.length > 0 && (
            <div className="mb-4">
              <h3 className="text-base font-semibold text-gray-900 mb-3 text-center">
                No messages from selected collaborator
              </h3>
            </div>
          )}
          {messages.length === 0 && (
            <div className="mb-4">
              <h3 className="text-base font-semibold text-gray-900 mb-3 text-center">Try asking:</h3>
              <div className="flex flex-wrap gap-2 justify-center" data-testid="suggestion-chips">
                {getSuggestions(messages, columns, numericColumns, aiSuggestions).map((suggestion, idx) => (
                  <Button
                    key={idx}
                    variant="outline"
                    size="sm"
                    onClick={() => handleSuggestionClick(suggestion)}
                    disabled={isLoading}
                    data-testid={`suggestion-${idx}`}
                    className="text-xs px-3 py-1.5 rounded-full border-gray-200 hover:border-primary hover:bg-primary/5 transition-colors"
                  >
                    {suggestion}
                  </Button>
                ))}
              </div>
            </div>
          )}
          
          {/* Show follow-up suggestions after assistant messages */}
          {filteredMessages.length > 0 && filteredMessages[filteredMessages.length - 1].role === 'assistant' && !isLoading && (
            <div className="mb-4 mt-2">
              <div className="flex flex-wrap gap-2 justify-center">
                {getSuggestions(messages, columns, numericColumns, aiSuggestions).slice(0, 3).map((suggestion, idx) => (
                  <Button
                    key={idx}
                    variant="ghost"
                    size="sm"
                    onClick={() => handleSuggestionClick(suggestion)}
                    disabled={isLoading}
                    className="text-xs px-3 py-1.5 rounded-full text-gray-600 hover:text-primary hover:bg-primary/5 transition-colors"
                  >
                    {suggestion}
                  </Button>
                ))}
              </div>
            </div>
          )}
          
          <form onSubmit={handleSubmit} className="flex items-end gap-2">
            <div className="relative flex-1">
              <Textarea
                ref={inputRef}
                value={inputValue}
                onChange={handleInputChange}
                onKeyDown={handleKeyDown}
                onBlur={handleTextareaBlur}
                placeholder="Ask a question about your data..."
                disabled={isLoading}
                data-testid="input-message"
                rows={1}
                className="flex-1 min-h-[44px] max-h-40 resize-none text-sm rounded-xl bg-white border-2 border-gray-200 focus-visible:ring-2 focus-visible:ring-primary/40 focus-visible:border-primary shadow-sm pr-8"
              />
              {mentionState.active && mentionState.options.length > 0 && (
                <div className="absolute left-0 right-0 bottom-full z-20 mb-2 max-h-60 overflow-y-auto rounded-xl border border-gray-200 bg-white py-2 shadow-lg">
                  {mentionState.options.map((column, idx) => {
                    const isActive = idx === mentionState.selectedIndex;
                    return (
                      <button
                        type="button"
                        key={column}
                        className={`flex w-full items-center justify-between px-3 py-2 text-sm transition-colors ${
                          isActive ? 'bg-primary/10 text-primary' : 'text-gray-700 hover:bg-gray-100'
                        }`}
                        onMouseDown={(event) => {
                          event.preventDefault();
                          selectMention(column);
                        }}
                      >
                        <span className="truncate">{column}</span>
                      </button>
                    );
                  })}
                </div>
              )}
            </div>
            {isLoading && onStopGeneration ? (
              <Button
                type="button"
                onClick={onStopGeneration}
                data-testid="button-stop"
                size="icon"
                className="h-10 w-10 rounded-xl shadow-sm hover:shadow-md transition-all bg-red-500 hover:bg-red-600 text-white"
                title="Stop generating"
              >
                <Square className="w-4 h-4 fill-current" />
              </Button>
            ) : (
              <Button
                type="submit"
                disabled={!inputValue.trim() || isLoading}
                data-testid="button-send"
                size="icon"
                className="h-10 w-10 rounded-xl shadow-sm hover:shadow-md transition-all"
              >
                <Send className="w-4 h-4" />
              </Button>
            )}
          </form>
        </div>
      </div>
    </div>
  );
}
