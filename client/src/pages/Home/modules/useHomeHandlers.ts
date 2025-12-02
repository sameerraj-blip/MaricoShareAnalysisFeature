import { Message } from '@/shared/schema';

interface UseHomeHandlersProps {
  sessionId: string | null;
  messages: Message[];
  setMessages: (messages: Message[] | ((prev: Message[]) => Message[])) => void;
  uploadMutation: {
    mutate: (file: File) => void;
  };
  chatMutation: {
    mutate: (payload: { message: string; targetTimestamp?: number }) => void;
  };
  resetState: () => void;
}

export const useHomeHandlers = ({
  sessionId,
  messages,
  setMessages,
  uploadMutation,
  chatMutation,
  resetState,
}: UseHomeHandlersProps) => {
  const handleFileSelect = (file: File) => {
    uploadMutation.mutate(file);
  };

  const handleSendMessage = (message: string) => {
    if (!sessionId) return;
    
    const userMessage: Message = {
      role: 'user',
      content: message,
      timestamp: Date.now(),
    };
    
    setMessages((prev) => [...prev, userMessage]);
    chatMutation.mutate({ message, targetTimestamp: userMessage.timestamp });
  };

  const handleUploadNew = () => {
    resetState();
  };

  const handleEditMessage = (messageIndex: number, newContent: string) => {
    if (!sessionId) return;
    
    setMessages((prev) => {
      const updated = [...prev];
      
      // Update the user message
      if (updated[messageIndex] && updated[messageIndex].role === 'user') {
        updated[messageIndex] = {
          ...updated[messageIndex],
          content: newContent,
        };
        
        // Remove ALL messages below the edited message (not just the immediate assistant response)
        // This includes all subsequent user and assistant messages
        if (updated.length > messageIndex + 1) {
          updated.splice(messageIndex + 1);
        }
      }
      
      // Use setTimeout to ensure state update and ref sync before mutation
      const targetTimestamp = updated[messageIndex]?.timestamp;
      setTimeout(() => {
        chatMutation.mutate({ message: newContent, targetTimestamp });
      }, 0);
      
      return updated;
    });
  };

  return {
    handleFileSelect,
    handleSendMessage,
    handleUploadNew,
    handleEditMessage,
  };
};
