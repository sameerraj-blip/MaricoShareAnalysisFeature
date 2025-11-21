import React, { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { sessionsApi } from '@/lib/api';
import { getUserEmail } from '@/utils/userStorage';
import { Search, Plus, ArrowUpDown, FileText } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent } from '@/components/ui/card';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { SharedAnalysesPanel } from './SharedAnalysesPanel';
import { ShareAnalysisDialog } from './ShareAnalysisDialog';
import { AnalysisSessionSummary } from '@/shared/schema';
import { AnalysisLoadingState } from './Components/AnalysisLoadingState';
import { AnalysisErrorState } from './Components/AnalysisErrorState';
import { SessionCard } from './Components/SessionCard';
import { EditSessionDialog } from './Components/EditSessionDialog';
import { DeleteSessionDialog } from './Components/DeleteSessionDialog';
import { useSessionFilters } from './modules/useSessionFilters';
import { useSessionManagement } from './modules/useSessionManagement';
import { useToast } from '@/hooks/use-toast';
import { AnalysisProps, SessionsResponse } from './types';

/**
 * Main Analysis page component
 * Displays a list of analysis sessions with search, sort, and management capabilities
 */
const Analysis: React.FC<AnalysisProps> = ({ onNavigate, onNewChat, onLoadSession, onUploadNew }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [sortOrder, setSortOrder] = useState<'newest' | 'oldest'>('newest');
  const { toast } = useToast();
  const userEmail = getUserEmail();

  // Fetch sessions data
  const { data: sessionsData, isLoading, error, refetch } = useQuery<SessionsResponse>({
    queryKey: ['sessions', userEmail],
    queryFn: async () => {
      console.log('🔍 Fetching sessions from API for user:', userEmail);
      const result = await sessionsApi.getAllSessions();
      console.log('✅ Sessions API response:', result);
      return result;
    },
    enabled: !!userEmail,
    retry: 2,
    refetchOnWindowFocus: true,
    refetchOnMount: true,
    staleTime: 0,
  });

  // Refetch sessions when page becomes visible or when userEmail changes
  useEffect(() => {
    if (!userEmail) return;

    const timeoutId = setTimeout(() => {
      refetch();
    }, 200);

    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible' && userEmail) {
        refetch();
      }
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);

    return () => {
      clearTimeout(timeoutId);
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  }, [userEmail, refetch]);

  // Filter and sort sessions
  const filteredSessions = useSessionFilters({
    sessions: sessionsData?.sessions,
    searchQuery,
    sortOrder,
  });

  // Session management operations
  const {
    loadingSessionId,
    deleteDialogOpen,
    setDeleteDialogOpen,
    sessionToDelete,
    isDeleting,
    handleDeleteClick,
    handleDeleteConfirm,
    handleDeleteCancel,
    editDialogOpen,
    setEditDialogOpen,
    editFileName,
    setEditFileName,
    isUpdating,
    handleEditClick,
    handleEditConfirm,
    handleEditCancel,
    shareDialogOpen,
    setShareDialogOpen,
    sessionToShare,
    setSessionToShare,
    handleShareClick,
    handleSessionClick,
  } = useSessionManagement({
    onLoadSession,
    onNavigate,
    refetch,
  });

  // Handle new chat
  const handleNewChat = () => {
    if (onUploadNew) {
      onUploadNew();
    } else if (onNewChat) {
      onNewChat();
    } else if (onNavigate) {
      onNavigate('home');
    } else {
      toast({ title: 'New Analysis', description: 'Starting a new analysis session' });
    }
  };

  // Handle shared analysis accepted
  const handleSharedAccepted = async (summary: AnalysisSessionSummary) => {
    await refetch();
    toast({
      title: 'Shared analysis added',
      description: `${summary.fileName} is now part of your workspace.`,
    });
  };

  // Loading state
  if (isLoading) {
    return <AnalysisLoadingState onSharedAccepted={handleSharedAccepted} />;
  }

  // Error state
  if (error) {
    return <AnalysisErrorState onRetry={() => refetch()} />;
  }

  return (
    <div className="h-[calc(100vh-10vh)] bg-gray-50 flex flex-col" data-analysis-page>
      <div className="max-w-7xl mx-auto px-6 py-8 flex-1 flex flex-col min-h-0">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Your Analysis History</h1>
            <p className="text-gray-600 mt-2">
              {sessionsData?.count || 0} analysis sessions{userEmail ? ` for ${userEmail}` : ''}
            </p>
          </div>
          <Button onClick={handleNewChat} className="bg-black text-white hover:bg-gray-800">
            <Plus className="h-4 w-4 mr-2" />
            New Analysis
          </Button>
        </div>

        {/* Search Bar and Sort */}
        <div className="flex gap-4 mb-6">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 h-4 w-4" />
            <Input
              type="text"
              placeholder="Search your analyses..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10 h-12 text-lg border-gray-300 focus:border-blue-500 focus:ring-blue-500"
            />
          </div>
          <div className="flex items-center gap-2">
            <ArrowUpDown className="h-4 w-4 text-gray-500" />
            <Select value={sortOrder} onValueChange={(value: 'newest' | 'oldest') => setSortOrder(value)}>
              <SelectTrigger className="w-[180px] h-12 border-gray-300">
                <SelectValue placeholder="Sort by" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="newest">Newest First</SelectItem>
                <SelectItem value="oldest">Oldest First</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>

        {/* Horizontal Layout: Shared Panel and Sessions List */}
        <div className="flex gap-6 flex-1 min-h-0">
          {/* Left Column: Shared Analyses Panel */}
          <div className="w-96 flex-shrink-0 flex flex-col min-h-0">
            <SharedAnalysesPanel onAccepted={handleSharedAccepted} />
          </div>

          {/* Right Column: Sessions List */}
          <div className="flex-1 flex flex-col min-w-0">
            <div className="space-y-4 flex-1 overflow-y-auto">
              {filteredSessions.length === 0 ? (
                <Card className="text-center py-12">
                  <CardContent>
                    <FileText className="h-12 w-12 mx-auto mb-4 text-gray-400" />
                    <h3 className="text-lg font-semibold text-gray-900 mb-2">
                      {searchQuery ? 'No matching analyses found' : 'No analysis sessions yet'}
                    </h3>
                    <p className="text-gray-600 mb-4">
                      {searchQuery
                        ? 'Try adjusting your search terms'
                        : `Welcome! Upload your first file to start analyzing data${userEmail ? ` as ${userEmail}` : ''}`
                      }
                    </p>
                    {!searchQuery && (
                      <Button onClick={handleNewChat} className="bg-blue-600 hover:bg-blue-700">
                        <Plus className="h-4 w-4 mr-2" />
                        Start Your First Analysis
                      </Button>
                    )}
                  </CardContent>
                </Card>
              ) : (
                filteredSessions.map((session) => (
                  <SessionCard
                    key={session.id}
                    session={session}
                    isLoading={loadingSessionId === session.sessionId}
                    onSessionClick={handleSessionClick}
                    onEditClick={handleEditClick}
                    onDeleteClick={handleDeleteClick}
                    onShareClick={handleShareClick}
                  />
                ))
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Edit Session Dialog */}
      <EditSessionDialog
        open={editDialogOpen}
        onOpenChange={setEditDialogOpen}
        fileName={editFileName}
        onFileNameChange={setEditFileName}
        isUpdating={isUpdating}
        onConfirm={handleEditConfirm}
        onCancel={handleEditCancel}
      />

      {/* Delete Session Dialog */}
      <DeleteSessionDialog
        open={deleteDialogOpen}
        onOpenChange={setDeleteDialogOpen}
        sessionFileName={sessionToDelete?.fileName}
        isDeleting={isDeleting}
        onConfirm={handleDeleteConfirm}
        onCancel={handleDeleteCancel}
      />

      {/* Share Analysis Dialog */}
      <ShareAnalysisDialog
        open={shareDialogOpen}
        onOpenChange={(open) => {
          setShareDialogOpen(open);
          if (!open) {
            setSessionToShare(null);
          }
        }}
        sessionId={sessionToShare?.sessionId}
        fileName={sessionToShare?.fileName}
      />
    </div>
  );
};

export default Analysis;
