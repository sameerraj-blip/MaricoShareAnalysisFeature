import React, { useState, useEffect } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { sessionsApi } from '@/lib/api';
import { getUserEmail } from '@/utils/userStorage';
import { Search, Plus, Calendar, FileText, MessageSquare, BarChart3, Loader2, Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { useToast } from '@/hooks/use-toast';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';

interface Session {
  id: string;
  username: string;
  fileName: string;
  uploadedAt: number;
  createdAt: number;
  lastUpdatedAt: number;
  messageCount: number;
  chartCount: number;
  sessionId: string;
}

interface SessionsResponse {
  sessions: Session[];
  count: number;
  message: string;
}

interface AnalysisProps {
  onNavigate?: (page: 'home' | 'dashboard' | 'analysis') => void;
  onNewChat?: () => void;
  onLoadSession?: (sessionId: string, sessionData: any) => void;
  onUploadNew?: () => void;
}

const Analysis: React.FC<AnalysisProps> = ({ onNavigate, onNewChat, onLoadSession, onUploadNew }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [filteredSessions, setFilteredSessions] = useState<Session[]>([]);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [sessionToDelete, setSessionToDelete] = useState<Session | null>(null);
  const [isDeleting, setIsDeleting] = useState(false);
  const { toast } = useToast();
  const userEmail = getUserEmail();
  const queryClient = useQueryClient();

  // Debug user email
  useEffect(() => {
    console.log('👤 User email:', userEmail);
  }, [userEmail]);

  // Fetch sessions data
  const { data: sessionsData, isLoading, error, refetch } = useQuery<SessionsResponse>({
    queryKey: ['sessions', userEmail], // Include userEmail in query key for proper caching
    queryFn: async () => {
      console.log('🔍 Fetching sessions from API for user:', userEmail);
      const result = await sessionsApi.getAllSessions();
      console.log('✅ Sessions API response:', result);
      return result;
    },
    enabled: !!userEmail,
    retry: 2,
  });

  // Filter sessions based on search query
  useEffect(() => {
    if (sessionsData?.sessions) {
      const filtered = sessionsData.sessions.filter(session =>
        session.fileName.toLowerCase().includes(searchQuery.toLowerCase()) ||
        session.id.toLowerCase().includes(searchQuery.toLowerCase())
      );
      setFilteredSessions(filtered);
    }
  }, [sessionsData, searchQuery]);

  // Handle session click
  const handleSessionClick = async (session: Session) => {
    try {
      toast({
        title: 'Loading Session',
        description: `Loading analysis for ${session.fileName}...`,
      });

      console.log('🔍 Loading session details for:', session.sessionId);
      
      // Fetch session details
      const sessionDetails = await sessionsApi.getSessionDetails(session.sessionId);
      console.log('✅ Session details loaded:', sessionDetails);

      // If onLoadSession callback is provided, use it
      if (onLoadSession) {
        onLoadSession(session.sessionId, sessionDetails);
        toast({
          title: 'Session Loaded',
          description: `Analysis for ${session.fileName} is now active`,
        });
      } else {
        // Fallback: navigate to home with session data
        if (onNavigate) {
          onNavigate('home');
          toast({
            title: 'Session Selected',
            description: `Switched to analysis for ${session.fileName}`,
          });
        }
      }
    } catch (error) {
      console.error('❌ Failed to load session:', error);
      toast({
        title: 'Error Loading Session',
        description: 'Failed to load session details. Please try again.',
        variant: 'destructive',
      });
    }
  };

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

  // Handle delete button click
  const handleDeleteClick = (e: React.MouseEvent, session: Session) => {
    e.stopPropagation(); // Prevent card click event
    setSessionToDelete(session);
    setDeleteDialogOpen(true);
  };

  // Handle delete confirmation
  const handleDeleteConfirm = async () => {
    if (!sessionToDelete) return;

    setIsDeleting(true);
    try {
      await sessionsApi.deleteSession(sessionToDelete.sessionId);
      
      toast({
        title: 'Session Deleted',
        description: `Analysis session for ${sessionToDelete.fileName} has been deleted.`,
      });

      // Invalidate and refetch sessions
      await queryClient.invalidateQueries({ queryKey: ['sessions', userEmail] });
      refetch();

      setDeleteDialogOpen(false);
      setSessionToDelete(null);
    } catch (error) {
      console.error('❌ Failed to delete session:', error);
      toast({
        title: 'Error Deleting Session',
        description: error instanceof Error ? error.message : 'Failed to delete session. Please try again.',
        variant: 'destructive',
      });
    } finally {
      setIsDeleting(false);
    }
  };

  // Handle delete cancel
  const handleDeleteCancel = () => {
    setDeleteDialogOpen(false);
    setSessionToDelete(null);
  };

  // Format date for display
  const formatDate = (timestamp: number) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffTime = Math.abs(now.getTime() - date.getTime());
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
    
    if (diffDays === 1) return 'Yesterday';
    if (diffDays < 7) return `${diffDays} days ago`;
    if (diffDays < 30) return `${Math.ceil(diffDays / 7)} weeks ago`;
    return date.toLocaleDateString();
  };

  // Format file name for display
  const formatFileName = (fileName: string) => {
    if (fileName.length > 50) {
      return fileName.substring(0, 47) + '...';
    }
    return fileName;
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <Loader2 className="h-8 w-8 animate-spin mx-auto mb-4 text-blue-600" />
          <p className="text-gray-600">Loading your analysis history...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="text-red-600 mb-4">
            <FileText className="h-12 w-12 mx-auto mb-2" />
            <h2 className="text-xl font-semibold">Failed to load sessions</h2>
            <p className="text-gray-600 mt-2">There was an error loading your analysis history.</p>
          </div>
          <Button onClick={() => refetch()} variant="outline">
            Try Again
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="h-[calc(100vh-10vh)] bg-gray-50 flex flex-col">
      <div className="max-w-6xl mx-40 px-6 py-8 flex-1 flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
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

        {/* Search Bar */}
        <div className="relative mb-6">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 h-4 w-4" />
          <Input
            type="text"
            placeholder="Search your analyses..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10 h-12 text-lg border-gray-300 focus:border-blue-500 focus:ring-blue-500"
          />
        </div>

        {/* Sessions List */}
        <div className="space-y-4 max-h-[55vh] overflow-y-auto">
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
              <Card 
                key={session.id} 
                className="hover:shadow-md transition-shadow cursor-pointer border-gray-200"
                onClick={() => handleSessionClick(session)}
              >
                <CardContent className="p-6">
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <div className="flex items-center gap-3 mb-2">
                        <FileText className="h-5 w-5 text-blue-600 flex-shrink-0" />
                        <h3 className="text-lg font-semibold text-gray-900">
                          {formatFileName(session.fileName)}
                        </h3>
                        <Badge variant="secondary" className="text-xs">
                          {session.id.split('_')[0]}
                        </Badge>
                      </div>
                      
                      <div className="flex items-center gap-4 text-sm text-gray-600 mb-3">
                        <div className="flex items-center gap-1">
                          <Calendar className="h-4 w-4" />
                          <span>Last analysis {formatDate(session.lastUpdatedAt)}</span>
                        </div>
                        <div className="flex items-center gap-1">
                          <MessageSquare className="h-4 w-4" />
                          <span>{session.messageCount} messages</span>
                        </div>
                        <div className="flex items-center gap-1">
                          <BarChart3 className="h-4 w-4" />
                          <span>{session.chartCount} charts</span>
                        </div>
                      </div>
                      
                      <div className="text-xs text-gray-500 break-all">
                        Session ID: {session.sessionId}
                      </div>
                    </div>
                    
                    <div className="flex items-center gap-4">
                      <div className="text-right text-sm text-gray-500">
                        <div>{new Date(session.uploadedAt).toLocaleDateString()}</div>
                        <div className="text-xs">
                          {new Date(session.uploadedAt).toLocaleTimeString()}
                        </div>
                      </div>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={(e) => handleDeleteClick(e, session)}
                        className="text-red-600 hover:text-red-700 hover:bg-red-50"
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))
          )}
        </div>
      </div>

      {/* Delete Confirmation Dialog */}
      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Are you sure you want to delete this session?</AlertDialogTitle>
            <AlertDialogDescription>
              This action cannot be undone. This will permanently delete the analysis session
              {sessionToDelete && ` for "${sessionToDelete.fileName}"`} and all associated data,
              including messages, charts, and insights.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={handleDeleteCancel} disabled={isDeleting}>
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDeleteConfirm}
              disabled={isDeleting}
              className="bg-red-600 hover:bg-red-700 focus:ring-red-600"
            >
              {isDeleting ? (
                <>
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  Deleting...
                </>
              ) : (
                'Delete'
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
};

export default Analysis;