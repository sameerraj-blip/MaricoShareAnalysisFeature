import React, { useState, useEffect } from "react";
import { Switch, Route, useLocation, Router as WouterRouter } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { Layout } from "@/pages/Layout";
import Home from "@/pages/Home/Home";
import Dashboard from "@/pages/Dashboard/Dashboard";
import Analysis from "@/pages/Analysis/Analysis";
import NotFound from "@/pages/NotFound/not-found";
import { DashboardProvider } from "@/pages/Dashboard/context/DashboardContext";
import { AuthProvider } from "@/contexts/AuthContext";
import ProtectedRoute from "@/components/ProtectedRoute";
import AuthCallback from "@/components/AuthCallback";
import { PublicClientApplication } from '@azure/msal-browser';
import { MsalProvider } from '@azure/msal-react';
import { createMsalConfig } from '@/auth/msalConfig';

type PageType = 'home' | 'dashboard' | 'analysis';
type ModeType = 'analysis' | 'dataOps' | 'modeling';

function Router() {
  const [location, setLocation] = useLocation();
  const [resetTrigger, setResetTrigger] = useState(0);
  const [loadedSessionData, setLoadedSessionData] = useState<any>(null);
  const [currentSessionId, setCurrentSessionId] = useState<string | null>(null);
  const [currentFileName, setCurrentFileName] = useState<string | null>(null);

  // Extract mode from URL path
  const getModeFromPath = (path: string): ModeType => {
    if (path.startsWith('/data-ops')) return 'dataOps';
    if (path.startsWith('/modeling')) return 'modeling';
    return 'analysis'; // default
  };

  // Extract page type from location
  const getCurrentPage = (): PageType => {
    if (location.startsWith('/dashboard')) return 'dashboard';
    if (location === '/analysis/history' || location.startsWith('/analysis/history')) return 'analysis';
    // For /analysis, /data-ops, /modeling - these are chat interfaces, so return 'home'
    return 'home';
  };

  const handleNavigate = (page: PageType) => {
    if (page === 'home') {
      // Navigate to chat interface (default to analysis mode)
      setLocation('/analysis');
    } else if (page === 'dashboard') {
      setLocation('/dashboard');
    } else if (page === 'analysis') {
      // Navigate to analysis history page
      setLocation('/analysis/history');
    }
  };

  const handleNewChat = () => {
    const currentMode = getModeFromPath(location);
    setLocation(`/${currentMode === 'dataOps' ? 'data-ops' : currentMode === 'modeling' ? 'modeling' : 'analysis'}`);
    setLoadedSessionData(null);
    setCurrentSessionId(null);
    setCurrentFileName(null);
  };

  const handleUploadNew = () => {
    const currentMode = getModeFromPath(location);
    setLocation(`/${currentMode === 'dataOps' ? 'data-ops' : currentMode === 'modeling' ? 'modeling' : 'analysis'}`);
    setResetTrigger(prev => prev + 1);
    setLoadedSessionData(null);
    setCurrentSessionId(null);
    setCurrentFileName(null);
  };

  const handleLoadSession = (sessionId: string, sessionData: any) => {
    console.log('🔄 Loading session in App:', sessionId, sessionData);
    // Clear resetTrigger to prevent file dialog from opening when loading a session
    setResetTrigger(0);
    setLoadedSessionData(sessionData);
    // Navigate to chat interface (default to analysis mode) when loading a session
    setLocation('/analysis');
  };

  const handleModeChange = (mode: ModeType) => {
    const basePath = location.split('/').slice(0, -1).join('/') || '';
    if (mode === 'dataOps') {
      setLocation('/data-ops');
    } else if (mode === 'modeling') {
      setLocation('/modeling');
    } else {
      setLocation('/analysis');
    }
  };

  // Redirect root to /analysis
  useEffect(() => {
    if (location === '/') {
      setLocation('/analysis');
    }
  }, [location, setLocation]);

  const currentPage = getCurrentPage();
  const currentMode = getModeFromPath(location);

  const handleSessionChange = (sessionId: string | null, fileName: string | null) => {
    setCurrentSessionId(sessionId);
    setCurrentFileName(fileName);
  };

  return (
    <Layout 
      currentPage={currentPage}
      onNavigate={handleNavigate}
      onNewChat={handleNewChat}
      onUploadNew={handleUploadNew}
      sessionId={currentSessionId}
      fileName={currentFileName}
    >
      <Switch>
        <Route path="/analysis/history">
          <Analysis onNavigate={handleNavigate} onNewChat={handleNewChat} onLoadSession={handleLoadSession} onUploadNew={handleUploadNew} />
        </Route>
        <Route path="/analysis">
          <Home 
            resetTrigger={resetTrigger} 
            loadedSessionData={loadedSessionData}
            initialMode="analysis"
            onModeChange={handleModeChange}
            onSessionChange={handleSessionChange}
          />
        </Route>
        <Route path="/data-ops">
          <Home 
            resetTrigger={resetTrigger} 
            loadedSessionData={loadedSessionData}
            initialMode="dataOps"
            onModeChange={handleModeChange}
            onSessionChange={handleSessionChange}
          />
        </Route>
        <Route path="/modeling">
          <Home 
            resetTrigger={resetTrigger} 
            loadedSessionData={loadedSessionData}
            initialMode="modeling"
            onModeChange={handleModeChange}
            onSessionChange={handleSessionChange}
          />
        </Route>
        <Route path="/dashboard">
          <Dashboard />
        </Route>
        <Route>
          <NotFound />
        </Route>
      </Switch>
    </Layout>
  );
}

// Component to handle authentication redirects
function AuthRedirectHandler() {
  const [isHandlingRedirect, setIsHandlingRedirect] = useState(true);

  useEffect(() => {
    // Check if we're handling a redirect
    const urlParams = new URLSearchParams(window.location.search);
    const isRedirect = urlParams.has('code') || urlParams.has('error');
    
    if (isRedirect) {
      // We're in a redirect flow, show the callback component
      setIsHandlingRedirect(true);
    } else {
      // Normal app flow
      setIsHandlingRedirect(false);
    }
  }, []);

  if (isHandlingRedirect) {
    return <AuthCallback />;
  }

  return (
    <WouterRouter>
      <Router />
    </WouterRouter>
  );
}

// Create MSAL instance with dynamic config
const msalInstance = new PublicClientApplication(createMsalConfig());

function App() {
  return (
    <MsalProvider instance={msalInstance}>
      <QueryClientProvider client={queryClient}>
        <TooltipProvider>
          <AuthProvider>
            <ProtectedRoute>
              <DashboardProvider>
                <Toaster />
                <AuthRedirectHandler />
              </DashboardProvider>
            </ProtectedRoute>
          </AuthProvider>
        </TooltipProvider>
      </QueryClientProvider>
    </MsalProvider>
  );
}

export default App;
