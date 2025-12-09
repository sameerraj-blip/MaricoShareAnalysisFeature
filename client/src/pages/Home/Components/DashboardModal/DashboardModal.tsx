import React, { useState, useEffect, useRef, useMemo } from 'react';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Card, CardContent } from '@/components/ui/card';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Plus, BarChart3, X, Clock, ArrowLeft } from 'lucide-react';
import { ChartSpec } from '@/shared/schema';
import { useDashboardContext } from '@/pages/Dashboard/context/DashboardContext';
import { dashboardsApi } from '@/lib/api';
import { useToast } from '@/hooks/use-toast';
import { getUserEmail } from '@/utils/userStorage';
import { DashboardData } from '@/pages/Dashboard/modules/useDashboardState';

interface DashboardModalProps {
  isOpen: boolean;
  onClose: () => void;
  chart: ChartSpec;
}

export function DashboardModal({ isOpen, onClose, chart }: DashboardModalProps) {
  const [step, setStep] = useState<'select' | 'confirm'>('select');
  const [newDashboardName, setNewDashboardName] = useState('');
  const [selectedDashboard, setSelectedDashboard] = useState('');
  const [selectedSheetId, setSelectedSheetId] = useState<string>('');
  const [newSheetName, setNewSheetName] = useState('');
  const [createNewSheet, setCreateNewSheet] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [showDropdown, setShowDropdown] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const { toast } = useToast();
  
  const { dashboards, createDashboard, addChartToDashboard, refetch } = useDashboardContext();

  // Helper function to check if user has edit permission on a dashboard
  const hasEditPermission = useMemo(() => {
    const userEmail = getUserEmail()?.toLowerCase();
    return (dashboard: DashboardData): boolean => {
      // If it's a shared dashboard, check the permission
      if (dashboard.isShared) {
        return dashboard.sharedPermission === "edit";
      }
      // If not shared, check if user owns it
      const dashboardUsername = dashboard.username?.toLowerCase();
      return userEmail === dashboardUsername;
    };
  }, []);

  // Filter dashboards to only show those with edit permission
  const editableDashboards = useMemo(() => {
    return dashboards.filter(hasEditPermission);
  }, [dashboards, hasEditPermission]);

  // Filter dashboards based on search query (only editable ones)
  const filteredDashboards = editableDashboards.filter(dashboard =>
    dashboard.name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  // Get recently created dashboards (last 5, sorted by creation date, only editable ones)
  const recentDashboards = editableDashboards
    .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
    .slice(0, 5);

  // Get selected dashboard's sheets
  const selectedDashboardData = editableDashboards.find(d => d.id === selectedDashboard);
  // If no sheets exist, create a default one for backward compatibility
  const sheets = selectedDashboardData?.sheets && selectedDashboardData.sheets.length > 0 
    ? selectedDashboardData.sheets 
    : selectedDashboardData 
      ? [{ id: 'default', name: 'Overview', charts: selectedDashboardData.charts || [], order: 0 }]
      : [];
  
  // Debug logging
  useEffect(() => {
    if (selectedDashboard) {
      console.log('Selected dashboard:', selectedDashboardData);
      console.log('Sheets:', sheets);
      console.log('Selected sheet ID:', selectedSheetId);
    }
  }, [selectedDashboard, selectedDashboardData, sheets, selectedSheetId]);

  // Reset modal state when opening/closing
  const resetModal = () => {
    setStep('select');
    setNewDashboardName('');
    setSelectedDashboard('');
    setSelectedSheetId('');
    setNewSheetName('');
    setCreateNewSheet(false);
    setSearchQuery('');
    setShowDropdown(false);
  };

  // Reset when modal opens/closes
  useEffect(() => {
    if (isOpen) {
      resetModal();
    }
  }, [isOpen]);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setShowDropdown(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="sm:max-w-2xl w-full max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <BarChart3 className="h-5 w-5" />
            Add Chart to Dashboard
          </DialogTitle>
        </DialogHeader>
        
        <div className="space-y-4">
          {/* Chart Preview */}
          <Card className="border-0">
            <CardContent className="p-4">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-primary/10 rounded-lg">
                  <BarChart3 className="h-5 w-5 text-primary" />
                </div>
                <div>
                  <h3 className="font-medium">{chart.title}</h3>
                  <p className="text-sm text-muted-foreground capitalize">
                    {chart.type} Chart
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>

          {step === 'select' ? (
            <>
              {/* Dashboard Selection View */}
              {editableDashboards.length > 0 && (
                <div className="space-y-2">
                  <Label className="text-sm font-medium">Search Dashboard</Label>
                  <div className="relative" ref={dropdownRef}>
                    <Input
                      placeholder="Search dashboards..."
                      value={searchQuery}
                      onChange={(e) => {
                        setSearchQuery(e.target.value);
                        setShowDropdown(true);
                        setSelectedDashboard('');
                      }}
                      onFocus={() => setShowDropdown(true)}
                      className="w-full"
                    />
                    
                    {showDropdown && searchQuery && (
                      <div className="absolute top-full left-0 right-0 z-10 mt-1 bg-white border-0 rounded-md shadow-none max-h-48 overflow-y-auto">
                        {filteredDashboards.length > 0 ? (
                          filteredDashboards.map((dashboard) => (
                            <button
                              key={dashboard.id}
                              className="w-full px-3 py-2 text-left hover:bg-gray-100 flex items-center gap-2"
                              onClick={() => {
                                setSelectedDashboard(dashboard.id);
                                setSearchQuery(dashboard.name);
                                setShowDropdown(false);
                                setNewDashboardName('');
                                // Set default sheet to first sheet
                                const dashboardData = editableDashboards.find(d => d.id === dashboard.id);
                                if (dashboardData?.sheets && dashboardData.sheets.length > 0) {
                                  setSelectedSheetId(dashboardData.sheets[0].id);
                                  setCreateNewSheet(false);
                                } else {
                                  // Use default sheet for backward compatibility
                                  setSelectedSheetId('default');
                                  setCreateNewSheet(false);
                                }
                                setStep('confirm');
                              }}
                            >
                              <BarChart3 className="h-4 w-4" />
                              <span className="flex-1">{dashboard.name}</span>
                              <span className="text-xs text-muted-foreground">
                                {dashboard.sheets?.length || 1} sheet{(dashboard.sheets?.length || 1) === 1 ? '' : 's'}
                              </span>
                            </button>
                          ))
                        ) : (
                          <div className="px-3 py-2 text-sm text-muted-foreground">
                            No dashboards found
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Recently Created Dashboards */}
              {recentDashboards.length > 0 && !searchQuery && (
                <div className="space-y-2">
                  <Label className="text-sm font-medium flex items-center gap-2">
                    <Clock className="h-4 w-4" />
                    Recently Created Dashboards
                  </Label>
                  <div className="space-y-1">
                    {recentDashboards.map((dashboard) => (
                      <button
                        key={dashboard.id}
                        className="w-full px-3 py-2 text-left hover:bg-gray-50 rounded-md border flex items-center gap-3 transition-colors border-gray-200"
                        onClick={() => {
                          setSelectedDashboard(dashboard.id);
                          setSearchQuery(dashboard.name);
                          setNewDashboardName('');
                          // Set default sheet to first sheet
                          const dashboardData = editableDashboards.find(d => d.id === dashboard.id);
                          if (dashboardData?.sheets && dashboardData.sheets.length > 0) {
                            setSelectedSheetId(dashboardData.sheets[0].id);
                            setCreateNewSheet(false);
                          } else {
                            // Use default sheet for backward compatibility
                            setSelectedSheetId('default');
                            setCreateNewSheet(false);
                          }
                          setStep('confirm');
                        }}
                      >
                        <div className="p-1.5 bg-primary/10 rounded-md">
                          <BarChart3 className="h-4 w-4 text-primary" />
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="font-medium text-sm truncate">{dashboard.name}</div>
                          <div className="text-xs text-muted-foreground">
                            {dashboard.sheets?.length || 1} sheet{(dashboard.sheets?.length || 1) === 1 ? '' : 's'} • Created {new Date(dashboard.createdAt).toLocaleDateString()}
                          </div>
                        </div>
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {/* Create New Dashboard */}
              <div className="space-y-2">
                <Label className="text-sm font-medium">Create New Dashboard</Label>
                <Input
                  placeholder="Enter dashboard name..."
                  value={newDashboardName}
                  onChange={(e) => setNewDashboardName(e.target.value)}
                />
              </div>

              {/* Actions */}
              <div className="flex gap-2 pt-4">
                <Button
                  onClick={async () => {
                    if (newDashboardName.trim()) {
                      // Create new dashboard and add chart directly, skip confirmation step
                      try {
                        console.log('Creating new dashboard:', newDashboardName.trim());
                        const newDashboard = await createDashboard(newDashboardName.trim());
                        // New dashboards have a default "Overview" sheet, so we can add directly
                        await addChartToDashboard(newDashboard.id, chart);
                        toast({
                          title: 'Success',
                          description: `Dashboard "${newDashboardName.trim()}" created and chart added successfully.`,
                        });
                        onClose();
                      } catch (error: any) {
                        console.error('Failed to create dashboard:', error);
                        const errorMessage = error?.message || 'Failed to create dashboard';
                        toast({
                          title: 'Error',
                          description: errorMessage,
                          variant: 'destructive',
                        });
                        // If it's a duplicate name error, keep the modal open so user can change the name
                        if (errorMessage.includes('already exists')) {
                          // Don't close the modal, let user try again with a different name
                          return;
                        }
                      }
                    }
                  }}
                  disabled={!newDashboardName.trim()}
                  className="flex-1"
                >
                  Create New Dashboard
                </Button>
                <Button variant="outline" onClick={onClose}>
                  Cancel
                </Button>
              </div>
            </>
          ) : (
            <>
              {/* Confirmation View */}
              <div className="space-y-4">
                <div className="p-4 bg-gray-50 rounded-lg">
                  <h4 className="font-medium text-sm mb-2">Selected Dashboard:</h4>
                  <div className="flex items-center gap-3">
                    <div className="p-1.5 bg-primary/10 rounded-md">
                      <BarChart3 className="h-4 w-4 text-primary" />
                    </div>
                    <div>
                      <div className="font-medium text-sm">
                        {selectedDashboard ? 
                          editableDashboards.find(d => d.id === selectedDashboard)?.name : 
                          newDashboardName
                        }
                      </div>
                      <div className="text-xs text-muted-foreground">
                        {selectedDashboard ? 
                          `${editableDashboards.find(d => d.id === selectedDashboard)?.sheets?.length || 1} sheet${(editableDashboards.find(d => d.id === selectedDashboard)?.sheets?.length || 1) === 1 ? '' : 's'}` : 
                          'New dashboard'
                        }
                      </div>
                    </div>
                  </div>
                </div>

                {/* Sheet Selection (only for existing dashboards) */}
                {selectedDashboard && (
                  <div className="space-y-3">
                    <Label className="text-sm font-medium">Select Sheet:</Label>
                    <RadioGroup value={createNewSheet ? 'new' : (selectedSheetId || sheets[0]?.id || '')} onValueChange={(value) => {
                      if (value === 'new') {
                        setCreateNewSheet(true);
                        setSelectedSheetId('');
                      } else {
                        setCreateNewSheet(false);
                        setSelectedSheetId(value);
                      }
                    }}>
                      {sheets.map((sheet) => (
                        <div key={sheet.id} className="flex items-center space-x-2">
                          <RadioGroupItem value={sheet.id} id={sheet.id} />
                          <Label htmlFor={sheet.id} className="flex-1 cursor-pointer">
                            <div className="font-medium">{sheet.name}</div>
                            <div className="text-xs text-muted-foreground">
                              {sheet.charts.length} charts
                            </div>
                          </Label>
                        </div>
                      ))}
                      <div className="flex items-center space-x-2">
                        <RadioGroupItem value="new" id="new-sheet" />
                        <Label htmlFor="new-sheet" className="flex-1 cursor-pointer">
                          <div className="font-medium flex items-center gap-2">
                            <Plus className="h-4 w-4" />
                            Create New Sheet
                          </div>
                        </Label>
                      </div>
                    </RadioGroup>

                    {createNewSheet && (
                      <div className="ml-6 space-y-2">
                        <Label htmlFor="new-sheet-name" className="text-sm">Sheet Name:</Label>
                        <Input
                          id="new-sheet-name"
                          placeholder="Enter sheet name..."
                          value={newSheetName}
                          onChange={(e) => setNewSheetName(e.target.value)}
                        />
                      </div>
                    )}
                  </div>
                )}

                {/* Actions */}
                <div className="flex gap-2 pt-4">
                  <Button
                    onClick={async () => {
                      console.log('Button clicked:', { selectedDashboard, newDashboardName, chart, selectedSheetId, createNewSheet, newSheetName });
                      if (selectedDashboard) {
                        // Add to existing dashboard
                        let targetSheetId = selectedSheetId;
                        
                        // If creating a new sheet, create it first
                        if (createNewSheet && newSheetName.trim()) {
                          try {
                            const updated = await dashboardsApi.addSheet(selectedDashboard, newSheetName.trim());
                            const newSheet = updated.sheets?.find(s => s.name === newSheetName.trim());
                            if (newSheet) {
                              targetSheetId = newSheet.id;
                            }
                            await refetch();
                          } catch (error) {
                            console.error('Failed to create sheet:', error);
                            return;
                          }
                        }
                        
                        console.log('Adding to existing dashboard:', selectedDashboard, 'sheet:', targetSheetId);
                        // If targetSheetId is 'default' but dashboard doesn't have sheets, pass undefined to let backend handle it
                        const finalSheetId = (targetSheetId === 'default' && selectedDashboardData?.sheets && selectedDashboardData.sheets.length === 0) 
                          ? undefined 
                          : targetSheetId || undefined;
                        await addChartToDashboard(selectedDashboard, chart, finalSheetId);
                        toast({
                          title: 'Success',
                          description: 'Chart added to dashboard successfully.',
                        });
                        onClose();
                      } else if (newDashboardName.trim()) {
                        // Create new dashboard and add chart
                        try {
                          console.log('Creating new dashboard:', newDashboardName.trim());
                          const newDashboard = await createDashboard(newDashboardName.trim());
                          // New dashboards have a default "Overview" sheet, so we can add directly
                          await addChartToDashboard(newDashboard.id, chart);
                          toast({
                            title: 'Success',
                            description: `Dashboard "${newDashboardName.trim()}" created and chart added successfully.`,
                          });
                          onClose();
                        } catch (error: any) {
                          console.error('Failed to create dashboard:', error);
                          const errorMessage = error?.message || 'Failed to create dashboard';
                          toast({
                            title: 'Error',
                            description: errorMessage,
                            variant: 'destructive',
                          });
                          // If it's a duplicate name error, keep the modal open so user can change the name
                          if (errorMessage.includes('already exists')) {
                            // Don't close the modal, let user try again with a different name
                            return;
                          }
                        }
                      }
                    }}
                    className="flex-1"
                    disabled={createNewSheet && !newSheetName.trim()}
                  >
                    {selectedDashboard ? 'Add to Dashboard' : 'Create New Dashboard'}
                  </Button>
                  <Button 
                    variant="outline" 
                    onClick={() => setStep('select')}
                    className="flex items-center gap-2"
                  >
                    <ArrowLeft className="h-4 w-4" />
                    Back
                  </Button>
                </div>
              </div>
            </>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}