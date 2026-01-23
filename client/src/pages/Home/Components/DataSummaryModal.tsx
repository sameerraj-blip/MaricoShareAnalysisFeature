import { useState, useEffect } from 'react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { 
  Database, 
  AlertCircle, 
  CheckCircle2, 
  TrendingUp, 
  Loader2,
  Sparkles,
  X
} from 'lucide-react';
import { dataApi } from '@/lib/api/data';
import { useToast } from '@/hooks/use-toast';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';

interface ColumnSummary {
  variable: string;
  datatype: string;
  total_values: number;
  null_values: number;
  non_null_values: number;
  mean?: number | null;
  median?: number | null;
  mode?: any;
  std_dev?: number | null;
  min?: number | string | null;
  max?: number | string | null;
}

interface DataSummaryResponse {
  summary: ColumnSummary[];
  qualityScore: number;
  recommendedQuestions: string[];
}

interface DataSummaryModalProps {
  isOpen: boolean;
  onClose: () => void;
  sessionId: string | null;
  onSendMessage?: (message: string) => void;
}

export function DataSummaryModal({ 
  isOpen, 
  onClose, 
  sessionId,
  onSendMessage 
}: DataSummaryModalProps) {
  const [loading, setLoading] = useState(false);
  const [dataSummary, setDataSummary] = useState<DataSummaryResponse | null>(null);
  const [dataOpsInput, setDataOpsInput] = useState('');
  const [dataOpsLoading, setDataOpsLoading] = useState(false);
  const { toast } = useToast();

  useEffect(() => {
    if (isOpen && sessionId) {
      loadDataSummary();
    } else {
      setDataSummary(null);
      setDataOpsInput('');
    }
  }, [isOpen, sessionId]);

  const loadDataSummary = async () => {
    if (!sessionId) return;
    
    setLoading(true);
    try {
      const response = await dataApi.getDataSummary(sessionId);
      setDataSummary(response);
    } catch (error) {
      console.error('Failed to load data summary:', error);
      toast({
        title: 'Error',
        description: 'Failed to load data summary. Please try again.',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  const handleDataOperation = async () => {
    if (!dataOpsInput.trim() || !sessionId || !onSendMessage) return;

    setDataOpsLoading(true);
    try {
      // Close modal and send the data operation message
      onClose();
      onSendMessage(dataOpsInput.trim());
      setDataOpsInput('');
      toast({
        title: 'Data Operation Sent',
        description: 'Your data operation request has been sent to the chat.',
      });
    } catch (error) {
      console.error('Failed to send data operation:', error);
      toast({
        title: 'Error',
        description: 'Failed to send data operation. Please try again.',
        variant: 'destructive',
      });
    } finally {
      setDataOpsLoading(false);
    }
  };

  const handleQuestionClick = (question: string) => {
    if (onSendMessage) {
      onClose();
      onSendMessage(question);
    }
  };

  const calculateQualityScore = (summary: ColumnSummary[]): number => {
    if (!summary || summary.length === 0) return 0;
    
    const totalCells = summary.reduce((sum, col) => sum + col.total_values, 0);
    const totalNulls = summary.reduce((sum, col) => sum + col.null_values, 0);
    
    if (totalCells === 0) return 0;
    
    const nullPercentage = (totalNulls / totalCells) * 100;
    // Quality score: 100 - null percentage (capped at 0)
    return Math.max(0, Math.round(100 - nullPercentage));
  };

  // Use backend quality score if available, otherwise calculate it
  const qualityScore = dataSummary 
    ? (dataSummary.qualityScore ?? calculateQualityScore(dataSummary.summary))
    : 0;

  const getQualityColor = (score: number) => {
    if (score >= 90) return 'text-green-600 bg-green-50';
    if (score >= 70) return 'text-yellow-600 bg-yellow-50';
    return 'text-red-600 bg-red-50';
  };

  const getQualityLabel = (score: number) => {
    if (score >= 90) return 'Excellent';
    if (score >= 70) return 'Good';
    if (score >= 50) return 'Fair';
    return 'Poor';
  };

  const formatValue = (value: any): string => {
    if (value === null || value === undefined) return 'N/A';
    if (typeof value === 'number') {
      if (Number.isInteger(value)) return value.toString();
      return value.toFixed(2);
    }
    return String(value);
  };

  return (
    <Dialog open={isOpen} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-w-[95vw] w-[95vw] max-h-[95vh] h-[95vh] p-6 flex flex-col">
        <DialogHeader className="flex-shrink-0">
          <DialogTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Data Summary
          </DialogTitle>
          <DialogDescription>
            View data quality metrics, statistics, and perform data operations
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 overflow-hidden flex flex-col min-h-0">
          {loading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
            </div>
          ) : dataSummary ? (
            <Tabs defaultValue="summary" className="w-full h-full flex flex-col min-h-0">
              <TabsList className="grid w-full grid-cols-3 flex-shrink-0">
                <TabsTrigger value="summary">Summary</TabsTrigger>
                <TabsTrigger value="operations">Data Operations</TabsTrigger>
                <TabsTrigger value="questions">Recommended Questions</TabsTrigger>
              </TabsList>

            <TabsContent value="summary" className="space-y-4 mt-4 flex-1 overflow-hidden flex flex-col min-h-0">
              {/* Quality Score Card */}
              <Card className="flex-shrink-0">
                <CardHeader>
                  <CardTitle className="flex items-center justify-between">
                    <span>Data Quality Score</span>
                    <Badge className={`${getQualityColor(qualityScore)} text-sm font-semibold px-3 py-1`}>
                      {qualityScore}/100 - {getQualityLabel(qualityScore)}
                    </Badge>
                  </CardTitle>
                  <CardDescription>
                    Based on null value percentage across all columns
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="w-full bg-gray-200 rounded-full h-3">
                    <div
                      className={`h-3 rounded-full transition-all ${
                        qualityScore >= 90 ? 'bg-green-500' :
                        qualityScore >= 70 ? 'bg-yellow-500' : 'bg-red-500'
                      }`}
                      style={{ width: `${qualityScore}%` }}
                    />
                  </div>
                  <p className="text-sm text-muted-foreground mt-2">
                    {dataSummary.summary.reduce((sum, col) => sum + col.null_values, 0)} null values out of{' '}
                    {dataSummary.summary.reduce((sum, col) => sum + col.total_values, 0)} total cells
                  </p>
                </CardContent>
              </Card>

              {/* Summary Table */}
              <Card className="flex-1 overflow-hidden flex flex-col min-h-0">
                <CardHeader className="flex-shrink-0">
                  <CardTitle>Column Statistics</CardTitle>
                  <CardDescription>
                    Detailed statistics for each column in your dataset
                  </CardDescription>
                </CardHeader>
                <CardContent className="flex-1 overflow-hidden min-h-0 p-0">
                  <div className="h-full overflow-auto border-t">
                    <div className="overflow-x-auto">
                      <Table>
                        <TableHeader className="sticky top-0 bg-background z-10">
                          <TableRow>
                            <TableHead className="bg-muted/50">Column</TableHead>
                            <TableHead className="bg-muted/50">Type</TableHead>
                            <TableHead className="bg-muted/50">Null Values</TableHead>
                            <TableHead className="bg-muted/50">Mean</TableHead>
                            <TableHead className="bg-muted/50">Median</TableHead>
                            <TableHead className="bg-muted/50">Mode</TableHead>
                            <TableHead className="bg-muted/50">Std Dev</TableHead>
                            <TableHead className="bg-muted/50">Min</TableHead>
                            <TableHead className="bg-muted/50">Max</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {dataSummary.summary.map((col, idx) => (
                            <TableRow key={idx}>
                              <TableCell className="font-medium">{col.variable}</TableCell>
                              <TableCell>
                                <Badge variant="outline">{col.datatype}</Badge>
                              </TableCell>
                              <TableCell>
                                <div className="flex items-center gap-2">
                                  {col.null_values > 0 ? (
                                    <AlertCircle className="h-4 w-4 text-yellow-600" />
                                  ) : (
                                    <CheckCircle2 className="h-4 w-4 text-green-600" />
                                  )}
                                  <span>{col.null_values}</span>
                                </div>
                              </TableCell>
                              <TableCell>{formatValue(col.mean)}</TableCell>
                              <TableCell>{formatValue(col.median)}</TableCell>
                              <TableCell className="max-w-[150px] truncate">
                                {formatValue(col.mode)}
                              </TableCell>
                              <TableCell>{formatValue(col.std_dev)}</TableCell>
                              <TableCell>{formatValue(col.min)}</TableCell>
                              <TableCell>{formatValue(col.max)}</TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="operations" className="space-y-4 mt-4 flex-1 overflow-auto">
              <Card>
                <CardHeader>
                  <CardTitle>Fix Data Issues</CardTitle>
                  <CardDescription>
                    Perform data operations like imputation, feature creation, deletion, etc.
                    Type your request in natural language, just like in the chat.
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="space-y-2">
                    <Textarea
                      placeholder="E.g., Fill null values in price column with mean, Remove column 'notes', Create new column 'total' = price * quantity..."
                      value={dataOpsInput}
                      onChange={(e) => setDataOpsInput(e.target.value)}
                      className="min-h-[120px] resize-none"
                      disabled={dataOpsLoading || !onSendMessage}
                    />
                    <div className="flex gap-2">
                      <Button
                        onClick={handleDataOperation}
                        disabled={!dataOpsInput.trim() || dataOpsLoading || !onSendMessage}
                        className="flex-1"
                      >
                        {dataOpsLoading ? (
                          <>
                            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                            Processing...
                          </>
                        ) : (
                          'Execute Operation'
                        )}
                      </Button>
                    </div>
                    <p className="text-sm text-muted-foreground">
                      Examples: "Fill nulls with mean", "Remove column X", "Create column Y = A + B"
                    </p>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="questions" className="space-y-4 mt-4 flex-1 overflow-auto">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Sparkles className="h-5 w-5" />
                    Recommended Questions
                  </CardTitle>
                  <CardDescription>
                    AI-generated questions based on your data context
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="grid gap-2">
                    {(dataSummary.recommendedQuestions || []).map((question, idx) => (
                      <Button
                        key={idx}
                        variant="outline"
                        className="w-full justify-start text-left h-auto py-3 px-4 hover:bg-primary/5 hover:border-primary/50 transition-colors"
                        onClick={() => handleQuestionClick(question)}
                        disabled={!onSendMessage}
                      >
                        <span className="flex-1">{question}</span>
                        <TrendingUp className="h-4 w-4 ml-2 text-muted-foreground" />
                      </Button>
                    ))}
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
            </Tabs>
          ) : (
            <div className="text-center py-8 text-muted-foreground">
              No data summary available
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
