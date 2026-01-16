import { useMemo, useState } from 'react';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Download, Loader2 } from 'lucide-react';
import { downloadModifiedDataset } from '@/lib/api';
import { useToast } from '@/hooks/use-toast';

interface DataPreviewTableProps {
  data: Record<string, any>[];
  title?: string;
  maxRows?: number;
  sessionId?: string | null; // Session ID for downloading the full modified dataset
}

export function DataPreviewTable({ 
  data, 
  title, 
  maxRows = 100, 
  sessionId,
}: DataPreviewTableProps) {
  const [downloadingFormat, setDownloadingFormat] = useState<'csv' | 'xlsx' | null>(null);
  const { toast } = useToast();
  
  const displayData = useMemo(() => {
    if (!data || data.length === 0) return [];
    return data.slice(0, maxRows);
  }, [data, maxRows]);

  const handleDownload = async (format: 'csv' | 'xlsx') => {
    if (!sessionId) {
      toast({
        title: 'Error',
        description: 'Session ID is required to download the dataset',
        variant: 'destructive',
      });
      return;
    }

    setDownloadingFormat(format);
    try {
      await downloadModifiedDataset(sessionId, format);
      toast({
        title: 'Success',
        description: `Dataset downloaded as ${format.toUpperCase()}`,
      });
    } catch (error: any) {
      toast({
        title: 'Download Failed',
        description: error?.message || 'Failed to download dataset',
        variant: 'destructive',
      });
    } finally {
      setDownloadingFormat(null);
    }
  };

  // Early return after all hooks
  if (!data || data.length === 0 || !data[0]) {
    return (
      <Card className="p-4">
        <p className="text-sm text-gray-500">No data to display</p>
      </Card>
    );
  }

  const columns = Object.keys(data[0]);

  return (
    <Card className="p-4 mt-2">
      {(title || sessionId) && (
        <div className="flex items-center justify-between mb-3">
          {title && (
            <h4 className="text-sm font-semibold text-gray-900">{title}</h4>
          )}
          {sessionId && (
            <div className="flex gap-2 ml-auto">
              <Button
                variant="outline"
                size="sm"
                onClick={() => handleDownload('csv')}
                disabled={downloadingFormat !== null}
                className="text-xs"
              >
                {downloadingFormat === 'csv' ? (
                  <>
                    <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                    Downloading...
                  </>
                ) : (
                  <>
                    <Download className="h-3 w-3 mr-1" />
                    Download CSV
                  </>
                )}
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => handleDownload('xlsx')}
                disabled={downloadingFormat !== null}
                className="text-xs"
              >
                {downloadingFormat === 'xlsx' ? (
                  <>
                    <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                    Downloading...
                  </>
                ) : (
                  <>
                    <Download className="h-3 w-3 mr-1" />
                    Download Excel
                  </>
                )}
              </Button>
            </div>
          )}
        </div>
      )}
      <div className="overflow-x-auto max-h-[500px] overflow-y-auto border border-gray-200 rounded-md">
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 bg-gray-50 z-10">
            <tr className="border-b border-gray-200">
              {columns.map((col) => (
                <th
                  key={col}
                  className="px-3 py-2 text-left font-semibold text-gray-700 bg-gray-50"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {displayData.map((row, idx) => (
              <tr
                key={idx}
                className="border-b border-gray-100 hover:bg-gray-50 transition-colors"
              >
                {columns.map((col) => (
                  <td key={col} className="px-3 py-2 text-gray-700">
                    {row[col] !== null && row[col] !== undefined
                      ? String(row[col])
                      : <span className="text-gray-400 italic">null</span>}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data.length > maxRows && (
        <p className="text-xs text-gray-500 mt-2">
          Showing {maxRows} of {data.length} rows
        </p>
      )}
    </Card>
  );
}

interface DataSummaryTableProps {
  summary: Array<{
    variable: string;
    datatype: string;
    total_values: number;
    null_values: number;
    non_null_values: number;
    mean?: number | null;
    median?: number | null;
    std_dev?: number | null;
    min?: number | null;
    max?: number | null;
    mode?: any;
  }>;
}

export function DataSummaryTable({ summary }: DataSummaryTableProps) {
  if (!summary || summary.length === 0) {
    return (
      <Card className="p-4">
        <p className="text-sm text-gray-500">No summary data available</p>
      </Card>
    );
  }

  return (
    <Card className="p-4 mt-2">
      <h4 className="text-sm font-semibold mb-3 text-gray-900">Data Summary</h4>
      <div className="overflow-x-auto max-h-[500px] overflow-y-auto border border-gray-200 rounded-md">
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 bg-gray-50 z-10">
            <tr className="border-b border-gray-200">
              <th className="px-3 py-2 text-left font-semibold text-gray-700 bg-gray-50">Variable</th>
              <th className="px-3 py-2 text-left font-semibold text-gray-700 bg-gray-50">Datatype</th>
              <th className="px-3 py-2 text-left font-semibold text-gray-700 bg-gray-50">#Values</th>
              <th className="px-3 py-2 text-left font-semibold text-gray-700 bg-gray-50">#Nulls</th>
              <th className="px-3 py-2 text-left font-semibold text-gray-700 bg-gray-50">Mean</th>
              <th className="px-3 py-2 text-left font-semibold text-gray-700 bg-gray-50">Median</th>
              <th className="px-3 py-2 text-left font-semibold text-gray-700 bg-gray-50">Mode</th>
              <th className="px-3 py-2 text-left font-semibold text-gray-700 bg-gray-50">STD Dev</th>
            </tr>
          </thead>
          <tbody>
            {summary.map((row, idx) => (
              <tr
                key={idx}
                className="border-b border-gray-100 hover:bg-gray-50 transition-colors"
              >
                <td className="px-3 py-2 text-gray-700 font-medium">{row.variable}</td>
                <td className="px-3 py-2 text-gray-600">{row.datatype}</td>
                <td className="px-3 py-2 text-gray-700">{row.total_values}</td>
                <td className="px-3 py-2 text-gray-700">{row.null_values}</td>
                <td className="px-3 py-2 text-gray-700">
                  {row.mean !== null && row.mean !== undefined
                    ? typeof row.mean === 'number'
                      ? row.mean.toFixed(2)
                      : String(row.mean)
                    : '-'}
                </td>
                <td className="px-3 py-2 text-gray-700">
                  {row.median !== null && row.median !== undefined
                    ? typeof row.median === 'number'
                      ? row.median.toFixed(2)
                      : String(row.median)
                    : '-'}
                </td>
                <td className="px-3 py-2 text-gray-700">
                  {row.mode !== null && row.mode !== undefined
                    ? String(row.mode)
                    : '-'}
                </td>
                <td className="px-3 py-2 text-gray-700">
                  {row.std_dev !== null && row.std_dev !== undefined
                    ? typeof row.std_dev === 'number'
                      ? row.std_dev.toFixed(2)
                      : String(row.std_dev)
                    : '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

