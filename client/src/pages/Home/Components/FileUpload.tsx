import { useCallback, useEffect, useRef } from 'react';
import { useDropzone } from 'react-dropzone';
import { Upload, FileSpreadsheet, Loader2 } from 'lucide-react';
import { Card } from '@/components/ui/card';


interface FileUploadProps {
  onFileSelect: (file: File) => void;
  isUploading: boolean;
  autoOpenTrigger?: number;
}

export function FileUpload({ onFileSelect, isUploading, autoOpenTrigger = 0 }: FileUploadProps) {
  const onDrop = useCallback((acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      const file = acceptedFiles[0];
      if (file.size > 500 * 1024 * 1024) {
        alert('File size must be less than 500MB');
        return;
      }
      onFileSelect(file);
    }
  }, [onFileSelect]);

  const { getRootProps, getInputProps, isDragActive, open } = useDropzone({
    onDrop,
    accept: {
      'text/csv': ['.csv'],
      'application/vnd.ms-excel': ['.xls'],
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
    },
    multiple: false,
    disabled: isUploading,
  });

  // Programmatically open the file dialog when trigger changes
  // Only open when explicitly triggered (autoOpenTrigger > 0), not on initial render
  const lastTriggerRef = useRef<number>(0);
  const isInitialMount = useRef<boolean>(true);
  
  useEffect(() => {
    // Skip on initial mount to prevent auto-opening when component first renders
    if (isInitialMount.current) {
      isInitialMount.current = false;
      return;
    }
    
    // Only open once per unique trigger value; avoid re-open on cancel
    // Only open when autoOpenTrigger is explicitly set (> 0)
    if (autoOpenTrigger > 0 && autoOpenTrigger !== lastTriggerRef.current && !isUploading) {
      lastTriggerRef.current = autoOpenTrigger;
      try { 
        open(); 
      } catch (error) {
        console.error('Failed to open file dialog:', error);
      }
    }
  }, [autoOpenTrigger, isUploading, open]);

  return (
    <div className="h-[calc(100vh-80px)] bg-gradient-to-br from-slate-50 to-white flex items-center justify-center p-4">
      <div className="w-full max-w-2xl">
        {/* Header Section */}
        <div className="text-center mb-6">
          <h1 className="text-2xl font-bold text-gray-900 mb-2">
            Welcome to Marico Insight
          </h1>
          <p className="text-base text-gray-600">
            Upload your data and get instant AI-powered insights and visualizations
          </p>
        </div>

        {/* Upload Area */}
        <Card
          {...getRootProps()}
          className={`
            cursor-pointer transition-all duration-300 rounded-2xl border-2 border-dashed
            ${isDragActive 
              ? 'border-primary bg-primary/5 scale-[1.01] shadow-lg' 
              : 'border-gray-200 hover:border-primary/50 hover:bg-primary/2'
            }
            ${isUploading ? 'opacity-50 cursor-not-allowed' : ''}
            bg-white shadow-sm hover:shadow-md
          `}
          data-testid="file-upload-zone"
        >
          <input {...getInputProps()} data-testid="file-input" />
          <div className="flex flex-col items-center justify-center py-10 px-6">
            {isUploading ? (
              <>
                <div className="relative mb-4">
                  <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center">
                    <Loader2 className="w-8 h-8 text-primary animate-spin" />
                  </div>
                </div>
                <h3 className="text-lg font-semibold text-gray-900 mb-1">Analyzing your data...</h3>
                <p className="text-sm text-gray-500">This may take a moment</p>
              </>
            ) : (
              <>
                <div className="relative mb-6">
                  <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center shadow-inner">
                    <Upload className="w-8 h-8 text-primary" />
                  </div>
                  {isDragActive && (
                    <div className="absolute -top-1 -right-1 w-5 h-5 bg-green-500 rounded-full flex items-center justify-center">
                      <div className="w-2 h-2 bg-white rounded-full"></div>
                    </div>
                  )}
                </div>
                <h3 className="text-xl font-semibold text-gray-900 mb-2">
                  {isDragActive ? 'Drop your file here' : 'Drag & drop your data file'}
                </h3>
                <p className="text-gray-500 mb-4">
                  or click to browse
                </p>
                <div className="flex items-center gap-2 text-xs text-gray-400 bg-gray-50 px-3 py-1.5 rounded-full">
                  <FileSpreadsheet className="w-3 h-3" />
                  <span>Supports CSV, XLS, XLSX</span>
                  <span>•</span>
                  <span>Max 500MB</span>
                </div>
              </>
            )}
          </div>
        </Card>

        {/* Features Section */}
        <div className="mt-6 grid grid-cols-1 md:grid-cols-3 gap-3">
          <div className="text-center p-4 rounded-xl bg-white shadow-sm hover:shadow-md transition-shadow">
            <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center mx-auto mb-3">
              <span className="text-lg">📊</span>
            </div>
            <h4 className="font-semibold text-gray-900 mb-1 text-sm">Smart Charts</h4>
            <p className="text-xs text-gray-500">Auto-generated visualizations</p>
          </div>
          <div className="text-center p-4 rounded-xl bg-white shadow-sm hover:shadow-md transition-shadow">
            <div className="w-10 h-10 bg-yellow-100 rounded-lg flex items-center justify-center mx-auto mb-3">
              <span className="text-lg">💡</span>
            </div>
            <h4 className="font-semibold text-gray-900 mb-1 text-sm">AI Insights</h4>
            <p className="text-xs text-gray-500">Actionable suggestions</p>
          </div>
          <div className="text-center p-4 rounded-xl bg-white shadow-sm hover:shadow-md transition-shadow">
            <div className="w-10 h-10 bg-green-100 rounded-lg flex items-center justify-center mx-auto mb-3">
              <span className="text-lg">💬</span>
            </div>
            <h4 className="font-semibold text-gray-900 mb-1 text-sm">Natural Language</h4>
            <p className="text-xs text-gray-500">Ask questions about your data</p>
          </div>
        </div>
      </div>
    </div>
  );
}
