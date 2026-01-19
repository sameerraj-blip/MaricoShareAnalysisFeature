import { useState, useEffect, useMemo } from 'react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Input } from '@/components/ui/input';
import { Checkbox } from '@/components/ui/checkbox';
import { FilterCondition } from './ColumnFilterDialog';

interface FilterDataModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  columns: string[];
  numericColumns?: string[];
  dateColumns?: string[];
  data?: Record<string, any>[]; // Sample data to extract unique values
  onApply: (condition: FilterCondition) => void;
}

export function FilterDataModal({
  open,
  onOpenChange,
  columns = [],
  numericColumns = [],
  dateColumns = [],
  data = [],
  onApply,
}: FilterDataModalProps) {
  const [selectedColumn, setSelectedColumn] = useState<string>('');
  const [operator, setOperator] = useState<string>('=');
  const [value, setValue] = useState<string>('');
  const [value2, setValue2] = useState<string>('');
  const [selectedValues, setSelectedValues] = useState<string[]>([]);

  // Reset form when dialog opens/closes
  useEffect(() => {
    if (open) {
      if (columns.length > 0 && !selectedColumn) {
        setSelectedColumn(columns[0]);
      }
      setOperator('=');
      setValue('');
      setValue2('');
      setSelectedValues([]);
    }
  }, [open, columns, selectedColumn]);

  // Get column type
  const columnType = useMemo(() => {
    if (!selectedColumn) return 'text';
    if (numericColumns.includes(selectedColumn)) return 'numeric';
    if (dateColumns.includes(selectedColumn)) return 'date';
    return 'text';
  }, [selectedColumn, numericColumns, dateColumns]);

  // Get unique values for selected column
  const uniqueValues = useMemo(() => {
    if (!selectedColumn || !data || data.length === 0) return [];
    const sampleValues = data.slice(0, 1000).map(row => row[selectedColumn]).filter(v => v !== null && v !== undefined);
    const uniqueSet = new Set<string>();
    sampleValues.forEach(v => uniqueSet.add(String(v)));
    return Array.from(uniqueSet).slice(0, 100).sort();
  }, [selectedColumn, data]);

  // Get available operators based on column type
  const availableOperators = useMemo(() => {
    if (columnType === 'numeric') {
      return [
        { value: '=', label: 'Equals' },
        { value: '!=', label: 'Not equals' },
        { value: '>', label: 'Greater than' },
        { value: '>=', label: 'Greater than or equal' },
        { value: '<', label: 'Less than' },
        { value: '<=', label: 'Less than or equal' },
        { value: 'between', label: 'Between' },
        { value: 'in', label: 'In (multiple values)' },
      ];
    } else {
      return [
        { value: '=', label: 'Equals' },
        { value: '!=', label: 'Not equals' },
        { value: 'contains', label: 'Contains' },
        { value: 'startsWith', label: 'Starts with' },
        { value: 'endsWith', label: 'Ends with' },
        { value: 'in', label: 'In (multiple values)' },
      ];
    }
  }, [columnType]);

  // Reset operator when column changes
  useEffect(() => {
    if (selectedColumn) {
      setOperator(columnType === 'numeric' ? '=' : 'contains');
      setValue('');
      setValue2('');
      setSelectedValues([]);
    }
  }, [selectedColumn, columnType]);

  const handleApply = () => {
    if (!selectedColumn) return;

    if (operator === 'between') {
      if (!value || !value2) return;
      onApply({
        column: selectedColumn,
        operator: 'between',
        value: columnType === 'numeric' ? Number(value) : value,
        value2: columnType === 'numeric' ? Number(value2) : value2,
      });
    } else if (operator === 'in') {
      if (selectedValues.length === 0) return;
      onApply({
        column: selectedColumn,
        operator: 'in',
        values: selectedValues.map(v => columnType === 'numeric' ? Number(v) : v),
      });
    } else {
      if (!value) return;
      onApply({
        column: selectedColumn,
        operator: operator as FilterCondition['operator'],
        value: columnType === 'numeric' ? Number(value) : value,
      });
    }
    onOpenChange(false);
  };

  const handleClear = () => {
    setValue('');
    setValue2('');
    setSelectedValues([]);
    setOperator(columnType === 'numeric' ? '=' : 'contains');
  };

  const handleValueToggle = (val: string) => {
    setSelectedValues(prev => {
      if (prev.includes(val)) {
        return prev.filter(v => v !== val);
      } else {
        return [...prev, val];
      }
    });
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Filter Data</DialogTitle>
          <DialogDescription>
            Set filter conditions to filter your dataset. The filtered data will become your working dataset for all subsequent queries.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label htmlFor="column">Select Column</Label>
            <Select value={selectedColumn} onValueChange={setSelectedColumn}>
              <SelectTrigger id="column">
                <SelectValue placeholder="Select a column" />
              </SelectTrigger>
              <SelectContent>
                {columns.map((col) => (
                  <SelectItem key={col} value={col}>
                    {col} {numericColumns.includes(col) && '(numeric)'} {dateColumns.includes(col) && '(date)'}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {selectedColumn && (
            <>
              <div className="space-y-2">
                <Label htmlFor="operator">Filter Operator</Label>
                <Select value={operator} onValueChange={setOperator}>
                  <SelectTrigger id="operator">
                    <SelectValue placeholder="Select operator" />
                  </SelectTrigger>
                  <SelectContent>
                    {availableOperators.map((op) => (
                      <SelectItem key={op.value} value={op.value}>
                        {op.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {operator === 'between' && (
                <div className="space-y-2">
                  <div className="grid grid-cols-2 gap-2">
                    <div className="space-y-2">
                      <Label htmlFor="value">From</Label>
                      <Input
                        id="value"
                        type={columnType === 'numeric' ? 'number' : 'text'}
                        value={value}
                        onChange={(e) => setValue(e.target.value)}
                        placeholder="Enter value"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="value2">To</Label>
                      <Input
                        id="value2"
                        type={columnType === 'numeric' ? 'number' : 'text'}
                        value={value2}
                        onChange={(e) => setValue2(e.target.value)}
                        placeholder="Enter value"
                      />
                    </div>
                  </div>
                </div>
              )}

              {operator === 'in' && (
                <div className="space-y-2">
                  <Label>Select Values (select multiple)</Label>
                  <div className="border rounded-md p-2 max-h-60 overflow-y-auto">
                    {uniqueValues.length > 0 ? (
                      <div className="space-y-2">
                        {uniqueValues.map((val, idx) => {
                          const valStr = String(val);
                          return (
                            <div key={idx} className="flex items-center space-x-2">
                              <Checkbox
                                id={`value-${idx}`}
                                checked={selectedValues.includes(valStr)}
                                onCheckedChange={() => handleValueToggle(valStr)}
                              />
                              <Label
                                htmlFor={`value-${idx}`}
                                className="text-sm font-normal cursor-pointer flex-1"
                              >
                                {valStr}
                              </Label>
                            </div>
                          );
                        })}
                      </div>
                    ) : (
                      <p className="text-sm text-gray-500">No unique values available. Enter values manually.</p>
                    )}
                  </div>
                  {selectedValues.length > 0 && (
                    <p className="text-xs text-gray-600">
                      {selectedValues.length} value(s) selected
                    </p>
                  )}
                </div>
              )}

              {operator !== 'between' && operator !== 'in' && (
                <div className="space-y-2">
                  <Label htmlFor="value">Filter Value</Label>
                  <Input
                    id="value"
                    type={columnType === 'numeric' ? 'number' : 'text'}
                    value={value}
                    onChange={(e) => setValue(e.target.value)}
                    placeholder={`Enter ${columnType === 'numeric' ? 'number' : 'text'} value`}
                  />
                  {uniqueValues.length > 0 && uniqueValues.length <= 50 && (
                    <div className="mt-2">
                      <p className="text-xs text-gray-600 mb-1">Quick select:</p>
                      <div className="flex flex-wrap gap-1">
                        {uniqueValues.slice(0, 20).map((val, idx) => (
                          <Button
                            key={idx}
                            variant="outline"
                            size="sm"
                            className="h-7 text-xs"
                            onClick={() => setValue(String(val))}
                          >
                            {String(val)}
                          </Button>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}
            </>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleClear}>
            Clear
          </Button>
          <Button
            onClick={handleApply}
            disabled={
              !selectedColumn ||
              (operator === 'between' && (!value || !value2)) ||
              (operator === 'in' && selectedValues.length === 0) ||
              (operator !== 'between' && operator !== 'in' && !value)
            }
          >
            Apply Filter
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
