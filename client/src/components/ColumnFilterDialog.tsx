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

export type FilterOperator = 
  | '=' 
  | '!=' 
  | '>' 
  | '>=' 
  | '<' 
  | '<=' 
  | 'contains' 
  | 'startsWith' 
  | 'endsWith' 
  | 'between' 
  | 'in';

export interface FilterCondition {
  column: string;
  operator: FilterOperator;
  value?: any;
  value2?: any; // For 'between'
  values?: any[]; // For 'in'
}

interface ColumnFilterDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  column: string;
  columnType: 'numeric' | 'text' | 'date';
  uniqueValues?: any[]; // For showing unique values in the column
  onApply: (condition: FilterCondition) => void;
  existingFilter?: FilterCondition | null;
}

export function ColumnFilterDialog({
  open,
  onOpenChange,
  column,
  columnType,
  uniqueValues = [],
  onApply,
  existingFilter,
}: ColumnFilterDialogProps) {
  const [operator, setOperator] = useState<FilterOperator>(
    existingFilter?.operator || (columnType === 'numeric' ? '=' : 'contains')
  );
  const [value, setValue] = useState<string>(
    existingFilter?.value !== undefined ? String(existingFilter.value) : ''
  );
  const [value2, setValue2] = useState<string>(
    existingFilter?.value2 !== undefined ? String(existingFilter.value2) : ''
  );
  const [selectedValues, setSelectedValues] = useState<string[]>(
    existingFilter?.values?.map(v => String(v)) || []
  );

  // Reset form when dialog opens/closes or column changes
  useEffect(() => {
    if (open) {
      if (existingFilter) {
        setOperator(existingFilter.operator);
        setValue(existingFilter.value !== undefined ? String(existingFilter.value) : '');
        setValue2(existingFilter.value2 !== undefined ? String(existingFilter.value2) : '');
        setSelectedValues(existingFilter.values?.map(v => String(v)) || []);
      } else {
        setOperator(columnType === 'numeric' ? '=' : 'contains');
        setValue('');
        setValue2('');
        setSelectedValues([]);
      }
    }
  }, [open, column, existingFilter, columnType]);

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

  const handleApply = () => {
    if (operator === 'between') {
      if (!value || !value2) {
        return; // Both values required
      }
      onApply({
        column,
        operator: 'between',
        value: columnType === 'numeric' ? Number(value) : value,
        value2: columnType === 'numeric' ? Number(value2) : value2,
      });
    } else if (operator === 'in') {
      if (selectedValues.length === 0) {
        return; // At least one value required
      }
      onApply({
        column,
        operator: 'in',
        values: selectedValues.map(v => columnType === 'numeric' ? Number(v) : v),
      });
    } else {
      if (!value) {
        return; // Value required
      }
      onApply({
        column,
        operator,
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

  // Limit unique values display to first 100
  const displayUniqueValues = uniqueValues.slice(0, 100);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Filter Column: {column}</DialogTitle>
          <DialogDescription>
            Set filter conditions for this column. The filtered data will become your working dataset.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label htmlFor="operator">Filter Operator</Label>
            <Select value={operator} onValueChange={(val) => setOperator(val as FilterOperator)}>
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
                {displayUniqueValues.length > 0 ? (
                  <div className="space-y-2">
                    {displayUniqueValues.map((val, idx) => {
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
                    {uniqueValues.length > 100 && (
                      <p className="text-xs text-gray-500 pt-2">
                        Showing first 100 of {uniqueValues.length} unique values
                      </p>
                    )}
                  </div>
                ) : (
                  <p className="text-sm text-gray-500">No unique values available</p>
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
                    {displayUniqueValues.slice(0, 20).map((val, idx) => (
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
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleClear}>
            Clear
          </Button>
          <Button
            onClick={handleApply}
            disabled={
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
