import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Box,
  Typography,
  Chip,
  Alert,
  Paper,
  CircularProgress,
  Autocomplete,
} from '@mui/material';
import { Visibility as VisibilityIcon, HelpOutline as HelpIcon } from '@mui/icons-material';
import { rulesAPI, reasoningAPI } from '../api/client';

interface RuleFormData {
  id?: string;
  label?: string;
  labels?: string[];
  description: string;
  filter_conditions: Record<string, string>;
  note: string;
  parent_schema?: string;
  child_table?: string;
}

interface RuleFormProps {
  open: boolean;
  onClose: () => void;
  onSave: () => void;
  rule?: RuleFormData | null;
}

export const RuleForm: React.FC<RuleFormProps> = ({ open, onClose, onSave, rule }) => {
  const [description, setDescription] = useState('');
  const [labels, setLabels] = useState<string[]>([]);
  const [labelInput, setLabelInput] = useState('');
  const [parentSchema, setParentSchema] = useState('');
  const [childTable, setChildTable] = useState('');
  const [parsedPreview, setParsedPreview] = useState<Record<string, any> | null>(null);
  const [parsing, setParsing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showPreview, setShowPreview] = useState(false);

  useEffect(() => {
    if (rule) {
      setDescription(rule.description || rule.note || '');
      setLabels(rule.labels || (rule.label ? [rule.label] : []));
      setParentSchema(rule.parent_schema || '');
      setChildTable(rule.child_table || '');
      setParsedPreview({ filter_conditions: rule.filter_conditions || {} });
    } else {
      setDescription('');
      setLabels([]);
      setLabelInput('');
      setParentSchema('');
      setChildTable('');
      setParsedPreview(null);
      setShowPreview(false);
    }
    setError(null);
  }, [rule, open]);

  const handleAddLabel = () => {
    if (labelInput.trim() && !labels.includes(labelInput.trim())) {
      setLabels([...labels, labelInput.trim()]);
      setLabelInput('');
    }
  };

  const handleRemoveLabel = (labelToRemove: string) => {
    setLabels(labels.filter(l => l !== labelToRemove));
  };

  const handleParseRules = async () => {
    if (!description.trim()) {
      setError('Please enter business rules in natural language');
      return;
    }

    setParsing(true);
    setError(null);

    try {
      const parseQuery = `You are parsing business rules written in natural language. Extract the following information and return ONLY a valid JSON object:

1. Date snapshots (e.g., "collections_mis: 2026-01-08" or "use outstanding_daily from 2026-01-07" → extract as "last_day": "2026-01-07")
2. Data quality filters (e.g., "paid_date IS NOT NULL", "status = SUCCESS", "__is_deleted = false")
3. Business filters (e.g., "settlement_flag = unsettled", "NBFC IN (quadrillion, slicenesfb, nesfb)", "order_type != credin")

Business Rules Text:
${description}

Return a JSON object with this exact structure:
{
  "filter_conditions": {
    "key1": "value1",
    "key2": "value2"
  }
}

Where keys are column names (with table prefix if mentioned, e.g., "repayments.status", "outstanding_daily.settlement_flag") and values are the filter values or conditions (e.g., "SUCCESS", "2026-01-07", "unsettled", "quadrillion, slicenesfb, nesfb", "NOT credin").

IMPORTANT: Return ONLY the JSON object, no other text.`;

      const response = await reasoningAPI.query(parseQuery);
      
      let parsed: Record<string, any> = { filter_conditions: {} };
      
      if (response.data?.result || response.data?.data?.result) {
        const resultText = response.data?.result || response.data?.data?.result || '';
        
        try {
          const jsonMatch = resultText.match(/\{[\s\S]*\}/);
          if (jsonMatch) {
            parsed = JSON.parse(jsonMatch[0]);
          } else {
            parsed = { filter_conditions: {} };
          }
        } catch (e) {
          parsed = { filter_conditions: {} };
        }
      }

      setParsedPreview(parsed);
      setShowPreview(true);
    } catch (err: any) {
      setError('LLM parsing unavailable. Rules will be saved as natural language and parsed during execution.');
      setParsedPreview({ filter_conditions: {} });
    } finally {
      setParsing(false);
    }
  };

  const handleSave = async () => {
    if (!description.trim()) {
      setError('Please enter business rules');
      return;
    }

    const filterConditions = parsedPreview?.filter_conditions || {};

    const ruleData = {
      description: description,
      note: description,
      filter_conditions: filterConditions,
      labels: labels,
      parent_schema: parentSchema || undefined,
      child_table: childTable || undefined,
    };

    setSaving(true);
    setError(null);

    try {
      if (rule?.id) {
        await rulesAPI.update(rule.id, ruleData);
      } else {
        await rulesAPI.create(ruleData);
      }
      onSave();
      onClose();
    } catch (err: any) {
      setError(err.response?.data?.error || err.message || 'Failed to save rule');
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="md"
      fullWidth
      PaperProps={{
        sx: {
          backgroundColor: '#161B22',
          color: '#E6EDF3',
        },
      }}
    >
      <DialogTitle sx={{ borderBottom: '1px solid #30363D', display: 'flex', alignItems: 'center', gap: 1 }}>
        {rule ? 'Edit Rule' : 'Add Rule'}
        <HelpIcon sx={{ fontSize: 18, color: '#8B949E', ml: 1 }} />
      </DialogTitle>
      <DialogContent sx={{ pt: 3, maxHeight: '80vh', overflow: 'auto' }}>
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}

        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
          {/* Labels */}
          <Box>
            <Typography variant="subtitle2" sx={{ mb: 1, color: '#E6EDF3', fontWeight: 500 }}>
              Labels
            </Typography>
            <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mb: 1 }}>
              {labels.map((label) => (
                <Chip
                  key={label}
                  label={label}
                  onDelete={() => handleRemoveLabel(label)}
                  sx={{
                    backgroundColor: '#1F6FEB',
                    color: '#E6EDF3',
                    '& .MuiChip-deleteIcon': {
                      color: '#E6EDF3',
                    },
                  }}
                />
              ))}
            </Box>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <TextField
                fullWidth
                size="small"
                placeholder="Add label..."
                value={labelInput}
                onChange={(e) => setLabelInput(e.target.value)}
                onKeyPress={(e) => {
                  if (e.key === 'Enter') {
                    e.preventDefault();
                    handleAddLabel();
                  }
                }}
                sx={{
                  '& .MuiOutlinedInput-root': {
                    color: '#E6EDF3',
                    '& fieldset': { borderColor: '#30363D' },
                  },
                  '& .MuiInputLabel-root': { color: '#8B949E' },
                }}
              />
              <Button
                onClick={handleAddLabel}
                variant="outlined"
                sx={{
                  borderColor: '#30363D',
                  color: '#E6EDF3',
                  '&:hover': { borderColor: '#FF6B35', backgroundColor: 'rgba(255, 107, 53, 0.1)' },
                }}
              >
                Add
              </Button>
            </Box>
          </Box>

          {/* Parent-Child Relationship */}
          <Box>
            <Typography variant="subtitle2" sx={{ mb: 1, color: '#E6EDF3', fontWeight: 500 }}>
              Schema & Table Relationship
            </Typography>
            <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 2 }}>
              <TextField
                fullWidth
                size="small"
                label="Parent Schema"
                placeholder="e.g., public, analytics"
                value={parentSchema}
                onChange={(e) => setParentSchema(e.target.value)}
                sx={{
                  '& .MuiOutlinedInput-root': {
                    color: '#E6EDF3',
                    '& fieldset': { borderColor: '#30363D' },
                  },
                  '& .MuiInputLabel-root': { color: '#8B949E' },
                }}
              />
              <TextField
                fullWidth
                size="small"
                label="Child Table"
                placeholder="e.g., loan_summary, repayments"
                value={childTable}
                onChange={(e) => setChildTable(e.target.value)}
                sx={{
                  '& .MuiOutlinedInput-root': {
                    color: '#E6EDF3',
                    '& fieldset': { borderColor: '#30363D' },
                  },
                  '& .MuiInputLabel-root': { color: '#8B949E' },
                }}
              />
            </Box>
          </Box>

          {/* Natural Language Business Rules */}
          <Box>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
              <Typography variant="subtitle2" sx={{ color: '#E6EDF3', fontWeight: 500 }}>
                Business Rules
              </Typography>
              <Button
                startIcon={parsing ? <CircularProgress size={16} sx={{ color: '#FF6B35' }} /> : <VisibilityIcon />}
                onClick={handleParseRules}
                disabled={parsing || !description.trim()}
                sx={{
                  color: '#FF6B35',
                  borderColor: '#30363D',
                  '&:hover': { borderColor: '#FF6B35', backgroundColor: 'rgba(255, 107, 53, 0.1)' },
                  '&:disabled': { borderColor: '#30363D', color: '#6E7681' },
                }}
                variant="outlined"
                size="small"
              >
                {parsing ? 'Parsing...' : 'Preview Parsed'}
              </Button>
            </Box>
            <TextField
              fullWidth
              multiline
              rows={10}
              placeholder="Write your business rules in natural language..."
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              sx={{
                fontFamily: 'monospace',
                fontSize: '0.9rem',
                '& .MuiOutlinedInput-root': {
                  color: '#E6EDF3',
                  backgroundColor: '#0D1117',
                  '& fieldset': { borderColor: '#30363D' },
                  '&:hover fieldset': { borderColor: '#FF6B35' },
                },
                '& .MuiInputLabel-root': { color: '#8B949E' },
              }}
            />
          </Box>

          {/* Parsed Preview */}
          {showPreview && parsedPreview && (
            <Box>
              <Typography variant="subtitle2" sx={{ mb: 1, color: '#E6EDF3', fontWeight: 500 }}>
                Parsed Preview
              </Typography>
              <Paper
                sx={{
                  p: 2,
                  backgroundColor: '#0D1117',
                  border: '1px solid #30363D',
                  maxHeight: 200,
                  overflow: 'auto',
                }}
              >
                <Typography
                  component="pre"
                  sx={{
                    color: '#E6EDF3',
                    fontFamily: 'monospace',
                    fontSize: '0.85rem',
                    margin: 0,
                    whiteSpace: 'pre-wrap',
                  }}
                >
                  {JSON.stringify(parsedPreview.filter_conditions, null, 2)}
                </Typography>
              </Paper>
            </Box>
          )}
        </Box>
      </DialogContent>
      <DialogActions sx={{ borderTop: '1px solid #30363D', p: 2 }}>
        <Button onClick={onClose} sx={{ color: '#8B949E' }}>
          Cancel
        </Button>
        <Button
          onClick={handleSave}
          variant="contained"
          disabled={saving}
          sx={{
            backgroundColor: '#FF6B35',
            '&:hover': { backgroundColor: '#E55A2B' },
            '&:disabled': { backgroundColor: '#6E7681' },
          }}
        >
          {saving ? 'Saving...' : 'Save'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};
