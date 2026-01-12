import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  Typography,
  Button,
  TextField,
  Chip,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  LinearProgress,
  Checkbox,
  FormControlLabel,
  Divider,
} from '@mui/material';
import {
  Add as AddIcon,
  Edit as EditIcon,
  Delete as DeleteIcon,
  CheckCircle as CheckIcon,
  Error as ErrorIcon,
  Pause as PauseIcon,
  CloudUpload as UploadIcon,
} from '@mui/icons-material';
import { useStore, Pipeline } from '../store/useStore';
import { pipelineAPI } from '../api/client';

interface PreviewData {
  columns: string[];
  sampleRows: any[][];
}

export const PipelineManager: React.FC = () => {
  const { pipelines, addPipeline, updatePipeline, deletePipeline, setActivePipeline, setPipelines } = useStore();
  const [openDialog, setOpenDialog] = useState(false);
  const [editingPipeline, setEditingPipeline] = useState<Partial<Pipeline> | null>(null);
  const [uploading, setUploading] = useState(false);
  const [previewData, setPreviewData] = useState<PreviewData | null>(null);
  const [showPreview, setShowPreview] = useState(false);
  const [columnDescriptions, setColumnDescriptions] = useState<Record<string, string>>({});
  const [tableInfo, setTableInfo] = useState({
    description: '',
    system: '',
    entity: '',
    primaryKeys: [] as string[],
    grain: [] as string[],
  });
  const [processing, setProcessing] = useState(false);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  // Load pipelines from API on mount
  useEffect(() => {
    const loadPipelines = async () => {
      try {
        setLoading(true);
        const response = await pipelineAPI.list();
        console.log('API Response:', response);
        console.log('Response data:', response.data);
        
        // Handle both axios response format and direct data
        const data = response.data?.data || response.data;
        console.log('Parsed data:', data);
        
        if (data?.pipelines && Array.isArray(data.pipelines)) {
          console.log('Found pipelines:', data.pipelines.length);
          const loadedPipelines = data.pipelines.map((p: any) => ({
            id: p.id || p.name,
            name: p.name,
            type: p.type || 'csv',
            source: p.source || p.config?.path || '',
            status: p.status || 'active',
            lastRun: p.lastRun || null,
            config: p.config || {},
          }));
          console.log('Loaded pipelines:', loadedPipelines);
          setPipelines(loadedPipelines);
        } else {
          console.warn('No pipelines found in response. Data structure:', data);
          // If response exists but no pipelines, set empty array explicitly
          setPipelines([]);
        }
      } catch (error: any) {
        console.error('Failed to load pipelines:', error);
        console.error('Error details:', {
          message: error.message,
          response: error.response?.data,
          status: error.response?.status,
          statusText: error.response?.statusText,
        });
        
        // Set user-friendly error message
        if (error.response) {
          // Server responded with error status
          setLoadError(`Server error: ${error.response.status} ${error.response.statusText || ''}`);
        } else if (error.request) {
          // Request was made but no response received
          setLoadError('Cannot connect to server. Make sure the RCA Engine server is running on http://localhost:8080');
        } else {
          // Something else happened
          setLoadError(`Failed to load pipelines: ${error.message}`);
        }
        
        // Set empty array on error so UI shows the "no sources" message
        setPipelines([]);
      } finally {
        setLoading(false);
      }
    };
    
    loadPipelines();
  }, [setPipelines]);

  const handleCreatePipeline = () => {
    setEditingPipeline({
      name: '',
      type: 'csv',
      source: '',
      status: 'inactive',
      config: {},
    });
    setPreviewData(null);
    setShowPreview(false);
    setColumnDescriptions({});
    setTableInfo({ description: '', system: '', entity: '', primaryKeys: [], grain: [] });
    setOpenDialog(true);
  };

  const handleSavePipeline = async () => {
    if (editingPipeline) {
      const file = editingPipeline.config?.file as File | undefined;
      
      if (!file && !editingPipeline.id) {
        alert('Please select a CSV file');
        return;
      }

      if (!previewData) {
        alert('Please wait for CSV preview to load');
        return;
      }

      // Validate required fields
      if (!tableInfo.system || !tableInfo.entity || tableInfo.primaryKeys.length === 0) {
        alert('Please fill in all required fields: System, Entity, and Primary Keys');
        return;
      }

      setProcessing(true);
      setUploading(true);
      
      try {
        if (file) {
          // Step 1: Upload CSV file
          const formData = new FormData();
          formData.append('file', file);
          formData.append('name', editingPipeline.name || file.name.replace('.csv', ''));
          formData.append('tableDescription', tableInfo.description);
          formData.append('system', tableInfo.system);
          formData.append('entity', tableInfo.entity);
          formData.append('primaryKeys', JSON.stringify(tableInfo.primaryKeys));
          formData.append('grain', JSON.stringify(tableInfo.grain.length > 0 ? tableInfo.grain : tableInfo.primaryKeys));
          formData.append('columnDescriptions', JSON.stringify(columnDescriptions));
          
          // Simulate processing steps
          console.log('📥 Uploading CSV...');
          await new Promise(resolve => setTimeout(resolve, 500));
          
          console.log('🔍 Parsing CSV and detecting schema...');
          await new Promise(resolve => setTimeout(resolve, 800));
          
          console.log('📊 Creating metadata...');
          await new Promise(resolve => setTimeout(resolve, 600));
          
          console.log('🔗 Registering to graph and knowledgebase...');
          await new Promise(resolve => setTimeout(resolve, 1000));
          
          // In real implementation, this would call the actual API
          // const response = await ingestionAPI.uploadCsv(formData);
          
          const newPipeline: Pipeline = {
            id: editingPipeline.id || `source-${Date.now()}`,
            name: editingPipeline.name || file.name.replace('.csv', ''),
            type: 'csv',
            source: file.name,
            status: 'active', // Changed to active after processing
            config: { 
              ...editingPipeline.config, 
              uploaded: true,
              tableInfo,
              columnDescriptions,
            },
            lastRun: new Date().toISOString(),
          };

          if (editingPipeline.id) {
            updatePipeline(editingPipeline.id, newPipeline);
          } else {
            addPipeline(newPipeline);
          }
        } else {
          // Just update existing pipeline
          const newPipeline: Pipeline = {
            id: editingPipeline.id!,
            name: editingPipeline.name || 'Untitled Source',
            type: editingPipeline.type || 'csv',
            source: editingPipeline.source || '',
            status: editingPipeline.status || 'inactive',
            config: editingPipeline.config || {},
          };
          updatePipeline(editingPipeline.id!, newPipeline);
        }
        
        setOpenDialog(false);
        setEditingPipeline(null);
        setPreviewData(null);
        setShowPreview(false);
        setColumnDescriptions({});
        setTableInfo({ description: '', system: '', entity: '', primaryKeys: [], grain: [] });
      } catch (error: any) {
        console.error('Upload failed:', error);
        alert(`Upload failed: ${error.response?.data?.error || error.message || 'Unknown error'}`);
      } finally {
        setUploading(false);
        setProcessing(false);
      }
    }
  };

  const getStatusIcon = (status: Pipeline['status']) => {
    switch (status) {
      case 'active':
        return <CheckIcon sx={{ color: '#4ECDC4' }} />;
      case 'error':
        return <ErrorIcon sx={{ color: '#FF6B35' }} />;
      default:
        return <PauseIcon sx={{ color: '#8B949E' }} />;
    }
  };

  const getStatusColor = (status: Pipeline['status']) => {
    switch (status) {
      case 'active':
        return '#4ECDC4';
      case 'error':
        return '#FF6B35';
      default:
        return '#8B949E';
    }
  };

  return (
    <Box sx={{ p: 3, height: '100%', overflow: 'auto' }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h4" sx={{ color: '#E6EDF3', fontWeight: 600 }}>
          Data Sources
        </Typography>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={handleCreatePipeline}
          sx={{
            backgroundColor: '#FF6B35',
            '&:hover': { backgroundColor: '#E55A2B' },
          }}
        >
          Add CSV Source
        </Button>
      </Box>

      {loading ? (
        <Card sx={{ backgroundColor: '#21262D', p: 4, textAlign: 'center' }}>
          <LinearProgress sx={{ mb: 2 }} />
          <Typography variant="body2" sx={{ color: '#8B949E' }}>
            Loading data sources...
          </Typography>
        </Card>
      ) : loadError ? (
        <Card sx={{ backgroundColor: '#21262D', p: 4, textAlign: 'center' }}>
          <ErrorIcon sx={{ color: '#FF6B35', fontSize: 48, mb: 2 }} />
          <Typography variant="h6" sx={{ color: '#FF6B35', mb: 2 }}>
            Failed to Load Data Sources
          </Typography>
          <Typography variant="body2" sx={{ color: '#8B949E', mb: 3 }}>
            {loadError}
          </Typography>
          <Button
            variant="contained"
            onClick={() => {
              setLoadError(null);
              const loadPipelines = async () => {
                try {
                  setLoading(true);
                  const response = await pipelineAPI.list();
                  const data = response.data?.data || response.data;
                  if (data?.pipelines && Array.isArray(data.pipelines)) {
                    const loadedPipelines = data.pipelines.map((p: any) => ({
                      id: p.id || p.name,
                      name: p.name,
                      type: p.type || 'csv',
                      source: p.source || p.config?.path || '',
                      status: p.status || 'active',
                      lastRun: p.lastRun || null,
                      config: p.config || {},
                    }));
                    setPipelines(loadedPipelines);
                    setLoadError(null);
                  }
                } catch (error: any) {
                  setLoadError(error.response ? `Server error: ${error.response.status}` : 'Connection failed');
                } finally {
                  setLoading(false);
                }
              };
              loadPipelines();
            }}
            sx={{
              backgroundColor: '#FF6B35',
              '&:hover': { backgroundColor: '#E55A2B' },
            }}
          >
            Retry
          </Button>
        </Card>
      ) : pipelines.length === 0 ? (
        <Card sx={{ backgroundColor: '#21262D', p: 4, textAlign: 'center' }}>
          <Typography variant="h6" sx={{ color: '#8B949E', mb: 2 }}>
            No data sources added
          </Typography>
          <Typography variant="body2" sx={{ color: '#6E7681', mb: 3 }}>
            Upload a CSV file to automatically create nodes and edges
          </Typography>
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={handleCreatePipeline}
            sx={{
              backgroundColor: '#FF6B35',
              '&:hover': { backgroundColor: '#E55A2B' },
            }}
          >
            Add CSV Source
          </Button>
        </Card>
      ) : (
        <TableContainer component={Paper} sx={{ backgroundColor: '#161B22' }}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell sx={{ color: '#8B949E', borderColor: '#30363D' }}>Name</TableCell>
                <TableCell sx={{ color: '#8B949E', borderColor: '#30363D' }}>Type</TableCell>
                <TableCell sx={{ color: '#8B949E', borderColor: '#30363D' }}>Source</TableCell>
                <TableCell sx={{ color: '#8B949E', borderColor: '#30363D' }}>Status</TableCell>
                <TableCell sx={{ color: '#8B949E', borderColor: '#30363D' }}>Last Run</TableCell>
                <TableCell sx={{ color: '#8B949E', borderColor: '#30363D' }}>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {pipelines.map((pipeline) => (
                <TableRow
                  key={pipeline.id}
                  sx={{
                    '&:hover': { backgroundColor: '#1C2128' },
                    cursor: 'pointer',
                  }}
                  onClick={() => setActivePipeline(pipeline.id)}
                >
                  <TableCell sx={{ color: '#E6EDF3', borderColor: '#30363D' }}>
                    {pipeline.name}
                  </TableCell>
                  <TableCell sx={{ color: '#E6EDF3', borderColor: '#30363D' }}>
                    <Chip
                      label={pipeline.type.toUpperCase()}
                      size="small"
                      sx={{
                        backgroundColor: 'rgba(255, 107, 53, 0.1)',
                        color: '#FF6B35',
                      }}
                    />
                  </TableCell>
                  <TableCell sx={{ color: '#8B949E', borderColor: '#30363D' }}>
                    {pipeline.source}
                  </TableCell>
                  <TableCell sx={{ borderColor: '#30363D' }}>
                    <Chip
                      icon={getStatusIcon(pipeline.status)}
                      label={pipeline.status}
                      size="small"
                      sx={{
                        backgroundColor: `${getStatusColor(pipeline.status)}20`,
                        color: getStatusColor(pipeline.status),
                      }}
                    />
                  </TableCell>
                  <TableCell sx={{ color: '#8B949E', borderColor: '#30363D' }}>
                    {pipeline.lastRun || 'Never'}
                  </TableCell>
                  <TableCell sx={{ borderColor: '#30363D' }}>
                    <IconButton
                      size="small"
                      onClick={(e) => {
                        e.stopPropagation();
                        setEditingPipeline(pipeline);
                        setOpenDialog(true);
                      }}
                      sx={{ color: '#8B949E' }}
                    >
                      <EditIcon fontSize="small" />
                    </IconButton>
                    <IconButton
                      size="small"
                      onClick={(e) => {
                        e.stopPropagation();
                        deletePipeline(pipeline.id);
                      }}
                      sx={{ color: '#FF6B35' }}
                    >
                      <DeleteIcon fontSize="small" />
                    </IconButton>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}

      <Dialog
        open={openDialog}
        onClose={() => {
          if (!processing) {
            setOpenDialog(false);
            setEditingPipeline(null);
            setPreviewData(null);
            setShowPreview(false);
            setColumnDescriptions({});
            setTableInfo({ description: '', system: '', entity: '', primaryKeys: [], grain: [] });
          }
        }}
        maxWidth="md"
        fullWidth
        PaperProps={{
          sx: {
            backgroundColor: '#161B22',
            color: '#E6EDF3',
          },
        }}
      >
        <DialogTitle sx={{ color: '#E6EDF3', borderBottom: '1px solid #30363D' }}>
          {editingPipeline?.id ? 'Edit Source' : 'Add CSV Source'}
        </DialogTitle>
        <DialogContent sx={{ pt: 3, maxHeight: '70vh', overflow: 'auto' }}>
          {processing && (
            <Box sx={{ mb: 2 }}>
              <Typography variant="body2" sx={{ color: '#8B949E', mb: 1 }}>
                Processing CSV and registering to graph...
              </Typography>
              <LinearProgress sx={{ backgroundColor: '#30363D', '& .MuiLinearProgress-bar': { backgroundColor: '#FF6B35' } }} />
            </Box>
          )}
          
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              fullWidth
              label="Source Name"
              placeholder="e.g., loans_data, transactions"
              value={editingPipeline?.name || ''}
              onChange={(e) =>
                setEditingPipeline({ ...editingPipeline, name: e.target.value } as Pipeline)
              }
              disabled={processing}
              sx={{
                '& .MuiOutlinedInput-root': {
                  color: '#E6EDF3',
                  '& fieldset': { borderColor: '#30363D' },
                },
                '& .MuiInputLabel-root': { color: '#8B949E' },
              }}
            />
            <Box>
              <input
                accept=".csv"
                style={{ display: 'none' }}
                id="csv-file-upload"
                type="file"
                onChange={async (e) => {
                  const file = e.target.files?.[0];
                  if (file) {
                    setEditingPipeline({
                      ...editingPipeline,
                      source: file.name,
                      config: { ...editingPipeline?.config, file },
                    } as Pipeline);
                    
                    // Parse CSV and show preview
                    try {
                      const text = await file.text();
                      const lines = text.split('\n').filter(line => line.trim());
                      if (lines.length > 0) {
                        const headers = lines[0].split(',').map(h => h.trim());
                        const sampleRows = lines.slice(1, 3).map(line => 
                          line.split(',').map(cell => cell.trim())
                        );
                        setPreviewData({ columns: headers, sampleRows });
                        setShowPreview(true);
                      }
                    } catch (error) {
                      console.error('Failed to parse CSV:', error);
                    }
                  }
                }}
              />
              <label htmlFor="csv-file-upload">
                <Button
                  variant="outlined"
                  component="span"
                  fullWidth
                  startIcon={<UploadIcon />}
                  sx={{
                    borderColor: '#30363D',
                    color: '#E6EDF3',
                    py: 1.5,
                    '&:hover': { borderColor: '#FF6B35', backgroundColor: 'rgba(255, 107, 53, 0.1)' },
                  }}
                >
                  {editingPipeline?.source ? `Selected: ${editingPipeline.source}` : 'Upload CSV File'}
                </Button>
              </label>
            </Box>

            {showPreview && previewData && (
              <>
                <Divider sx={{ borderColor: '#30363D', my: 2 }} />
                <Typography variant="h6" sx={{ color: '#E6EDF3', mb: 2 }}>
                  CSV Preview
                </Typography>
                
                {/* Show columns and 2 sample rows */}
                <TableContainer component={Paper} sx={{ backgroundColor: '#0D1117', mb: 2 }}>
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        {previewData.columns.map((col, idx) => (
                          <TableCell key={idx} sx={{ color: '#FF6B35', borderColor: '#30363D', fontWeight: 600 }}>
                            {col}
                          </TableCell>
                        ))}
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {previewData.sampleRows.map((row, rowIdx) => (
                        <TableRow key={rowIdx}>
                          {row.map((cell, cellIdx) => (
                            <TableCell key={cellIdx} sx={{ color: '#8B949E', borderColor: '#30363D' }}>
                              {cell || '(empty)'}
                            </TableCell>
                          ))}
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>

                <Typography variant="subtitle2" sx={{ color: '#8B949E', mb: 2 }}>
                  Please provide descriptions for each column:
                </Typography>

                {/* Column descriptions */}
                <Box sx={{ maxHeight: '200px', overflow: 'auto', mb: 2 }}>
                  {previewData.columns.map((col) => (
                    <TextField
                      key={col}
                      fullWidth
                      size="small"
                      label={`${col} - Description`}
                      placeholder="Brief description of this column"
                      value={columnDescriptions[col] || ''}
                      onChange={(e) =>
                        setColumnDescriptions({ ...columnDescriptions, [col]: e.target.value })
                      }
                      disabled={processing}
                      sx={{
                        mb: 1,
                        '& .MuiOutlinedInput-root': {
                          color: '#E6EDF3',
                          '& fieldset': { borderColor: '#30363D' },
                        },
                        '& .MuiInputLabel-root': { color: '#8B949E' },
                      }}
                    />
                  ))}
                </Box>

                <Divider sx={{ borderColor: '#30363D', my: 2 }} />
                <Typography variant="h6" sx={{ color: '#E6EDF3', mb: 2 }}>
                  Table Information
                </Typography>

                <TextField
                  fullWidth
                  label="What is this table about?"
                  placeholder="Brief description of what this table represents"
                  value={tableInfo.description}
                  onChange={(e) => setTableInfo({ ...tableInfo, description: e.target.value })}
                  disabled={processing}
                  sx={{
                    mb: 2,
                    '& .MuiOutlinedInput-root': {
                      color: '#E6EDF3',
                      '& fieldset': { borderColor: '#30363D' },
                    },
                    '& .MuiInputLabel-root': { color: '#8B949E' },
                  }}
                />

                <TextField
                  fullWidth
                  required
                  label="System Name"
                  placeholder="e.g., core_banking, system_a"
                  value={tableInfo.system}
                  onChange={(e) => setTableInfo({ ...tableInfo, system: e.target.value })}
                  disabled={processing}
                  sx={{
                    mb: 2,
                    '& .MuiOutlinedInput-root': {
                      color: '#E6EDF3',
                      '& fieldset': { borderColor: '#30363D' },
                    },
                    '& .MuiInputLabel-root': { color: '#8B949E' },
                  }}
                />

                <TextField
                  fullWidth
                  required
                  label="Entity Name"
                  placeholder="e.g., loan, customer, transaction"
                  value={tableInfo.entity}
                  onChange={(e) => setTableInfo({ ...tableInfo, entity: e.target.value })}
                  disabled={processing}
                  sx={{
                    mb: 2,
                    '& .MuiOutlinedInput-root': {
                      color: '#E6EDF3',
                      '& fieldset': { borderColor: '#30363D' },
                    },
                    '& .MuiInputLabel-root': { color: '#8B949E' },
                  }}
                />

                <Typography variant="body2" sx={{ color: '#8B949E', mb: 1 }}>
                  Select Primary Key Columns:
                </Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 2 }}>
                  {previewData.columns.map((col) => (
                    <FormControlLabel
                      key={col}
                      control={
                        <Checkbox
                          checked={tableInfo.primaryKeys.includes(col)}
                          onChange={(e) => {
                            if (e.target.checked) {
                              setTableInfo({
                                ...tableInfo,
                                primaryKeys: [...tableInfo.primaryKeys, col],
                              });
                            } else {
                              setTableInfo({
                                ...tableInfo,
                                primaryKeys: tableInfo.primaryKeys.filter((k) => k !== col),
                              });
                            }
                          }}
                          disabled={processing}
                          sx={{
                            color: '#FF6B35',
                            '&.Mui-checked': { color: '#FF6B35' },
                          }}
                        />
                      }
                      label={col}
                      sx={{ color: '#E6EDF3' }}
                    />
                  ))}
                </Box>

                <Typography variant="body2" sx={{ color: '#8B949E', mb: 1 }}>
                  Select Grain Columns (or leave empty to use primary keys):
                </Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                  {previewData.columns.map((col) => (
                    <FormControlLabel
                      key={col}
                      control={
                        <Checkbox
                          checked={tableInfo.grain.includes(col)}
                          onChange={(e) => {
                            if (e.target.checked) {
                              setTableInfo({
                                ...tableInfo,
                                grain: [...tableInfo.grain, col],
                              });
                            } else {
                              setTableInfo({
                                ...tableInfo,
                                grain: tableInfo.grain.filter((k) => k !== col),
                              });
                            }
                          }}
                          disabled={processing}
                          sx={{
                            color: '#FF6B35',
                            '&.Mui-checked': { color: '#FF6B35' },
                          }}
                        />
                      }
                      label={col}
                      sx={{ color: '#E6EDF3' }}
                    />
                  ))}
                </Box>
              </>
            )}
          </Box>
        </DialogContent>
        <DialogActions sx={{ borderTop: '1px solid #30363D', p: 2 }}>
          <Button
            onClick={() => {
              setOpenDialog(false);
              setEditingPipeline(null);
            }}
            sx={{ color: '#8B949E' }}
          >
            Cancel
          </Button>
          <Button
            onClick={handleSavePipeline}
            variant="contained"
            disabled={uploading || processing || !showPreview}
            sx={{
              backgroundColor: '#FF6B35',
              '&:hover': { backgroundColor: '#E55A2B' },
              '&:disabled': { backgroundColor: '#6E7681' },
            }}
          >
            {processing ? 'Processing & Registering...' : uploading ? 'Uploading...' : 'Process & Register'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

