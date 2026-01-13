import React, { useEffect, useState } from 'react';
import {
  Box,
  Typography,
  Button,
  IconButton,
  Menu,
  MenuItem,
  Chip,
  Divider,
} from '@mui/material';
import {
  Add as AddIcon,
  MoreVert as MoreVertIcon,
  HelpOutline as HelpIcon,
} from '@mui/icons-material';
import { RuleForm } from './RuleForm';
import { rulesAPI } from '../api/client';

interface Rule {
  id: string;
  description?: string;
  note?: string;
  labels?: string[];
  label?: string;
  parent_schema?: string;
  child_table?: string;
  filter_conditions?: Record<string, string>;
  system?: string;
  metric?: string;
  target_entity?: string;
}

export const RulesView: React.FC = () => {
  const [rules, setRules] = useState<Rule[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [formOpen, setFormOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<Rule | null>(null);
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [selectedRuleId, setSelectedRuleId] = useState<string | null>(null);

  useEffect(() => {
    loadRules();
  }, []);

  const loadRules = async () => {
    try {
      setLoading(true);
      const response = await rulesAPI.list();
      const data = response.data;
      setRules(data.rules || []);
      setError(null);
    } catch (err: any) {
      setError(err.response?.data?.error || err.message || 'Failed to load rules');
    } finally {
      setLoading(false);
    }
  };

  const handleCreateRule = () => {
    setEditingRule(null);
    setFormOpen(true);
  };

  const handleMenuOpen = (event: React.MouseEvent<HTMLElement>, ruleId: string) => {
    setAnchorEl(event.currentTarget);
    setSelectedRuleId(ruleId);
  };

  const handleMenuClose = () => {
    setAnchorEl(null);
    setSelectedRuleId(null);
  };

  const handleEditRule = () => {
    if (selectedRuleId) {
      const rule = rules.find(r => r.id === selectedRuleId);
      if (rule) {
        setEditingRule(rule);
        setFormOpen(true);
      }
    }
    handleMenuClose();
  };

  const handleDeleteRule = async () => {
    if (selectedRuleId && window.confirm('Are you sure you want to delete this rule?')) {
      try {
        await rulesAPI.delete(selectedRuleId);
        loadRules();
      } catch (err: any) {
        alert(err.response?.data?.error || err.message || 'Failed to delete rule');
      }
    }
    handleMenuClose();
  };

  const handleFormClose = () => {
    setFormOpen(false);
    setEditingRule(null);
  };

  const handleFormSave = () => {
    loadRules();
  };

  const getRuleDescription = (rule: Rule): string => {
    return rule.description || rule.note || 'No description';
  };

  const getRuleLabels = (rule: Rule): string[] => {
    return rule.labels || (rule.label ? [rule.label] : []);
  };

  if (loading) {
    return (
      <Box sx={{ p: 3, display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '400px' }}>
        <Typography sx={{ color: '#8B949E' }}>Loading rules...</Typography>
      </Box>
    );
  }

  return (
    <Box sx={{ p: 3, maxWidth: 1200, mx: 'auto' }}>
      {/* Header */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 3 }}>
        <Box>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
            <Typography variant="h4" sx={{ color: '#E6EDF3', fontWeight: 600 }}>
              User Rules
            </Typography>
            <HelpIcon sx={{ fontSize: 20, color: '#8B949E', cursor: 'help' }} />
          </Box>
          <Typography variant="body2" sx={{ color: '#8B949E' }}>
            Manage your custom user rules and preferences.
          </Typography>
        </Box>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={handleCreateRule}
          sx={{
            backgroundColor: '#FF6B35',
            '&:hover': { backgroundColor: '#E55A2B' },
          }}
        >
          Add Rule
        </Button>
      </Box>

      {error && (
        <Box sx={{ mb: 2, p: 2, backgroundColor: '#21262D', borderRadius: 1, border: '1px solid #30363D' }}>
          <Typography sx={{ color: '#FF6B35' }}>{error}</Typography>
        </Box>
      )}

      {/* Rules List */}
      {rules.length === 0 ? (
        <Box
          sx={{
            p: 4,
            textAlign: 'center',
            backgroundColor: '#161B22',
            borderRadius: 1,
            border: '1px solid #30363D',
          }}
        >
          <Typography sx={{ color: '#8B949E', mb: 2 }}>No rules found.</Typography>
          <Button
            variant="outlined"
            startIcon={<AddIcon />}
            onClick={handleCreateRule}
            sx={{
              borderColor: '#30363D',
              color: '#E6EDF3',
              '&:hover': { borderColor: '#FF6B35', backgroundColor: 'rgba(255, 107, 53, 0.1)' },
            }}
          >
            Create your first rule
          </Button>
        </Box>
      ) : (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
          {rules.map((rule, index) => {
            const ruleLabels = getRuleLabels(rule);
            const hasParentChild = rule.parent_schema || rule.child_table;

            return (
              <React.Fragment key={rule.id}>
                <Box
                  sx={{
                    p: 2.5,
                    display: 'flex',
                    alignItems: 'flex-start',
                    justifyContent: 'space-between',
                    backgroundColor: '#161B22',
                    border: '1px solid #30363D',
                    borderRadius: index === 0 ? '8px 8px 0 0' : index === rules.length - 1 ? '0 0 8px 8px' : '0',
                    borderBottom: index < rules.length - 1 ? 'none' : '1px solid #30363D',
                    '&:hover': {
                      backgroundColor: '#1C2128',
                    },
                  }}
                >
                  <Box sx={{ flex: 1, pr: 2 }}>
                    {/* Labels */}
                    {ruleLabels.length > 0 && (
                      <Box sx={{ display: 'flex', gap: 0.5, mb: 1.5, flexWrap: 'wrap' }}>
                        {ruleLabels.map((label) => (
                          <Chip
                            key={label}
                            label={label}
                            size="small"
                            sx={{
                              backgroundColor: '#1F6FEB',
                              color: '#E6EDF3',
                              fontSize: '0.75rem',
                              height: '22px',
                            }}
                          />
                        ))}
                      </Box>
                    )}

                    {/* Parent-Child Relationship */}
                    {hasParentChild && (
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5 }}>
                        {rule.parent_schema && (
                          <>
                            <Chip
                              label={`Schema: ${rule.parent_schema}`}
                              size="small"
                              sx={{
                                backgroundColor: '#21262D',
                                color: '#8B949E',
                                fontSize: '0.75rem',
                                height: '22px',
                              }}
                            />
                            <Typography sx={{ color: '#6E7681', fontSize: '0.875rem' }}>→</Typography>
                          </>
                        )}
                        {rule.child_table && (
                          <Chip
                            label={`Table: ${rule.child_table}`}
                            size="small"
                            sx={{
                              backgroundColor: '#21262D',
                              color: '#8B949E',
                              fontSize: '0.75rem',
                              height: '22px',
                            }}
                          />
                        )}
                      </Box>
                    )}

                    {/* Description */}
                    <Typography
                      sx={{
                        color: '#E6EDF3',
                        fontSize: '0.9375rem',
                        lineHeight: 1.6,
                        whiteSpace: 'pre-wrap',
                      }}
                    >
                      {getRuleDescription(rule)}
                    </Typography>
                  </Box>

                  {/* Actions Menu */}
                  <IconButton
                    size="small"
                    onClick={(e) => handleMenuOpen(e, rule.id)}
                    sx={{
                      color: '#8B949E',
                      '&:hover': {
                        color: '#E6EDF3',
                        backgroundColor: 'rgba(255, 255, 255, 0.05)',
                      },
                    }}
                  >
                    <MoreVertIcon fontSize="small" />
                  </IconButton>
                </Box>
              </React.Fragment>
            );
          })}
        </Box>
      )}

      {/* Context Menu */}
      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleMenuClose}
        PaperProps={{
          sx: {
            backgroundColor: '#161B22',
            border: '1px solid #30363D',
            minWidth: 150,
          },
        }}
      >
        <MenuItem
          onClick={handleEditRule}
          sx={{
            color: '#E6EDF3',
            '&:hover': { backgroundColor: '#1C2128' },
          }}
        >
          Edit
        </MenuItem>
        <MenuItem
          onClick={handleDeleteRule}
          sx={{
            color: '#FF6B35',
            '&:hover': { backgroundColor: 'rgba(255, 107, 53, 0.1)' },
          }}
        >
          Delete
        </MenuItem>
      </Menu>

      {/* Rule Form Dialog */}
      <RuleForm
        open={formOpen}
        onClose={handleFormClose}
        onSave={handleFormSave}
        rule={editingRule ? {
          id: editingRule.id,
          description: getRuleDescription(editingRule),
          note: editingRule.note || editingRule.description,
          labels: getRuleLabels(editingRule),
          filter_conditions: editingRule.filter_conditions || {},
          parent_schema: editingRule.parent_schema,
          child_table: editingRule.child_table,
        } : null}
      />
    </Box>
  );
};
