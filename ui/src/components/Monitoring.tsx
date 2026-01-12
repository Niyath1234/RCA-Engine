import React from 'react';
import {
  Box,
  Typography,
  Card,
  CardContent,
} from '@mui/material';
import {
  Monitor as MonitorIcon,
  CheckCircle as CheckIcon,
  Error as ErrorIcon,
  Schedule as ScheduleIcon,
} from '@mui/icons-material';
import { useStore } from '../store/useStore';

export const Monitoring: React.FC = () => {
  const { pipelines } = useStore();

  const stats = {
    total: pipelines.length,
    active: pipelines.filter((p) => p.status === 'active').length,
    inactive: pipelines.filter((p) => p.status === 'inactive').length,
    error: pipelines.filter((p) => p.status === 'error').length,
  };

  return (
    <Box sx={{ p: 3, height: '100%', overflow: 'auto' }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 3 }}>
        <MonitorIcon sx={{ color: '#FF6B35' }} />
        <Typography variant="h4" sx={{ color: '#E6EDF3', fontWeight: 600 }}>
          Pipeline Monitoring
        </Typography>
      </Box>

      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(4, 1fr)' }, gap: 3 }}>
        <Card sx={{ backgroundColor: '#21262D' }}>
          <CardContent>
            <Typography variant="body2" sx={{ color: '#8B949E', mb: 1 }}>
              Total Pipelines
            </Typography>
            <Typography variant="h4" sx={{ color: '#E6EDF3', fontWeight: 600 }}>
              {stats.total}
            </Typography>
          </CardContent>
        </Card>
        <Card sx={{ backgroundColor: '#21262D' }}>
          <CardContent>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
              <CheckIcon sx={{ color: '#4ECDC4' }} />
              <Typography variant="body2" sx={{ color: '#8B949E' }}>
                Active
              </Typography>
            </Box>
            <Typography variant="h4" sx={{ color: '#4ECDC4', fontWeight: 600 }}>
              {stats.active}
            </Typography>
          </CardContent>
        </Card>
        <Card sx={{ backgroundColor: '#21262D' }}>
          <CardContent>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
              <ScheduleIcon sx={{ color: '#8B949E' }} />
              <Typography variant="body2" sx={{ color: '#8B949E' }}>
                Inactive
              </Typography>
            </Box>
            <Typography variant="h4" sx={{ color: '#8B949E', fontWeight: 600 }}>
              {stats.inactive}
            </Typography>
          </CardContent>
        </Card>
        <Card sx={{ backgroundColor: '#21262D' }}>
          <CardContent>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
              <ErrorIcon sx={{ color: '#FF6B35' }} />
              <Typography variant="body2" sx={{ color: '#8B949E' }}>
                Errors
              </Typography>
            </Box>
            <Typography variant="h4" sx={{ color: '#FF6B35', fontWeight: 600 }}>
              {stats.error}
            </Typography>
          </CardContent>
        </Card>
      </Box>

      {stats.total === 0 && (
        <Card sx={{ backgroundColor: '#21262D', p: 4, textAlign: 'center', mt: 3 }}>
          <MonitorIcon sx={{ fontSize: 64, color: '#8B949E', mb: 2, opacity: 0.5 }} />
          <Typography variant="h6" sx={{ color: '#8B949E', mb: 1 }}>
            No pipelines to monitor
          </Typography>
          <Typography variant="body2" sx={{ color: '#6E7681' }}>
            Create pipelines to see monitoring data here
          </Typography>
        </Card>
      )}
    </Box>
  );
};

