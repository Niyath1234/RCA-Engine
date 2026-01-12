import React from 'react';
import {
  AppBar,
  Toolbar,
  IconButton,
  Typography,
  Box,
  Chip,
} from '@mui/material';
import {
  Menu as MenuIcon,
  Settings as SettingsIcon,
} from '@mui/icons-material';
import { useStore } from '../store/useStore';

export const TopBar: React.FC = () => {
  const { sidebarOpen, setSidebarOpen, viewMode } = useStore();

  return (
    <AppBar
      position="static"
      sx={{
        backgroundColor: '#161B22',
        borderBottom: '1px solid #30363D',
        boxShadow: 'none',
        height: 48,
      }}
    >
      <Toolbar sx={{ minHeight: '48px !important', px: 2 }}>
        {!sidebarOpen && (
          <IconButton
            edge="start"
            onClick={() => setSidebarOpen(true)}
            sx={{ color: '#E6EDF3', mr: 2 }}
          >
            <MenuIcon />
          </IconButton>
        )}
        <Typography
          variant="h6"
          sx={{
            flexGrow: 1,
            color: '#FF6B35',
            fontWeight: 700,
            fontSize: '1.1rem',
          }}
        >
          RCA ENGINE
        </Typography>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Chip
            label={viewMode.toUpperCase()}
            size="small"
            sx={{
              backgroundColor: 'rgba(255, 107, 53, 0.1)',
              color: '#FF6B35',
            }}
          />
          <IconButton sx={{ color: '#8B949E' }}>
            <SettingsIcon />
          </IconButton>
        </Box>
      </Toolbar>
    </AppBar>
  );
};

