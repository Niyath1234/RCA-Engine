import React from 'react';
import { Box, Tabs, Tab, IconButton } from '@mui/material';
import { Settings as SettingsIcon } from '@mui/icons-material';

interface TopBarProps {
  activeView: 'query' | 'chat' | 'graph';
  onViewChange: (view: 'query' | 'chat' | 'graph') => void;
}

export const TopBar: React.FC<TopBarProps> = ({ activeView, onViewChange }) => {
  return (
    <Box
      sx={{
        height: 40,
        backgroundColor: '#252526',
        borderBottom: '1px solid #3E3E42',
        display: 'flex',
        alignItems: 'center',
        px: 2,
        gap: 2,
      }}
    >
      <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', gap: 1 }}>
        <Tabs
          value={activeView}
          onChange={(_, value) => onViewChange(value)}
          sx={{
            minHeight: 40,
            '& .MuiTab-root': {
              minHeight: 40,
              padding: '0 16px',
              textTransform: 'none',
              color: '#CCCCCC',
              fontSize: '0.875rem',
              '&.Mui-selected': {
                color: '#FFFFFF',
                backgroundColor: '#1E1E1E',
              },
            },
            '& .MuiTabs-indicator': {
              display: 'none',
            },
          }}
        >
          <Tab label="Query" value="query" />
          <Tab label="Chat" value="chat" />
          <Tab label="Graph" value="graph" />
        </Tabs>
      </Box>
      <IconButton
        size="small"
        sx={{
          color: '#CCCCCC',
          '&:hover': { backgroundColor: '#3E3E42' },
        }}
      >
        <SettingsIcon fontSize="small" />
      </IconButton>
    </Box>
  );
};
