import { useState } from 'react';
import { Box, CssBaseline, ThemeProvider, createTheme } from '@mui/material';
import { TopBar } from './components/TopBar';
import { ObjectExplorer } from './components/ObjectExplorer';
import { QueryEditor } from './components/QueryEditor';
import { ResultsPanel } from './components/ResultsPanel';
import { ChatPanel } from './components/ChatPanel';
import { GraphVisualizer } from './components/GraphVisualizer';

const darkTheme = createTheme({
  palette: {
    mode: 'dark',
    background: {
      default: '#1E1E1E',
      paper: '#252526',
    },
    text: {
      primary: '#CCCCCC',
      secondary: '#858585',
    },
  },
});

function App() {
  const [activeView, setActiveView] = useState<'query' | 'chat' | 'graph'>('query');
  const [queryResult, setQueryResult] = useState<any>(null);
  const [isExecuting, setIsExecuting] = useState(false);

  const handleExecuteQuery = async (query: string, mode: string = 'sql') => {
    console.log('Executing query:', query, 'mode:', mode);
    setIsExecuting(true);
    try {
      const response = await fetch('http://localhost:8080/api/query/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, mode }),
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        console.error('Query execution failed:', response.status, errorText);
        setQueryResult({ error: `Query failed: ${response.status} ${errorText}` });
        return;
      }
      
      const data = await response.json();
      console.log('Query result:', data);
      setQueryResult(data);
    } catch (error) {
      console.error('Query execution error:', error);
      setQueryResult({ error: `Failed to execute query: ${error instanceof Error ? error.message : String(error)}` });
    } finally {
      setIsExecuting(false);
    }
  };

  return (
    <ThemeProvider theme={darkTheme}>
      <Box sx={{ display: 'flex', flexDirection: 'column', height: '100vh', overflow: 'hidden', backgroundColor: '#1E1E1E' }}>
        <CssBaseline />
        <TopBar activeView={activeView} onViewChange={setActiveView} />
        <Box sx={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
          <ObjectExplorer />
          <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
            {activeView === 'query' ? (
              <>
                <QueryEditor onExecute={handleExecuteQuery} isExecuting={isExecuting} />
                <ResultsPanel result={queryResult} />
              </>
            ) : activeView === 'chat' ? (
              <ChatPanel />
            ) : (
              <GraphVisualizer />
            )}
          </Box>
        </Box>
      </Box>
    </ThemeProvider>
  );
}

export default App;
