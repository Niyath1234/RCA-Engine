const express = require('express');
const cors = require('cors');
const { exec } = require('child_process');
const { promisify } = require('util');
const fs = require('fs');
const fsPromises = require('fs').promises;
const path = require('path');
const csv = require('csv-parser');

const execAsync = promisify(exec);
const app = express();
const PORT = 8080;

app.use(cors());
app.use(express.json());

// Store pipelines in memory (in production, use a database)
let pipelines = [];
let reasoningHistory = [];
let rules = [];

// Helper to run Rust CLI commands
async function runRustCommand(command, args = []) {
  const projectRoot = path.join(__dirname, '..');
  const fullCommand = `cd ${projectRoot} && cargo run -- ${command} ${args.join(' ')}`;
  
  try {
    const { stdout, stderr } = await execAsync(fullCommand, { 
      maxBuffer: 10 * 1024 * 1024 // 10MB buffer
    });
    return { success: true, output: stdout, error: stderr };
  } catch (error) {
    return { success: false, output: error.stdout, error: error.stderr || error.message };
  }
}

// Helper to read CSV and get summary
async function readCSVSummary(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', () => {
        resolve({
          rowCount: results.length,
          columns: Object.keys(results[0] || {}),
          sample: results.slice(0, 5),
        });
      })
      .on('error', reject);
  });
}

// API Routes

// Root route
app.get('/', (req, res) => {
  res.json({
    name: 'RCA Engine API',
    version: '1.0.0',
    status: 'running',
    endpoints: {
      pipelines: '/api/pipelines',
      reasoning: '/api/reasoning/query',
      rules: '/api/rules',
      ingestion: '/api/ingestion',
    },
  });
});

// Rules API
app.get('/api/rules', (req, res) => {
  res.json({ rules });
});

app.get('/api/rules/:id', (req, res) => {
  const rule = rules.find(r => r.id === req.params.id);
  if (!rule) {
    return res.status(404).json({ error: 'Rule not found' });
  }
  res.json(rule);
});

app.post('/api/rules', (req, res) => {
  const rule = {
    id: `rule-${Date.now()}`,
    ...req.body,
    createdAt: new Date().toISOString(),
  };
  rules.push(rule);
  res.json(rule);
});

app.put('/api/rules/:id', (req, res) => {
  const index = rules.findIndex(r => r.id === req.params.id);
  if (index === -1) {
    return res.status(404).json({ error: 'Rule not found' });
  }
  rules[index] = { ...rules[index], ...req.body, updatedAt: new Date().toISOString() };
  res.json(rules[index]);
});

app.delete('/api/rules/:id', (req, res) => {
  const index = rules.findIndex(r => r.id === req.params.id);
  if (index === -1) {
    return res.status(404).json({ error: 'Rule not found' });
  }
  rules.splice(index, 1);
  res.json({ success: true });
});

// Pipelines
app.get('/api/pipelines', (req, res) => {
  res.json(pipelines);
});

app.post('/api/pipelines', async (req, res) => {
  const pipeline = {
    id: `pipeline-${Date.now()}`,
    ...req.body,
    status: 'inactive',
    createdAt: new Date().toISOString(),
  };
  pipelines.push(pipeline);
  res.json(pipeline);
});

app.put('/api/pipelines/:id', (req, res) => {
  const index = pipelines.findIndex(p => p.id === req.params.id);
  if (index === -1) {
    return res.status(404).json({ error: 'Pipeline not found' });
  }
  pipelines[index] = { ...pipelines[index], ...req.body };
  res.json(pipelines[index]);
});

app.delete('/api/pipelines/:id', (req, res) => {
  const index = pipelines.findIndex(p => p.id === req.params.id);
  if (index === -1) {
    return res.status(404).json({ error: 'Pipeline not found' });
  }
  pipelines.splice(index, 1);
  res.json({ success: true });
});

app.post('/api/pipelines/:id/run', async (req, res) => {
  const pipeline = pipelines.find(p => p.id === req.params.id);
  if (!pipeline) {
    return res.status(404).json({ error: 'Pipeline not found' });
  }

  try {
    // Update status
    pipeline.status = 'active';
    pipeline.lastRun = new Date().toISOString();

    // For CSV files, we can use the Rust CLI
    if (pipeline.type === 'csv' && pipeline.source) {
      // Copy CSV to data directory if needed
      const dataDir = path.join(__dirname, '..', 'data');
      const fileName = path.basename(pipeline.source);
      const destPath = path.join(dataDir, fileName);
      
      await fsPromises.mkdir(dataDir, { recursive: true });
      await fsPromises.copyFile(pipeline.source, destPath);

      // Get CSV summary
      const summary = await readCSVSummary(pipeline.source);
      
      res.json({
        success: true,
        message: `Ingested ${summary.rowCount} rows from ${fileName}`,
        summary,
        pipeline,
      });
    } else {
      res.json({
        success: true,
        message: 'Pipeline executed',
        pipeline,
      });
    }
  } catch (error) {
    pipeline.status = 'error';
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/pipelines/:id/status', (req, res) => {
  const pipeline = pipelines.find(p => p.id === req.params.id);
  if (!pipeline) {
    return res.status(404).json({ error: 'Pipeline not found' });
  }
  res.json({ status: pipeline.status, lastRun: pipeline.lastRun });
});

// Reasoning API
app.post('/api/reasoning/query', async (req, res) => {
  const { query, context } = req.body;
  
  // Add reasoning steps
  const steps = [
    {
      type: 'thought',
      content: `Analyzing query: "${query}"`,
      timestamp: new Date().toISOString(),
    },
    {
      type: 'thought',
      content: 'Checking available pipelines and data sources...',
      timestamp: new Date().toISOString(),
    },
  ];

  // If query mentions reconciliation or ledger balance
  if (query.toLowerCase().includes('recon') || query.toLowerCase().includes('ledger')) {
    // Find CSV pipelines
    const csvPipelines = pipelines.filter(p => p.type === 'csv' && p.source);
    
    if (csvPipelines.length >= 2) {
      steps.push({
        type: 'action',
        content: `Found ${csvPipelines.length} CSV pipelines. Comparing ledger balances...`,
        timestamp: new Date().toISOString(),
      });

      try {
        // Read both CSVs and compare ledger balances
        const summaries = await Promise.all(
          csvPipelines.slice(0, 2).map(p => readCSVSummary(p.source))
        );

        const [summary1, summary2] = summaries;
        
        // Calculate ledger balance differences
        if (summary1.columns.includes('leadger_balance') && summary2.columns.includes('leadger_balance')) {
          const data1 = await new Promise((resolve, reject) => {
            const results = [];
            fs.createReadStream(csvPipelines[0].source)
              .pipe(csv())
              .on('data', (d) => results.push(d))
              .on('end', () => resolve(results))
              .on('error', reject);
          });

          const data2 = await new Promise((resolve, reject) => {
            const results = [];
            fs.createReadStream(csvPipelines[1].source)
              .pipe(csv())
              .on('data', (d) => results.push(d))
              .on('end', () => resolve(results))
              .on('error', reject);
          });

          // Create a map by loan_account_id
          const map1 = new Map(data1.map(d => [d.loan_account_id, parseFloat(d.leadger_balance || 0)]));
          const map2 = new Map(data2.map(d => [d.loan_account_id, parseFloat(d.leadger_balance || 0)]));

          const differences = [];
          const allIds = new Set([...map1.keys(), ...map2.keys()]);

          for (const id of allIds) {
            const bal1 = map1.get(id) || 0;
            const bal2 = map2.get(id) || 0;
            const diff = bal1 - bal2;
            
            if (Math.abs(diff) > 0.01) { // Only show significant differences
              differences.push({
                loan_account_id: id,
                scf_v1_balance: bal1,
                scf_v2_balance: bal2,
                difference: diff,
              });
            }
          }

          const totalDiff = differences.reduce((sum, d) => sum + d.difference, 0);
          const totalV1 = Array.from(map1.values()).reduce((a, b) => a + b, 0);
          const totalV2 = Array.from(map2.values()).reduce((a, b) => a + b, 0);

          steps.push({
            type: 'result',
            content: `Reconciliation Analysis Complete:\n\n` +
              `Total Accounts in v1: ${map1.size}\n` +
              `Total Accounts in v2: ${map2.size}\n` +
              `Total Ledger Balance (v1): ${totalV1.toLocaleString()}\n` +
              `Total Ledger Balance (v2): ${totalV2.toLocaleString()}\n` +
              `Total Difference: ${totalDiff.toLocaleString()}\n` +
              `Accounts with Differences: ${differences.length}\n\n` +
              `Top 10 Differences:\n` +
              differences.slice(0, 10).map(d => 
                `  Account ${d.loan_account_id}: v1=${d.scf_v1_balance.toLocaleString()}, v2=${d.scf_v2_balance.toLocaleString()}, diff=${d.difference.toLocaleString()}`
              ).join('\n'),
            timestamp: new Date().toISOString(),
            metadata: {
              totalAccountsV1: map1.size,
              totalAccountsV2: map2.size,
              totalBalanceV1: totalV1,
              totalBalanceV2: totalV2,
              totalDifference: totalDiff,
              differencesCount: differences.length,
              topDifferences: differences.slice(0, 10),
            },
          });
        }
      } catch (error) {
        steps.push({
          type: 'error',
          content: `Error during reconciliation: ${error.message}`,
          timestamp: new Date().toISOString(),
        });
      }
    } else {
      steps.push({
        type: 'error',
        content: 'Need at least 2 CSV pipelines for reconciliation',
        timestamp: new Date().toISOString(),
      });
    }
  } else {
    steps.push({
      type: 'result',
      content: `Query processed: "${query}"\n\nAvailable pipelines: ${pipelines.length}\nCSV pipelines: ${pipelines.filter(p => p.type === 'csv').length}`,
      timestamp: new Date().toISOString(),
    });
  }

  reasoningHistory.push(...steps);
  
  res.json({
    result: steps[steps.length - 1].content,
    steps,
  });
});

// Ingestion API
app.post('/api/ingestion/ingest', async (req, res) => {
  const { config } = req.body;
  res.json({ success: true, message: 'Ingestion started' });
});

app.post('/api/ingestion/validate', async (req, res) => {
  const { config } = req.body;
  res.json({ valid: true, message: 'Configuration is valid' });
});

app.post('/api/ingestion/preview', async (req, res) => {
  const { config } = req.body;
  res.json({ preview: 'Preview data' });
});

app.listen(PORT, () => {
  console.log(`🚀 Backend API server running on http://localhost:${PORT}`);
  console.log(`📊 Ready to handle pipeline and reasoning requests`);
});

