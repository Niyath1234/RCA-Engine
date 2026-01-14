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

// Load metadata from files on startup
function loadMetadata() {
  try {
    // Load tables and convert to pipelines
    const tablesPath = path.join(__dirname, '..', 'metadata', 'tables.json');
    if (fs.existsSync(tablesPath)) {
      const tablesData = JSON.parse(fs.readFileSync(tablesPath, 'utf8'));
      if (tablesData.tables && Array.isArray(tablesData.tables)) {
        pipelines = tablesData.tables.map(table => {
          // Check if CSV file exists
          const csvPath = path.join(__dirname, '..', table.path);
          const fileExists = fs.existsSync(csvPath);
          const fileSize = fileExists ? fs.statSync(csvPath).size : 0;
          
          return {
            id: `pipeline-${table.name}`,
            name: table.name,
            type: 'csv',
            source: table.path,
            system: table.system,
            entity: table.entity,
            description: table.description || `Table: ${table.name}`,
            status: fileExists ? 'active' : 'inactive', // Active if file exists
            createdAt: new Date().toISOString(),
            lastRun: fileExists ? new Date().toISOString() : null,
            metadata: {
              primary_key: table.primary_key,
              time_column: table.time_column,
              grain: table.grain,
              labels: table.labels || [],
              columns: table.columns || [],
              file_exists: fileExists,
              file_size: fileSize
            }
          };
        });
        console.log(`✅ Loaded ${pipelines.length} tables as pipelines`);
      }
    }

    // Load rules
    const rulesPath = path.join(__dirname, '..', 'metadata', 'rules.json');
    if (fs.existsSync(rulesPath)) {
      const rulesData = JSON.parse(fs.readFileSync(rulesPath, 'utf8'));
      if (Array.isArray(rulesData)) {
        rules = rulesData.map(rule => ({
          id: rule.id || `rule-${Date.now()}-${Math.random()}`,
          ...rule,
          createdAt: new Date().toISOString()
        }));
        console.log(`✅ Loaded ${rules.length} rules`);
      }
    }
  } catch (error) {
    console.error('❌ Error loading metadata:', error.message);
  }
}

// Load metadata on startup
loadMetadata();

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
  res.json({ pipelines });
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

  // Try to use Rust RCA engine for queries about UUIDs, mismatches, or paid_amount
  if (query.toLowerCase().includes('uuid') || 
      query.toLowerCase().includes('mismatch') || 
      query.toLowerCase().includes('paid_amount') ||
      query.toLowerCase().includes('difference')) {
    
    try {
      steps.push({
        type: 'action',
        content: 'Running Rust RCA engine to find root causes...',
        timestamp: new Date().toISOString(),
      });

      // Call Rust RCA engine using OneShot command
      const projectRoot = path.join(__dirname, '..');
      const metadataDir = path.join(projectRoot, 'metadata');
      const dataDir = path.join(projectRoot, 'tables');
      const apiKey = process.env.OPENAI_API_KEY || '';
      
      const rustCommand = `one-shot "${query}" --metadata-dir "${metadataDir}" --data-dir "${dataDir}" ${apiKey ? `--api-key "${apiKey}"` : ''} --explain`;
      
      const { stdout, stderr } = await execAsync(
        `cd ${projectRoot} && JSON_OUTPUT=1 ./target/release/rca-engine ${rustCommand}`,
        { maxBuffer: 50 * 1024 * 1024, env: { ...process.env, JSON_OUTPUT: '1' } } // 50MB buffer
      );

      // Parse Rust output (it outputs JSON when JSON_OUTPUT=1)
      let rustResult = null;
      try {
        // Try to extract JSON from output
        const jsonMatch = stdout.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
          rustResult = JSON.parse(jsonMatch[0]);
        } else {
          // If no JSON, try parsing the whole output
          rustResult = JSON.parse(stdout.trim());
        }
      } catch (e) {
        // If JSON parsing fails, use raw output
        console.error('Failed to parse Rust output as JSON:', e.message);
        rustResult = { output: stdout, error: stderr, raw: true };
      }

      // Check if result_data contains RCA results with explanations
      if (rustResult && rustResult.result_data) {
        const resultData = rustResult.result_data;
        
        // Check if LLM-formatted display is available (generalized display decision)
        if (resultData.formatted_display) {
          const formatted = resultData.formatted_display;
          
          steps.push({
            type: 'result',
            content: formatted.display_content || 'RCA Analysis Complete',
            timestamp: new Date().toISOString(),
            metadata: {
              formatted_display: formatted,
              display_format: formatted.display_format,
              key_identifiers: formatted.key_identifiers || [],
              summary_stats: formatted.summary_stats,
              display_metadata: formatted.display_metadata,
              fullResult: rustResult,
            },
          });
        } else {
          // Fallback to legacy extraction logic if formatted_display not available
          // Try to extract explanations/UUIDs from various possible structures
          let explanations = [];
          let uuids = [];
          
          if (resultData.explanations) {
            explanations = resultData.explanations;
          } else if (resultData.rca_result && resultData.rca_result.explanations) {
            explanations = resultData.rca_result.explanations;
          } else if (resultData.row_diff) {
            // If we have row_diff, extract UUIDs from the dataframes
            if (resultData.row_diff.missing_left && resultData.row_diff.missing_left.length > 0) {
              explanations.push({ difference_type: 'MissingInRight', rows: resultData.row_diff.missing_left });
            }
            if (resultData.row_diff.missing_right && resultData.row_diff.missing_right.length > 0) {
              explanations.push({ difference_type: 'MissingInLeft', rows: resultData.row_diff.missing_right });
            }
            if (resultData.row_diff.value_mismatch && resultData.row_diff.value_mismatch.length > 0) {
              explanations.push({ difference_type: 'ValueMismatch', rows: resultData.row_diff.value_mismatch });
            }
          }
          
          // Extract UUIDs from explanations
          explanations.forEach(exp => {
            if (exp.row_id && Array.isArray(exp.row_id) && exp.row_id.length > 0) {
              uuids.push(...exp.row_id);
            } else if (exp.rows && Array.isArray(exp.rows)) {
              exp.rows.forEach(row => {
                if (row.uuid) uuids.push(row.uuid);
                if (row.user_uuid) uuids.push(row.user_uuid);
              });
            }
          });

          steps.push({
            type: 'result',
            content: `RCA Analysis Complete:\n\n` +
              `Found ${explanations.length} root cause explanations\n` +
              (uuids.length > 0 ? `UUIDs causing mismatch: ${uuids.slice(0, 20).join(', ')}${uuids.length > 20 ? ` ... and ${uuids.length - 20} more` : ''}\n` : '') +
              `\nDetails:\n` +
              explanations.slice(0, 5).map((exp, idx) => {
                const rowId = exp.row_id ? exp.row_id.join(', ') : (exp.rows && exp.rows[0] ? JSON.stringify(exp.rows[0]) : 'N/A');
                return `${idx + 1}. Row ID: ${rowId}\n   Type: ${exp.difference_type || 'Unknown'}\n   Confidence: ${exp.confidence ? (exp.confidence * 100).toFixed(1) + '%' : 'N/A'}`;
              }).join('\n\n'),
            timestamp: new Date().toISOString(),
            metadata: {
              explanations: explanations,
              uuids: uuids,
              totalExplanations: explanations.length,
              fullResult: rustResult,
            },
          });
        }
      } else {
        // Fallback: show what we got
        steps.push({
          type: 'result',
          content: `RCA Engine Response:\n\n` +
            `Success: ${rustResult.success || false}\n` +
            `Task Type: ${rustResult.intent?.task_type || 'Unknown'}\n` +
            `Systems: ${rustResult.intent?.systems?.join(', ') || 'None'}\n` +
            `Metrics: ${rustResult.intent?.target_metrics?.join(', ') || 'None'}\n` +
            (stderr ? `\nErrors:\n${stderr}` : ''),
          timestamp: new Date().toISOString(),
          metadata: rustResult,
        });
      }
    } catch (error) {
      steps.push({
        type: 'error',
        content: `Error running Rust RCA engine: ${error.message}\nFalling back to simple comparison...`,
        timestamp: new Date().toISOString(),
      });
      // Fall through to simple comparison
    }
  }

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

// Knowledge Base API
app.get('/api/knowledge-base', (req, res) => {
  try {
    const kbPath = path.join(__dirname, '..', 'metadata', 'knowledge_base.json');
    if (fs.existsSync(kbPath)) {
      const kbData = JSON.parse(fs.readFileSync(kbPath, 'utf8'));
      res.json(kbData);
    } else {
      // Return empty knowledge base structure
      res.json({
        terms: {},
        tables: {},
        relationships: {}
      });
    }
  } catch (error) {
    console.error('Error loading knowledge base:', error);
    res.json({
      terms: {},
      tables: {},
      relationships: {}
    });
  }
});

// Tables API (returns raw table metadata)
app.get('/api/tables', (req, res) => {
  try {
    const tablesPath = path.join(__dirname, '..', 'metadata', 'tables.json');
    if (fs.existsSync(tablesPath)) {
      const tablesData = JSON.parse(fs.readFileSync(tablesPath, 'utf8'));
      res.json(tablesData);
    } else {
      res.json({ tables: [] });
    }
  } catch (error) {
    console.error('Error loading tables:', error);
    res.json({ tables: [] });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Backend API server running on http://localhost:${PORT}`);
  console.log(`📊 Ready to handle pipeline and reasoning requests`);
  console.log(`✅ Loaded ${pipelines.length} pipelines and ${rules.length} rules`);
});

