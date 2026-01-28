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
      metadata: {
        ingest_table: '/api/metadata/ingest/table',
        ingest_join: '/api/metadata/ingest/join',
        ingest_rules: '/api/metadata/ingest/rules',
        ingest_complete: '/api/metadata/ingest/complete',
      },
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

// Query Regeneration API - Load metadata and generate SQL from natural language
// Query Builder API - Load metadata and business rules, build SQL from natural language
app.get('/api/query/load-prerequisites', async (req, res) => {
  try {
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'query_regeneration_api.py');
    
    const { stdout, stderr } = await execAsync(`cd ${projectRoot} && python3 ${scriptPath} load`, {
      maxBuffer: 10 * 1024 * 1024,
    });
    
    const result = JSON.parse(stdout.trim());
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.stderr || error.stdout,
    });
  }
});

app.post('/api/query/generate-sql', async (req, res) => {
  try {
    const { query, use_llm } = req.body;
    
    if (!query) {
      return res.status(400).json({ error: 'Query is required' });
    }
    
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'query_regeneration_api.py');
    
    // Pass query via stdin, with use_llm flag (defaults to true if OPENAI_API_KEY is set)
    const useLLMFlag = use_llm !== false && process.env.OPENAI_API_KEY ? true : false;
    const inputData = JSON.stringify({ command: 'generate', query, use_llm: useLLMFlag });
    
    // Pass environment variables (especially OPENAI_API_KEY) to Python script
    const env = { ...process.env };
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && echo '${inputData}' | python3 ${scriptPath}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: env, // Pass all environment variables including OPENAI_API_KEY
      }
    );
    
    const result = JSON.parse(stdout.trim());
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.stderr || error.stdout,
    });
  }
});

// Reasoning API - Uses LLM Query Generator with Chain of Thought
app.post('/api/reasoning/query', async (req, res) => {
  const { query, context } = req.body;
  
  if (!query) {
    return res.status(400).json({ error: 'Query is required' });
  }
  
  try {
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'query_regeneration_api.py');
    
    // Use LLM by default if API key is available
    const useLLM = process.env.OPENAI_API_KEY ? true : false;
    const inputData = JSON.stringify({ command: 'generate', query, use_llm: useLLM });
    
    // Pass environment variables (especially OPENAI_API_KEY) to Python script
    const env = { ...process.env };
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && echo '${inputData}' | python3 ${scriptPath}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: env,
      }
    );
    
    const result = JSON.parse(stdout.trim());
    
    // Convert reasoning_steps to the format expected by UI
    const steps = [];
    
    // Add initial step
    steps.push({
      type: 'thought',
      content: `🔍 Analyzing query: "${query}"`,
      timestamp: new Date().toISOString(),
    });
    
    // Add reasoning steps from LLM if available
    if (result.reasoning_steps && Array.isArray(result.reasoning_steps)) {
      result.reasoning_steps.forEach((stepContent, index) => {
        // Determine step type based on content
        let stepType = 'thought';
        if (stepContent.includes('✅') || stepContent.includes('Generated')) {
          stepType = 'result';
        } else if (stepContent.includes('❌') || stepContent.includes('Error')) {
          stepType = 'error';
        } else if (stepContent.includes('🔧') || stepContent.includes('Building')) {
          stepType = 'action';
        } else if (stepContent.includes('📊') || stepContent.includes('SQL')) {
          stepType = 'result';
        }
        
        steps.push({
          type: stepType,
          content: stepContent,
          timestamp: new Date(Date.now() + index * 100).toISOString(), // Slight delay for ordering
        });
      });
    } else {
      // Fallback: add basic steps
      steps.push({
        type: 'thought',
        content: '📊 Loading metadata and analyzing available tables...',
        timestamp: new Date().toISOString(),
      });
      
      if (result.success) {
        steps.push({
          type: 'action',
          content: '🤖 Generating SQL using LLM with comprehensive context...',
          timestamp: new Date().toISOString(),
        });
        
        if (result.sql) {
          steps.push({
            type: 'result',
            content: `✅ Generated SQL:\n\n\`\`\`sql\n${result.sql}\n\`\`\``,
            timestamp: new Date().toISOString(),
          });
        }
      } else {
        steps.push({
          type: 'error',
          content: `❌ Error: ${result.error || 'Unknown error'}`,
          timestamp: new Date().toISOString(),
        });
      }
    }
    
    // Add SQL result if available
    if (result.sql && !steps.some(s => s.content.includes(result.sql.substring(0, 50)))) {
      steps.push({
        type: 'result',
        content: `\`\`\`sql\n${result.sql}\n\`\`\``,
        timestamp: new Date().toISOString(),
      });
    }
    
    // Add warnings if any
    if (result.warnings) {
      steps.push({
        type: 'thought',
        content: `⚠️  Warnings: ${result.warnings}`,
        timestamp: new Date().toISOString(),
      });
    }
    
    reasoningHistory.push(...steps);
    
    res.json({
      result: result.sql || result.error || 'Query processed',
      steps,
      sql: result.sql,
      intent: result.intent,
      method: result.method || 'llm_with_full_context',
    });
  } catch (error) {
    const errorSteps = [
      {
        type: 'error',
        content: `❌ Error processing query: ${error.message}`,
        timestamp: new Date().toISOString(),
      },
    ];
    
    res.status(500).json({
      result: `Error: ${error.message}`,
      steps: errorSteps,
      error: error.message,
    });
  }
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

// Metadata Ingestion API - Natural Language to Structured JSON
app.post('/api/metadata/ingest/table', async (req, res) => {
  try {
    const { table_description, system, output_file } = req.body;
    
    if (!table_description) {
      return res.status(400).json({ error: 'table_description is required' });
    }
    
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'metadata_ingestion_api.py');
    
    // Build command arguments
    const args = ['table', JSON.stringify(table_description)];
    if (system) args.push(system);
    
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && python3 ${scriptPath} ${args.join(' ')}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: { ...process.env },
      }
    );
    
    const result = JSON.parse(stdout.trim());
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.stderr || error.stdout,
    });
  }
});

app.post('/api/metadata/ingest/join', async (req, res) => {
  try {
    const { join_condition, output_file } = req.body;
    
    if (!join_condition) {
      return res.status(400).json({ error: 'join_condition is required' });
    }
    
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'metadata_ingestion_api.py');
    
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && python3 ${scriptPath} join ${JSON.stringify(join_condition)}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: { ...process.env },
      }
    );
    
    const result = JSON.parse(stdout.trim());
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.stderr || error.stdout,
    });
  }
});

app.post('/api/metadata/ingest/rules', async (req, res) => {
  try {
    const { rules_text, output_file } = req.body;
    
    if (!rules_text) {
      return res.status(400).json({ error: 'rules_text is required' });
    }
    
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'metadata_ingestion_api.py');
    
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && python3 ${scriptPath} rules ${JSON.stringify(rules_text)}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: { ...process.env },
      }
    );
    
    const result = JSON.parse(stdout.trim());
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.stderr || error.stdout,
    });
  }
});

app.post('/api/metadata/ingest/complete', async (req, res) => {
  try {
    const { metadata_text, system } = req.body;
    
    if (!metadata_text) {
      return res.status(400).json({ error: 'metadata_text is required' });
    }
    
    const path = require('path');
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    const projectRoot = path.join(__dirname, '..');
    const scriptPath = path.join(__dirname, 'metadata_ingestion_api.py');
    
    // Build command arguments
    const args = ['complete', JSON.stringify(metadata_text)];
    if (system) args.push(system);
    
    const { stdout, stderr } = await execAsync(
      `cd ${projectRoot} && python3 ${scriptPath} ${args.join(' ')}`,
      {
        maxBuffer: 10 * 1024 * 1024,
        env: { ...process.env },
      }
    );
    
    const result = JSON.parse(stdout.trim());
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
      details: error.stderr || error.stdout,
    });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Backend API server running on http://localhost:${PORT}`);
  console.log(`📊 Ready to handle pipeline and reasoning requests`);
});

