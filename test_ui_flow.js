const axios = require('axios');

const API_BASE = 'http://localhost:8080/api';

async function testUIFlow() {
  console.log('🧪 Testing RCA Engine UI Flow\n');
  console.log('=' .repeat(50));

  try {
    // Step 1: Create Pipeline 1 (scf_v1.csv)
    console.log('\n📊 Step 1: Creating Pipeline 1 (scf_v1.csv)...');
    const pipeline1 = await axios.post(`${API_BASE}/pipelines`, {
      name: 'SCF v1 CSV Pipeline',
      type: 'csv',
      source: '/Users/niyathnair/Downloads/scf_v1.csv',
      destination: 'data/scf_v1.parquet',
      status: 'inactive',
      config: {},
    });
    console.log('✅ Created:', pipeline1.data.name, `(ID: ${pipeline1.data.id})`);

    // Step 2: Create Pipeline 2 (scf_v2.csv)
    console.log('\n📊 Step 2: Creating Pipeline 2 (scf_v2.csv)...');
    const pipeline2 = await axios.post(`${API_BASE}/pipelines`, {
      name: 'SCF v2 CSV Pipeline',
      type: 'csv',
      source: '/Users/niyathnair/Downloads/scf_v2.csv',
      destination: 'data/scf_v2.parquet',
      status: 'inactive',
      config: {},
    });
    console.log('✅ Created:', pipeline2.data.name, `(ID: ${pipeline2.data.id})`);

    // Step 3: Run Pipeline 1
    console.log('\n🚀 Step 3: Running Pipeline 1...');
    const run1 = await axios.post(`${API_BASE}/pipelines/${pipeline1.data.id}/run`);
    console.log('✅ Pipeline 1 executed:', run1.data.message);
    if (run1.data.summary) {
      console.log(`   - Rows: ${run1.data.summary.rowCount}`);
      console.log(`   - Columns: ${run1.data.summary.columns.length}`);
    }

    // Step 4: Run Pipeline 2
    console.log('\n🚀 Step 4: Running Pipeline 2...');
    const run2 = await axios.post(`${API_BASE}/pipelines/${pipeline2.data.id}/run`);
    console.log('✅ Pipeline 2 executed:', run2.data.message);
    if (run2.data.summary) {
      console.log(`   - Rows: ${run2.data.summary.rowCount}`);
      console.log(`   - Columns: ${run2.data.summary.columns.length}`);
    }

    // Step 5: Query for Reconciliation
    console.log('\n🤔 Step 5: Asking for Ledger Balance Reconciliation...');
    const query = 'recon the ledger balance difference between scf_v1 and scf_v2';
    console.log(`   Query: "${query}"`);
    
    const reasoning = await axios.post(`${API_BASE}/reasoning/query`, {
      query,
      context: {
        pipelines: [pipeline1.data.id, pipeline2.data.id],
      },
    });

    console.log('\n💭 Reasoning Steps:');
    reasoning.data.steps.forEach((step, index) => {
      const icon = {
        thought: '💭',
        action: '⚡',
        result: '✅',
        error: '❌',
      }[step.type] || '📝';
      
      console.log(`\n${icon} Step ${index + 1} [${step.type.toUpperCase()}]`);
      console.log(`   ${step.content}`);
      
      if (step.metadata) {
        console.log(`   Metadata:`, JSON.stringify(step.metadata, null, 2));
      }
    });

    console.log('\n' + '='.repeat(50));
    console.log('✅ Test Complete!');
    console.log('\n📋 Summary:');
    console.log(`   - Pipelines created: 2`);
    console.log(`   - Pipelines executed: 2`);
    console.log(`   - Reasoning steps: ${reasoning.data.steps.length}`);
    console.log(`   - Final result: ${reasoning.data.result.substring(0, 100)}...`);

  } catch (error) {
    console.error('\n❌ Error:', error.message);
    if (error.response) {
      console.error('   Response:', error.response.data);
    }
    process.exit(1);
  }
}

// Run the test
testUIFlow();

