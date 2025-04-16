const express = require('express');
const router = express.Router();
const clickhouseController = require('../controllers/clickhouseController');
const { createClient } = require('@clickhouse/client');

// Test connection to ClickHouse
router.post('/test-connection', clickhouseController.testConnection);

// Debug connection to ClickHouse (direct test)
router.post('/debug-connection', async (req, res) => {
  try {
    const { host, port, database, username, jwtToken } = req.body;
    console.log('Debug connection test with:', { host, port, database, username });
    
    // Create proper URL without port duplication
    let url;
    // Check if host already includes port
    const hasPort = /:\d+$/.test(host);
    
    if (host.startsWith('http://') || host.startsWith('https://')) {
      url = hasPort ? host : `${host}:${port}`;
    } else {
      const protocol = [9440, 8443].includes(Number(port)) ? 'https' : 'http';
      url = hasPort ? `${protocol}://${host}` : `${protocol}://${host}:${port}`;
    }
    
    console.log('Connection URL:', url);
    
    const client = createClient({
      host: url,
      database: database,
      username: username,
      password: jwtToken,
      request_timeout: 30000
    });
    
    // Try a simple query
    console.log('Executing test query: SELECT 1');
    const resultSet = await client.query({
      query: 'SELECT 1',
      format: 'JSONEachRow'
    });
    
    const result = await resultSet.json();
    console.log('Query result:', result);
    
    // Try to list tables
    console.log(`Listing tables from database: ${database}`);
    const tablesResult = await client.query({
      query: `SHOW TABLES FROM ${database}`,
      format: 'JSONEachRow'
    });
    
    const tables = await tablesResult.json();
    console.log('Tables found:', tables);
    
    res.json({
      success: true,
      message: 'Debug connection successful',
      testQueryResult: result,
      tables: tables
    });
  } catch (error) {
    console.error('Debug connection error:', error);
    console.error('Error stack:', error.stack);
    
    res.status(500).json({
      success: false,
      message: `Debug connection failed: ${error.message}`,
      details: error.toString(),
      stack: error.stack
    });
  }
});

// Get tables from ClickHouse database
router.post('/get-tables', clickhouseController.getTables);

// Get table schema (columns) from ClickHouse
router.post('/get-table-schema', clickhouseController.getTableSchema);

// Preview data from ClickHouse table
router.post('/preview-data', clickhouseController.previewData);

// Export data from ClickHouse to a flat file
router.post('/export-to-file', clickhouseController.exportToFile);

// Execute a custom JOIN query and export to file
router.post('/execute-join-query', clickhouseController.executeJoinQuery);

module.exports = router; 