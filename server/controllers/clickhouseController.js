const clickhouseService = require('../services/clickhouse');
const fileService = require('../services/fileService');
const path = require('path');

/**
 * Test connection to ClickHouse
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
const testConnection = async (req, res) => {
  try {
    const { host, port, database, username, jwtToken } = req.body;
    
    // Validate required fields
    if (!host || !port || !database || !username || !jwtToken) {
      return res.status(400).json({ 
        success: false, 
        message: 'Missing required connection parameters' 
      });
    }
    
    // Create client
    const client = clickhouseService.createClickHouseClient({
      host, port, database, username, jwtToken
    });
    
    // Test connection
    const result = await clickhouseService.testConnection(client);
    
    res.json({ 
      success: true, 
      message: 'Connection successful',
      connected: result
    });
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      message: `Connection failed: ${error.message}` 
    });
  }
};

/**
 * Get tables from ClickHouse database
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
const getTables = async (req, res) => {
  try {
    console.log('getTables request received', req.body);
    const { host, port, database, username, jwtToken } = req.body;
    
    // Validate required fields
    if (!host || !port || !database || !username || !jwtToken) {
      return res.status(400).json({ 
        success: false, 
        message: 'Missing required connection parameters' 
      });
    }
    
    console.log(`Creating ClickHouse client for ${host}:${port}, database: ${database}`);
    // Create client
    const client = clickhouseService.createClickHouseClient({
      host, port, database, username, jwtToken
    });
    
    console.log('Client created, fetching tables...');
    // Get tables
    const tables = await clickhouseService.getTables(client, database);
    
    console.log(`Successfully fetched ${tables.length} tables`);
    res.json({ 
      success: true, 
      tables 
    });
  } catch (error) {
    console.error('Error in getTables controller:', error);
    console.error('Error stack:', error.stack);
    res.status(500).json({ 
      success: false, 
      message: `Failed to get tables: ${error.message}`,
      details: error.toString(),
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
};

/**
 * Get table schema (columns) from ClickHouse
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
const getTableSchema = async (req, res) => {
  try {
    const { host, port, database, username, jwtToken, table } = req.body;
    
    // Validate required fields
    if (!host || !port || !database || !username || !jwtToken || !table) {
      return res.status(400).json({ 
        success: false, 
        message: 'Missing required parameters' 
      });
    }
    
    // Create client
    const client = clickhouseService.createClickHouseClient({
      host, port, database, username, jwtToken
    });
    
    // Get table schema
    const columns = await clickhouseService.getTableSchema(client, database, table);
    
    res.json({ 
      success: true, 
      columns 
    });
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      message: `Failed to get table schema: ${error.message}` 
    });
  }
};

/**
 * Preview data from ClickHouse table
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
const previewData = async (req, res) => {
  try {
    const { host, port, database, username, jwtToken, table, columns = ['*'], limit = 100 } = req.body;
    
    // Validate required fields
    if (!host || !port || !database || !username || !jwtToken || !table) {
      return res.status(400).json({ 
        success: false, 
        message: 'Missing required parameters' 
      });
    }
    
    // Create client
    const client = clickhouseService.createClickHouseClient({
      host, port, database, username, jwtToken
    });
    
    // Get preview data
    const data = await clickhouseService.queryData(client, database, table, columns, limit);
    
    res.json({ 
      success: true, 
      data,
      count: data.length
    });
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      message: `Failed to preview data: ${error.message}` 
    });
  }
};

/**
 * Export data from ClickHouse to a flat file
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
const exportToFile = async (req, res) => {
  try {
    const { host, port, database, username, jwtToken, table, columns = ['*'], fileName, delimiter = ',' } = req.body;
    
    // Validate required fields
    if (!host || !port || !database || !username || !jwtToken || !table || !fileName) {
      return res.status(400).json({ 
        success: false, 
        message: 'Missing required parameters' 
      });
    }
    
    // Create client
    const client = clickhouseService.createClickHouseClient({
      host, port, database, username, jwtToken
    });
    
    // Export data from ClickHouse
    const data = await clickhouseService.exportData(client, database, table, columns);
    
    // Generate file path with timestamp to avoid overwriting
    const timestamp = Date.now();
    const fileNameWithExt = fileName.endsWith('.csv') ? fileName : `${fileName}.csv`;
    const exportPath = path.join(__dirname, '..', 'uploads', `${timestamp}_${fileNameWithExt}`);
    
    // Write data to CSV file
    await fileService.writeCSV(exportPath, data, {
      delimiter,
      headers: columns.length === 1 && columns[0] === '*' ? Object.keys(data[0] || {}) : columns
    });
    
    res.json({ 
      success: true, 
      message: 'Data exported successfully',
      filePath: exportPath,
      fileName: `${timestamp}_${fileNameWithExt}`,
      count: data.length
    });
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      message: `Failed to export data: ${error.message}` 
    });
  }
};

/**
 * Execute a custom JOIN query and export to file
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
const executeJoinQuery = async (req, res) => {
  try {
    const { host, port, database, username, jwtToken, query, fileName, delimiter = ',' } = req.body;
    
    // Validate required fields
    if (!host || !port || !database || !username || !jwtToken || !query || !fileName) {
      return res.status(400).json({ 
        success: false, 
        message: 'Missing required parameters' 
      });
    }
    
    // Create client
    const client = clickhouseService.createClickHouseClient({
      host, port, database, username, jwtToken
    });
    
    // Execute JOIN query
    const data = await clickhouseService.executeQuery(client, query);
    
    // Generate file path with timestamp to avoid overwriting
    const timestamp = Date.now();
    const fileNameWithExt = fileName.endsWith('.csv') ? fileName : `${fileName}.csv`;
    const exportPath = path.join(__dirname, '..', 'uploads', `${timestamp}_${fileNameWithExt}`);
    
    // Write data to CSV file
    await fileService.writeCSV(exportPath, data, {
      delimiter,
      headers: Object.keys(data[0] || {})
    });
    
    res.json({ 
      success: true, 
      message: 'JOIN query executed and data exported successfully',
      filePath: exportPath,
      fileName: `${timestamp}_${fileNameWithExt}`,
      count: data.length
    });
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      message: `Failed to execute JOIN query: ${error.message}` 
    });
  }
};

module.exports = {
  testConnection,
  getTables,
  getTableSchema,
  previewData,
  exportToFile,
  executeJoinQuery
}; 