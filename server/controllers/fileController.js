const path = require('path');
const fileService = require('../services/fileService');
const clickhouseService = require('../services/clickhouse');
const fs = require('fs-extra');

/**
 * Get list of uploaded files
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
const getUploadedFiles = async (req, res) => {
  try {
    const files = await fileService.getUploadedFiles();
    res.json({
      success: true,
      files
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: `Failed to get uploaded files: ${error.message}`
    });
  }
};

/**
 * Upload a file
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
const uploadFile = async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({
        success: false,
        message: 'No file uploaded'
      });
    }

    const fileInfo = await fileService.getFileInfo(req.file.path);
    
    res.json({
      success: true,
      message: 'File uploaded successfully',
      file: fileInfo
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: `File upload failed: ${error.message}`
    });
  }
};

/**
 * Get file schema (detect headers and types)
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
const getFileSchema = async (req, res) => {
  try {
    const { fileName, delimiter = ',' } = req.body;
    
    if (!fileName) {
      return res.status(400).json({
        success: false,
        message: 'File name is required'
      });
    }
    
    const uploadsDir = path.join(__dirname, '..', 'uploads');
    const filePath = path.join(uploadsDir, fileName);
    
    // Check if file exists
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({
        success: false,
        message: 'File not found'
      });
    }
    
    // Detect schema
    const schema = await fileService.detectCSVSchema(filePath, { delimiter });
    
    res.json({
      success: true,
      schema
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: `Failed to get file schema: ${error.message}`
    });
  }
};

/**
 * Preview file data
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
const previewFileData = async (req, res) => {
  try {
    const { fileName, delimiter = ',', limit = 100 } = req.body;
    
    if (!fileName) {
      return res.status(400).json({
        success: false,
        message: 'File name is required'
      });
    }
    
    const uploadsDir = path.join(__dirname, '..', 'uploads');
    const filePath = path.join(uploadsDir, fileName);
    
    // Check if file exists
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({
        success: false,
        message: 'File not found'
      });
    }
    
    // Read CSV with limit
    const data = await fileService.readCSV(filePath, { 
      delimiter,
      limit
    });
    
    const limitedData = data.slice(0, limit);
    
    res.json({
      success: true,
      data: limitedData,
      count: limitedData.length,
      totalCount: data.length
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: `Failed to preview file data: ${error.message}`
    });
  }
};

/**
 * Import file to ClickHouse
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
const importToClickHouse = async (req, res) => {
  try {
    console.log('Import to ClickHouse request received:', req.body);
    const { 
      fileName, 
      delimiter = ',',
      host, 
      port, 
      database, 
      username, 
      jwtToken,
      tableName,
      columns,
      createTable = false
    } = req.body;
    
    // Validate required fields
    if (!fileName || !host || !port || !database || !username || !jwtToken || !tableName) {
      return res.status(400).json({
        success: false,
        message: 'Missing required parameters'
      });
    }
    
    console.log(`Processing file: ${fileName}, table: ${tableName}, createTable: ${createTable}`);
    console.log(`Selected columns: ${JSON.stringify(columns)}`);
    
    const uploadsDir = path.join(__dirname, '..', 'uploads');
    const filePath = path.join(uploadsDir, fileName);
    
    // Check if file exists
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({
        success: false,
        message: 'File not found'
      });
    }
    
    console.log('Reading CSV data...');
    // Read CSV data
    const data = await fileService.readCSV(filePath, { delimiter });
    console.log(`Read ${data.length} rows from CSV`);
    
    if (data.length === 0) {
      return res.status(400).json({
        success: false,
        message: 'File is empty'
      });
    }
    
    console.log('Creating ClickHouse client...');
    // Create ClickHouse client
    const client = clickhouseService.createClickHouseClient({
      host, port, database, username, jwtToken
    });
    
    // Create table if requested
    if (createTable) {
      console.log('Creating table in ClickHouse...');
      // Get schema from file for column types
      const schema = await fileService.detectCSVSchema(filePath, { delimiter });
      console.log('Detected schema:', schema.columnTypes);
      
      // Convert schema to ClickHouse columns format
      const columnDefinitions = columns.map(col => ({
        name: col,
        type: schema.columnTypes[col] || 'String' // Default to String if type unknown
      }));
      
      console.log('Column definitions:', columnDefinitions);
      await clickhouseService.createTable(client, database, tableName, columnDefinitions);
      console.log('Table created successfully');
    }
    
    // Filter data to only include selected columns
    console.log('Filtering data to selected columns...');
    let filteredData = data;
    if (columns && columns.length > 0) {
      filteredData = data.map(row => {
        const filteredRow = {};
        columns.forEach(col => {
          filteredRow[col] = row[col];
        });
        return filteredRow;
      });
    }
    console.log(`Filtered data to ${filteredData.length} rows`);
    
    // Sample of data being inserted (first row)
    if (filteredData.length > 0) {
      console.log('Sample data (first row):', filteredData[0]);
    }
    
    // Insert data into ClickHouse
    console.log('Inserting data into ClickHouse...');
    const result = await clickhouseService.insertData(client, database, tableName, filteredData);
    console.log('Insert result:', result);
    
    res.json({
      success: true,
      message: 'Data imported to ClickHouse successfully',
      count: result.count
    });
  } catch (error) {
    console.error('Error importing data to ClickHouse:');
    console.error(error);
    console.error('Error stack:', error.stack);
    
    // Send detailed error for debugging
    res.status(500).json({
      success: false,
      message: `Failed to import data to ClickHouse: ${error.message}`,
      details: error.toString(),
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
};

module.exports = {
  getUploadedFiles,
  uploadFile,
  getFileSchema,
  previewFileData,
  importToClickHouse
}; 