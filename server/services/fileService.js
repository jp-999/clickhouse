const fs = require('fs-extra');
const path = require('path');
const csvtojson = require('csvtojson');
const { createObjectCsvWriter } = require('csv-writer');

/**
 * Read a CSV file and convert to JSON
 * @param {string} filePath - Path to the CSV file
 * @param {Object} options - CSV parsing options
 * @returns {Promise<Array>} - JSON data from CSV
 */
const readCSV = async (filePath, options = {}) => {
  try {
    const { delimiter = ',', headers = true } = options;
    
    // Default config for csvtojson
    const config = {
      delimiter,
      noheader: !headers,
      output: 'json'
    };
    
    // If headers are provided, use them
    if (headers && Array.isArray(options.headerList)) {
      config.headers = options.headerList;
    }
    
    const data = await csvtojson(config).fromFile(filePath);
    return data;
  } catch (error) {
    throw new Error(`Failed to read CSV file: ${error.message}`);
  }
};

/**
 * Detect CSV schema (headers and data types)
 * @param {string} filePath - Path to the CSV file
 * @param {Object} options - CSV parsing options
 * @returns {Promise<Object>} - Detected schema
 */
const detectCSVSchema = async (filePath, options = {}) => {
  try {
    const { delimiter = ',', maxRows = 100 } = options;
    
    // Read first few rows to detect schema
    const data = await csvtojson({
      delimiter,
      maxRows
    }).fromFile(filePath);
    
    if (!data || data.length === 0) {
      throw new Error('No data found in file or file is empty');
    }
    
    // Get headers from first row
    const headers = Object.keys(data[0]);
    
    // Detect data types (simple heuristic)
    const columnTypes = {};
    
    headers.forEach(header => {
      // Check first few rows to guess the type
      let isNumber = true;
      let isBoolean = true;
      let isDate = true;
      
      for (const row of data) {
        const value = row[header];
        
        // Skip null/empty values for type detection
        if (value === null || value === undefined || value === '') {
          continue;
        }
        
        // Check if value is a number
        if (isNumber && isNaN(Number(value))) {
          isNumber = false;
        }
        
        // Check if value is a boolean
        if (isBoolean && !['true', 'false', '0', '1'].includes(value.toString().toLowerCase())) {
          isBoolean = false;
        }
        
        // Simple date check (ISO format)
        if (isDate && isNaN(Date.parse(value))) {
          isDate = false;
        }
      }
      
      // Assign type based on checks
      if (isNumber) {
        columnTypes[header] = 'Float64';  // Use Float64 as a default numerical type for ClickHouse
      } else if (isBoolean) {
        columnTypes[header] = 'UInt8';    // ClickHouse uses UInt8 for booleans
      } else if (isDate) {
        columnTypes[header] = 'DateTime'; // Use DateTime for date values
      } else {
        columnTypes[header] = 'String';   // Default to String
      }
    });
    
    return {
      headers,
      columnTypes,
      sampleData: data.slice(0, 10) // Return sample data for preview
    };
  } catch (error) {
    throw new Error(`Failed to detect CSV schema: ${error.message}`);
  }
};

/**
 * Write data to a CSV file
 * @param {string} filePath - Path to save the CSV file
 * @param {Array} data - Data to write to CSV
 * @param {Object} options - CSV writing options
 * @returns {Promise<boolean>} - Success status
 */
const writeCSV = async (filePath, data, options = {}) => {
  try {
    if (!data || data.length === 0) {
      throw new Error('No data provided for CSV export');
    }
    
    const { delimiter = ',', headers = Object.keys(data[0]) } = options;
    
    // Create directory if it doesn't exist
    const dir = path.dirname(filePath);
    await fs.ensureDir(dir);
    
    // Configure CSV writer
    const csvWriter = createObjectCsvWriter({
      path: filePath,
      header: headers.map(id => ({ id, title: id })),
      fieldDelimiter: delimiter
    });
    
    // Write data
    await csvWriter.writeRecords(data);
    
    return true;
  } catch (error) {
    throw new Error(`Failed to write CSV file: ${error.message}`);
  }
};

/**
 * Get a list of files in the uploads directory
 * @returns {Promise<Array>} - List of files
 */
const getUploadedFiles = async () => {
  try {
    const uploadDir = path.join(__dirname, '..', 'uploads');
    await fs.ensureDir(uploadDir);
    
    const files = await fs.readdir(uploadDir);
    return files.map(file => ({
      name: file,
      path: path.join(uploadDir, file),
      size: fs.statSync(path.join(uploadDir, file)).size
    }));
  } catch (error) {
    throw new Error(`Failed to get uploaded files: ${error.message}`);
  }
};

/**
 * Get file information
 * @param {string} filePath - Path to the file
 * @returns {Promise<Object>} - File information
 */
const getFileInfo = async (filePath) => {
  try {
    const stats = await fs.stat(filePath);
    const ext = path.extname(filePath).toLowerCase();
    
    return {
      name: path.basename(filePath),
      path: filePath,
      size: stats.size,
      type: ext === '.csv' ? 'csv' : 'unknown',
      created: stats.birthtime,
      modified: stats.mtime
    };
  } catch (error) {
    throw new Error(`Failed to get file info: ${error.message}`);
  }
};

module.exports = {
  readCSV,
  detectCSVSchema,
  writeCSV,
  getUploadedFiles,
  getFileInfo
}; 