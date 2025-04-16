const { createClient } = require('@clickhouse/client');

/**
 * Create a ClickHouse client with provided connection details
 * @param {Object} config - The ClickHouse connection configuration
 * @returns {Object} - ClickHouse client instance
 */
const createClickHouseClient = (config) => {
  const { host, port, database, username, jwtToken } = config;
  
  try {
    // Fix: Create proper URL without protocol confusion and port duplication
    let url;
    // Check if host already includes port
    const hasPort = /:\d+$/.test(host);
    
    if (host.startsWith('http://') || host.startsWith('https://')) {
      // Host already has protocol
      url = hasPort ? host : `${host}:${port}`;
    } else {
      // Add protocol
      const protocol = [9440, 8443].includes(Number(port)) ? 'https' : 'http';
      url = hasPort ? `${protocol}://${host}` : `${protocol}://${host}:${port}`;
    }
    
    console.log('Connecting to ClickHouse with URL:', url); // For debugging
    
    const client = createClient({
      host: url,
      database: database,
      username: username,
      password: jwtToken, // Using JWT token for authentication
      request_timeout: 30000 // 30 seconds timeout
    });
    
    return client;
  } catch (error) {
    console.error('ClickHouse client creation error:', error);
    throw new Error(`Failed to create ClickHouse client: ${error.message}`);
  }
};

/**
 * Test connection to ClickHouse
 * @param {Object} client - ClickHouse client instance
 * @returns {Promise<boolean>} - Connection success status
 */
const testConnection = async (client) => {
  try {
    // Simple ping query to test connection
    const pingResult = await client.ping();
    return pingResult.success;
  } catch (error) {
    throw new Error(`ClickHouse connection test failed: ${error.message}`);
  }
};

/**
 * Get list of tables from a ClickHouse database
 * @param {Object} client - ClickHouse client instance
 * @param {string} database - Database name
 * @returns {Promise<Array>} - List of tables
 */
const getTables = async (client, database) => {
  try {
    console.log(`Fetching tables from database: ${database}`);
    const query = `SHOW TABLES FROM ${database}`;
    console.log(`Executing query: ${query}`);
    
    const resultSet = await client.query({
      query,
      format: 'JSONEachRow'
    });
    
    const tables = await resultSet.json();
    console.log(`Tables found: ${JSON.stringify(tables)}`);
    return tables;
  } catch (error) {
    console.error('Error fetching tables:', error);
    throw new Error(`Failed to get tables: ${error.message}`);
  }
};

/**
 * Get table schema (columns) from a ClickHouse table
 * @param {Object} client - ClickHouse client instance
 * @param {string} database - Database name
 * @param {string} table - Table name
 * @returns {Promise<Array>} - List of columns with their types
 */
const getTableSchema = async (client, database, table) => {
  try {
    const query = `DESCRIBE TABLE ${database}.${table}`;
    const resultSet = await client.query({
      query,
      format: 'JSONEachRow'
    });
    
    const columns = await resultSet.json();
    return columns;
  } catch (error) {
    throw new Error(`Failed to get table schema: ${error.message}`);
  }
};

/**
 * Query data from ClickHouse with selected columns
 * @param {Object} client - ClickHouse client instance
 * @param {string} database - Database name
 * @param {string} table - Table name
 * @param {Array} columns - List of columns to select
 * @param {number} limit - Maximum number of rows to fetch
 * @returns {Promise<Array>} - Query results
 */
const queryData = async (client, database, table, columns = ['*'], limit = 100) => {
  try {
    const columnsStr = columns.join(', ');
    const query = `SELECT ${columnsStr} FROM ${database}.${table} LIMIT ${limit}`;
    
    const resultSet = await client.query({
      query,
      format: 'JSONEachRow'
    });
    
    const data = await resultSet.json();
    return data;
  } catch (error) {
    throw new Error(`Failed to query data: ${error.message}`);
  }
};

/**
 * Export data from ClickHouse with selected columns
 * @param {Object} client - ClickHouse client instance
 * @param {string} database - Database name
 * @param {string} table - Table name
 * @param {Array} columns - List of columns to select
 * @returns {Promise<Array>} - Exported data
 */
const exportData = async (client, database, table, columns = ['*']) => {
  try {
    const columnsStr = columns.join(', ');
    const query = `SELECT ${columnsStr} FROM ${database}.${table}`;
    
    const resultSet = await client.query({
      query,
      format: 'JSONEachRow'
    });
    
    const data = await resultSet.json();
    return data;
  } catch (error) {
    throw new Error(`Failed to export data: ${error.message}`);
  }
};

/**
 * Insert data into ClickHouse
 * @param {Object} client - ClickHouse client instance
 * @param {string} database - Database name
 * @param {string} table - Table name
 * @param {Array} data - Data to insert
 * @returns {Promise<Object>} - Result of the insert operation
 */
const insertData = async (client, database, table, data) => {
  try {
    console.log(`Inserting ${data.length} rows into ${database}.${table}`);
    
    // Check if data is valid
    if (!Array.isArray(data) || data.length === 0) {
      throw new Error('No valid data provided for insertion');
    }
    
    // Log sample data for debugging
    console.log('Data sample (first row):', data[0]);
    
    // Perform the insert operation in batches to handle large datasets
    const BATCH_SIZE = 1000;
    let totalInserted = 0;
    
    for (let i = 0; i < data.length; i += BATCH_SIZE) {
      const batch = data.slice(i, i + BATCH_SIZE);
      console.log(`Inserting batch ${i / BATCH_SIZE + 1} (${batch.length} rows)`);
      
      try {
        await client.insert({
          table: `${database}.${table}`,
          values: batch,
          format: 'JSONEachRow'
        });
        
        totalInserted += batch.length;
        console.log(`Successfully inserted batch, total rows so far: ${totalInserted}`);
      } catch (batchError) {
        console.error(`Error inserting batch ${i / BATCH_SIZE + 1}:`, batchError);
        throw batchError;
      }
    }
    
    return { success: true, count: totalInserted };
  } catch (error) {
    console.error('Insert data error:', error);
    console.error('Error stack:', error.stack);
    throw new Error(`Failed to insert data: ${error.message}`);
  }
};

/**
 * Create a new table in ClickHouse
 * @param {Object} client - ClickHouse client instance
 * @param {string} database - Database name
 * @param {string} table - Table name
 * @param {Array} columns - Column definitions
 * @returns {Promise<boolean>} - Success status
 */
const createTable = async (client, database, table, columns) => {
  try {
    console.log(`Creating table ${database}.${table} with columns:`, columns);
    
    if (!columns || columns.length === 0) {
      throw new Error('No columns provided for table creation');
    }
    
    // Format column definitions
    const columnsDefinition = columns.map(col => {
      // Ensure column name is properly quoted if it contains special characters
      const columnName = col.name.includes(' ') ? `\`${col.name}\`` : col.name;
      return `${columnName} ${col.type}`;
    }).join(', ');
    
    const query = `CREATE TABLE IF NOT EXISTS ${database}.${table} (${columnsDefinition}) ENGINE = MergeTree() ORDER BY tuple()`;
    console.log(`Executing query: ${query}`);
    
    await client.query({
      query,
      format: 'JSONEachRow'
    });
    
    console.log(`Table ${database}.${table} created successfully`);
    return true;
  } catch (error) {
    console.error(`Failed to create table ${database}.${table}:`, error);
    console.error('Error stack:', error.stack);
    throw new Error(`Failed to create table: ${error.message}`);
  }
};

/**
 * Execute a custom query on ClickHouse
 * @param {Object} client - ClickHouse client instance
 * @param {string} query - SQL query to execute
 * @returns {Promise<Array>} - Query results
 */
const executeQuery = async (client, query) => {
  try {
    const resultSet = await client.query({
      query,
      format: 'JSONEachRow'
    });
    
    const result = await resultSet.json();
    return result;
  } catch (error) {
    throw new Error(`Failed to execute query: ${error.message}`);
  }
};

module.exports = {
  createClickHouseClient,
  testConnection,
  getTables,
  getTableSchema,
  queryData,
  exportData,
  insertData,
  createTable,
  executeQuery
}; 