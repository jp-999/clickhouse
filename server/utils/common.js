/**
 * Map JavaScript/JSON types to ClickHouse types
 * @param {string} jsType - JavaScript type
 * @returns {string} - ClickHouse type
 */
const mapJSTypeToClickHouseType = (jsType) => {
  const typeMap = {
    'string': 'String',
    'number': 'Float64',
    'boolean': 'UInt8',
    'object': 'String', // JSON objects are stored as strings
    'date': 'DateTime'
  };
  
  return typeMap[jsType.toLowerCase()] || 'String';
};

/**
 * Format error responses
 * @param {Error} error - Error object
 * @returns {Object} - Formatted error response
 */
const formatErrorResponse = (error) => {
  return {
    success: false,
    message: error.message || 'An unknown error occurred',
    error: error.toString(),
    stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
  };
};

/**
 * Format success responses
 * @param {Object} data - Response data
 * @param {string} message - Success message
 * @returns {Object} - Formatted success response
 */
const formatSuccessResponse = (data, message = 'Operation successful') => {
  return {
    success: true,
    message,
    ...data
  };
};

module.exports = {
  mapJSTypeToClickHouseType,
  formatErrorResponse,
  formatSuccessResponse
}; 