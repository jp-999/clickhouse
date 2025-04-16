# ClickHouse & Flat File Data Ingestion Tool

A bidirectional data connector between ClickHouse and flat files (CSV/TSV) with a web-based user interface.

## Features

### ClickHouse to Flat File
- Connect to ClickHouse using JWT token authentication
- Select tables and columns to export
- Preview data before export
- Support for custom JOIN queries
- Export data to CSV/TSV with configurable delimiter

### Flat File to ClickHouse
- Upload and select flat files (CSV/TSV)
- Auto-detect schema and data types
- Select columns to import
- Preview data before import
- Create new tables in ClickHouse or import to existing tables

## Technology Stack
- **Frontend**: React.js with Material-UI for responsive components
- **Backend**: Node.js with Express for RESTful API endpoints
- **ClickHouse Client**: Official @clickhouse/client library
- **File Processing**: csvtojson for parsing and csv-writer for file generation
- **State Management**: React hooks and context API
- **API Communication**: Axios for HTTP requests
- **Error Handling**: Global error boundaries and detailed server logs

## Prerequisites
- Node.js (v14 or higher)
- npm (v6 or higher)
- Access to a ClickHouse database (local or cloud)

## Installation

### Clone the repository
```bash
git clone <repository-url>
cd clickhouse-file-connector
```

### Setup backend
```bash
cd server
npm install
```

### Setup frontend
```bash
cd ../client
npm install
```

## Configuration
Create a `.env` file in the server directory with the following variables:
```
PORT=5000
NODE_ENV=development
UPLOADS_FOLDER=./public/uploads
MAX_FILE_SIZE=10485760  # 10MB
```

## Running the Application

### Start the backend server
```bash
cd server
npm run dev
```

### Start the frontend development server
```bash
cd client
npm start
```

The application will be available at: http://localhost:3000

## Usage

### ClickHouse to Flat File
1. Navigate to "ClickHouse → File" in the navigation menu
2. Enter ClickHouse connection details (host, port, database, username, JWT token)
3. Test the connection
4. Select a table and columns to export, or use a custom JOIN query
5. Preview the data (optional)
6. Enter an output file name and select a delimiter
7. Click "Export to File"
8. View the export results

### Flat File to ClickHouse
1. Navigate to "File → ClickHouse" in the navigation menu
2. Upload a new file or select an existing one
3. View schema and select columns to import
4. Enter ClickHouse connection details and test the connection
5. Configure the target table and import settings
6. Preview the data (optional)
7. Click "Import to ClickHouse"
8. View the import results

## Example Datasets
You can use ClickHouse example datasets for testing:
- [UK Price Paid](https://clickhouse.com/docs/en/getting-started/example-datasets/uk-price-paid)
- [OnTime](https://clickhouse.com/docs/en/getting-started/example-datasets/ontime)

## Development

### Project Structure
```
clickhouse-file-connector/
├── client/                      # React frontend
│   ├── public/                  # Static assets
│   ├── src/
│   │   ├── components/          # UI components
│   │   │   ├── common/          # Shared components
│   │   │   ├── clickhouse-to-file/  # Export workflow components
│   │   │   ├── file-to-clickhouse/  # Import workflow components
│   │   │   └── layout/          # Page layout components
│   │   ├── contexts/            # React context providers
│   │   ├── services/            # API service clients
│   │   ├── utils/               # Helper functions and constants
│   │   ├── App.js               # Main application component
│   │   └── index.js             # Entry point
├── server/                      # Node.js backend
│   ├── controllers/             # Request handlers
│   │   ├── clickhouseController.js  # ClickHouse operations
│   │   └── fileController.js    # File operations
│   ├── services/                # Business logic
│   │   ├── clickhouse.js        # ClickHouse client and operations
│   │   └── fileService.js       # File processing operations
│   ├── routes/                  # API routes
│   │   ├── clickhouseRoutes.js  # ClickHouse API endpoints
│   │   └── fileRoutes.js        # File API endpoints
│   ├── utils/                   # Helper functions
│   │   ├── error-handler.js     # Global error handling
│   │   └── validation.js        # Request validation
│   ├── middleware/              # Express middleware
│   │   ├── upload.js            # File upload middleware
│   │   └── error.js             # Error handling middleware
│   ├── public/                  # Public assets & uploaded files
│   │   └── uploads/             # Storage for uploaded files
│   └── index.js                 # Main server file
```

### Implementation Details

#### Frontend Components

**ConnectionForm**
- Collects and validates ClickHouse connection parameters
- Provides secure JWT token input
- Tests connection before proceeding with operations
- Persists connection details across sessions using localStorage

**FileUploader**
- Drag-and-drop file upload with progress indication
- File size and type validation
- Lists previously uploaded files with metadata
- Provides file preview functionality

**SchemaViewer**
- Automatic data type detection for columns
- Allows manual type modification
- Column selection for import/export
- Sample data preview

**DataPreview**
- Paginated preview of data before import/export
- Filtering capabilities for large datasets
- Column reordering and visibility controls

**ImportExportPanel**
- Table selection/creation options
- Batch size configuration for large datasets
- Delimiter selection for CSV/TSV formats
- Progress tracking for long-running operations

#### Backend Services

**clickhouse.js**
- Creates secure connection to ClickHouse using JWT authentication
- Handles proper URL formatting for various ClickHouse deployments
- Provides methods for table operations:
  - Table listing and schema retrieval
  - Data querying with filters and limits
  - Batch data insertion with chunking for large datasets
  - Table creation with appropriate column types
  - Custom SQL query execution

**fileService.js**
- Manages the file lifecycle on the server
- Implements CSV parsing with configurable options
- Provides automatic schema detection algorithms
- Handles efficient reading and writing of large files
- Implements memory-efficient streaming for large file operations

### Backend Controllers

**clickhouseController.js**
- Exposes RESTful endpoints for ClickHouse operations
- Validates connection parameters and request payloads
- Handles error scenarios with appropriate HTTP status codes
- Implements query building for complex data operations

**fileController.js**
- Manages file upload workflow and validation
- Processes schema detection requests with type inference
- Coordinates the data import pipeline:
  - File reading and parsing
  - Schema mapping and validation
  - Data transformation if needed
  - ClickHouse table creation or validation
  - Efficient data insertion with progress tracking
- Handles the export workflow from ClickHouse to files

### API Endpoints

#### ClickHouse Routes
- `POST /api/clickhouse/test-connection` - Test ClickHouse connection
- `POST /api/clickhouse/get-tables` - Get tables from ClickHouse
- `POST /api/clickhouse/get-table-schema` - Get table schema (columns)
- `POST /api/clickhouse/preview-data` - Preview data from a table
- `POST /api/clickhouse/export-to-file` - Export data to a flat file
- `POST /api/clickhouse/execute-join-query` - Execute a JOIN query and export results
- `POST /api/clickhouse/debug-connection` - Debug connection issues

#### File Routes
- `GET /api/file/uploaded-files` - Get list of uploaded files
- `POST /api/file/upload` - Upload a new file
- `POST /api/file/get-schema` - Get file schema (detect headers and types)
- `POST /api/file/preview-data` - Preview file data
- `POST /api/file/import-to-clickhouse` - Import file data to ClickHouse

### Error Handling
- Comprehensive error handling at both frontend and backend
- Detailed logging of errors during file operations and database interactions
- User-friendly error messages with actionable suggestions
- Automatic retry mechanisms for transient network failures

### Performance Optimizations
- Batch processing for large dataset imports
- Streaming data handling to minimize memory usage
- Caching of schema information for frequently accessed tables
- Pagination for large data previews

## Troubleshooting

### Common Issues
- **Connection Failures**: Ensure correct hostname, port, and JWT token
- **Import Errors**: Check file format and compatibility with selected data types
- **Export Timeouts**: For large exports, consider using batch processing
- **Permission Issues**: Verify user has appropriate ClickHouse permissions

## Future Enhancements
- Support for additional file formats (JSON, Parquet)
- Schema transformation capabilities
- Scheduled imports and exports
- Visual query builder for complex JOINs
- Data profiling and quality metrics

## License
This project is licensed under the MIT License. 