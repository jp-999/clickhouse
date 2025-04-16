import React, { useState, useEffect } from 'react';
import { Card, Form, Button, Alert, Spinner, Table, ListGroup, Badge } from 'react-bootstrap';
import axios from 'axios';

const API_URL = 'http://localhost:5000/api';

const FileToClickHouse = () => {
  // State for form inputs
  const [connectionData, setConnectionData] = useState({
    host: '',
    port: '',
    database: '',
    username: '',
    jwtToken: '',
    tableName: '',
    createTable: true
  });

  const [fileData, setFileData] = useState({
    file: null,
    fileName: '',
    delimiter: ','
  });

  // State for uploaded files
  const [uploadedFiles, setUploadedFiles] = useState([]);
  const [selectedFile, setSelectedFile] = useState('');
  
  // State for file schema and column selection
  const [fileSchema, setFileSchema] = useState(null);
  const [selectedColumns, setSelectedColumns] = useState([]);
  
  // State for notifications and UI state
  const [isConnected, setIsConnected] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [alert, setAlert] = useState({ show: false, variant: '', message: '' });
  const [previewData, setPreviewData] = useState([]);
  const [importResult, setImportResult] = useState(null);

  // Fetch uploaded files on component mount
  useEffect(() => {
    fetchUploadedFiles();
  }, []);

  // Handle connection form input changes
  const handleConnectionChange = (e) => {
    const { name, value } = e.target;
    setConnectionData({
      ...connectionData,
      [name]: value
    });
  };

  // Handle connection checkbox changes
  const handleConnectionCheckbox = (e) => {
    const { name, checked } = e.target;
    setConnectionData({
      ...connectionData,
      [name]: checked
    });
  };

  // Handle file form input changes
  const handleFileChange = (e) => {
    const { name, value } = e.target;
    setFileData({
      ...fileData,
      [name]: value
    });
  };

  // Handle file upload input change
  const handleFileInputChange = (e) => {
    if (e.target.files.length > 0) {
      setFileData({
        ...fileData,
        file: e.target.files[0]
      });
    }
  };

  // Handle file selection from the list of uploaded files
  const handleFileSelect = (e) => {
    const fileName = e.target.value;
    setSelectedFile(fileName);
    setSelectedColumns([]);
    setFileSchema(null);
    setPreviewData([]);
    
    if (fileName) {
      fetchFileSchema(fileName);
    }
  };

  // Test connection to ClickHouse
  const testConnection = async () => {
    setIsLoading(true);
    setAlert({ show: false, variant: '', message: '' });

    try {
      const { host, port, database, username, jwtToken } = connectionData;
      
      // Validate required fields
      if (!host || !port || !database || !username || !jwtToken) {
        throw new Error('Please fill in all required connection fields');
      }

      const response = await axios.post(`${API_URL}/clickhouse/test-connection`, {
        host, port, database, username, jwtToken
      });

      if (response.data.success) {
        setIsConnected(true);
        setAlert({ 
          show: true, 
          variant: 'success', 
          message: 'Connection successful!'
        });
      } else {
        throw new Error(response.data.message || 'Connection failed');
      }
    } catch (error) {
      setIsConnected(false);
      setAlert({ 
        show: true, 
        variant: 'danger', 
        message: `Connection failed: ${error.message}`
      });
    } finally {
      setIsLoading(false);
    }
  };

  // Fetch list of uploaded files
  const fetchUploadedFiles = async () => {
    setIsLoading(true);
    
    try {
      const response = await axios.get(`${API_URL}/file/uploaded-files`);

      if (response.data.success) {
        setUploadedFiles(response.data.files || []);
      } else {
        throw new Error(response.data.message || 'Failed to fetch uploaded files');
      }
    } catch (error) {
      setAlert({ 
        show: true, 
        variant: 'danger', 
        message: `Failed to fetch uploaded files: ${error.message}`
      });
    } finally {
      setIsLoading(false);
    }
  };

  // Upload a new file
  const uploadFile = async () => {
    if (!fileData.file) {
      setAlert({
        show: true,
        variant: 'warning',
        message: 'Please select a file to upload'
      });
      return;
    }

    setIsLoading(true);
    setAlert({ show: false, variant: '', message: '' });
    
    try {
      const formData = new FormData();
      formData.append('file', fileData.file);

      const response = await axios.post(`${API_URL}/file/upload`, formData, {
        headers: {
          'Content-Type': 'multipart/form-data'
        }
      });

      if (response.data.success) {
        setAlert({ 
          show: true, 
          variant: 'success', 
          message: 'File uploaded successfully'
        });
        
        // Update the list of uploaded files
        fetchUploadedFiles();
        
        // Reset file input
        setFileData({
          ...fileData,
          file: null
        });
        
        // Set the newly uploaded file as selected
        setSelectedFile(response.data.file.name);
        
        // Fetch schema for the uploaded file
        fetchFileSchema(response.data.file.name);
      } else {
        throw new Error(response.data.message || 'Upload failed');
      }
    } catch (error) {
      setAlert({ 
        show: true, 
        variant: 'danger', 
        message: `File upload failed: ${error.message}`
      });
    } finally {
      setIsLoading(false);
    }
  };

  // Fetch schema for the selected file
  const fetchFileSchema = async (fileName) => {
    setIsLoading(true);
    setFileSchema(null);
    
    try {
      const response = await axios.post(`${API_URL}/file/get-schema`, {
        fileName,
        delimiter: fileData.delimiter
      });

      if (response.data.success && response.data.schema) {
        setFileSchema(response.data.schema);
        // By default, select all columns
        setSelectedColumns(response.data.schema.headers);
      } else {
        throw new Error(response.data.message || 'Failed to get file schema');
      }
    } catch (error) {
      setAlert({ 
        show: true, 
        variant: 'danger', 
        message: `Failed to get file schema: ${error.message}`
      });
    } finally {
      setIsLoading(false);
    }
  };

  // Handle column selection/deselection
  const handleColumnSelect = (columnName) => {
    setSelectedColumns(prevSelected => {
      if (prevSelected.includes(columnName)) {
        return prevSelected.filter(col => col !== columnName);
      } else {
        return [...prevSelected, columnName];
      }
    });
  };

  // Select all columns
  const selectAllColumns = () => {
    if (fileSchema && fileSchema.headers) {
      setSelectedColumns(fileSchema.headers);
    }
  };

  // Deselect all columns
  const deselectAllColumns = () => {
    setSelectedColumns([]);
  };

  // Preview file data
  const handlePreview = async () => {
    if (!selectedFile) {
      setAlert({
        show: true,
        variant: 'warning',
        message: 'Please select a file first'
      });
      return;
    }

    setIsLoading(true);
    setPreviewData([]);
    
    try {
      const response = await axios.post(`${API_URL}/file/preview-data`, {
        fileName: selectedFile,
        delimiter: fileData.delimiter,
        limit: 100
      });

      if (response.data.success) {
        setPreviewData(response.data.data || []);
      } else {
        throw new Error(response.data.message || 'Failed to preview data');
      }
    } catch (error) {
      setAlert({ 
        show: true, 
        variant: 'danger', 
        message: `Failed to preview data: ${error.message}`
      });
    } finally {
      setIsLoading(false);
    }
  };

  // Import file to ClickHouse
  const handleImport = async () => {
    if (!selectedFile) {
      setAlert({
        show: true,
        variant: 'warning',
        message: 'Please select a file to import'
      });
      return;
    }

    if (selectedColumns.length === 0) {
      setAlert({
        show: true,
        variant: 'warning',
        message: 'Please select at least one column to import'
      });
      return;
    }

    if (!connectionData.tableName) {
      setAlert({
        show: true,
        variant: 'warning',
        message: 'Please enter a table name for the import'
      });
      return;
    }

    if (!isConnected) {
      setAlert({
        show: true,
        variant: 'warning',
        message: 'Please test and confirm the ClickHouse connection first'
      });
      return;
    }

    setIsLoading(true);
    setImportResult(null);
    
    try {
      const { host, port, database, username, jwtToken, tableName, createTable } = connectionData;
      
      const response = await axios.post(`${API_URL}/file/import-to-clickhouse`, {
        fileName: selectedFile,
        delimiter: fileData.delimiter,
        host, port, database, username, jwtToken,
        tableName,
        columns: selectedColumns,
        createTable
      });

      if (response.data.success) {
        setImportResult(response.data);
        setAlert({ 
          show: true, 
          variant: 'success', 
          message: `Successfully imported ${response.data.count} records to ClickHouse`
        });
      } else {
        throw new Error(response.data.message || 'Import failed');
      }
    } catch (error) {
      setAlert({ 
        show: true, 
        variant: 'danger', 
        message: `Import failed: ${error.message}`
      });
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div>
      <h2 className="mb-4">Import from Flat File to ClickHouse</h2>
      
      {alert.show && (
        <Alert variant={alert.variant} onClose={() => setAlert({...alert, show: false})} dismissible>
          {alert.message}
        </Alert>
      )}
      
      <div className="row">
        <div className="col-md-6">
          <Card className="mb-4">
            <Card.Header>File Selection and Upload</Card.Header>
            <Card.Body>
              <Form>
                <Form.Group className="mb-3">
                  <Form.Label>Upload New File</Form.Label>
                  <div className="d-flex">
                    <Form.Control 
                      type="file" 
                      accept=".csv,.tsv,.txt"
                      onChange={handleFileInputChange}
                      className="me-2"
                    />
                    <Button 
                      variant="primary" 
                      onClick={uploadFile}
                      disabled={isLoading || !fileData.file}
                    >
                      {isLoading ? <Spinner animation="border" size="sm" /> : 'Upload'}
                    </Button>
                  </div>
                </Form.Group>
                
                <Form.Group className="mb-3">
                  <Form.Label>Select Uploaded File</Form.Label>
                  <Form.Select 
                    value={selectedFile} 
                    onChange={handleFileSelect}
                  >
                    <option value="">-- Select a file --</option>
                    {uploadedFiles.map((file, index) => (
                      <option key={index} value={file.name}>
                        {file.name}
                      </option>
                    ))}
                  </Form.Select>
                </Form.Group>
                
                <Form.Group className="mb-3">
                  <Form.Label>Delimiter</Form.Label>
                  <Form.Select 
                    name="delimiter" 
                    value={fileData.delimiter} 
                    onChange={handleFileChange}
                  >
                    <option value=",">Comma (,)</option>
                    <option value="\t">Tab</option>
                    <option value=";">Semicolon (;)</option>
                    <option value="|">Pipe (|)</option>
                  </Form.Select>
                </Form.Group>
                
                <Button 
                  variant="secondary" 
                  onClick={handlePreview} 
                  disabled={isLoading || !selectedFile}
                >
                  {isLoading ? <><Spinner animation="border" size="sm" /> Previewing...</> : 'Preview Data'}
                </Button>
              </Form>
            </Card.Body>
          </Card>
          
          {fileSchema && (
            <Card className="mb-4">
              <Card.Header>Column Selection</Card.Header>
              <Card.Body>
                <Form>
                  <div className="mb-2">
                    <Button 
                      variant="outline-primary" 
                      size="sm" 
                      onClick={selectAllColumns}
                      className="me-2"
                    >
                      Select All
                    </Button>
                    <Button 
                      variant="outline-secondary" 
                      size="sm" 
                      onClick={deselectAllColumns}
                    >
                      Deselect All
                    </Button>
                  </div>
                  <div className="column-select-container" style={{ maxHeight: '200px', overflowY: 'auto', border: '1px solid #ced4da', borderRadius: '0.25rem', padding: '10px' }}>
                    {fileSchema.headers.map((header, index) => (
                      <Form.Check 
                        key={index}
                        type="checkbox" 
                        label={`${header} (${fileSchema.columnTypes[header] || 'Unknown'})`}
                        checked={selectedColumns.includes(header)}
                        onChange={() => handleColumnSelect(header)}
                        className="mb-1"
                      />
                    ))}
                  </div>
                </Form>
              </Card.Body>
            </Card>
          )}
        </div>
        
        <div className="col-md-6">
          <Card className="mb-4">
            <Card.Header>ClickHouse Connection</Card.Header>
            <Card.Body>
              <Form>
                <div className="row">
                  <div className="col-md-6">
                    <Form.Group className="mb-3">
                      <Form.Label>Host</Form.Label>
                      <Form.Control 
                        type="text" 
                        name="host" 
                        value={connectionData.host} 
                        onChange={handleConnectionChange}
                        placeholder="e.g., play.clickhouse.com" 
                        disabled={isConnected}
                      />
                    </Form.Group>
                  </div>
                  <div className="col-md-6">
                    <Form.Group className="mb-3">
                      <Form.Label>Port</Form.Label>
                      <Form.Control 
                        type="text" 
                        name="port" 
                        value={connectionData.port} 
                        onChange={handleConnectionChange}
                        placeholder="e.g., 9440 for https, 9000 for http" 
                        disabled={isConnected}
                      />
                    </Form.Group>
                  </div>
                </div>
                
                <div className="row">
                  <div className="col-md-4">
                    <Form.Group className="mb-3">
                      <Form.Label>Database</Form.Label>
                      <Form.Control 
                        type="text" 
                        name="database" 
                        value={connectionData.database} 
                        onChange={handleConnectionChange}
                        placeholder="Database name" 
                        disabled={isConnected}
                      />
                    </Form.Group>
                  </div>
                  <div className="col-md-4">
                    <Form.Group className="mb-3">
                      <Form.Label>Username</Form.Label>
                      <Form.Control 
                        type="text" 
                        name="username" 
                        value={connectionData.username} 
                        onChange={handleConnectionChange}
                        placeholder="Username" 
                        disabled={isConnected}
                      />
                    </Form.Group>
                  </div>
                  <div className="col-md-4">
                    <Form.Group className="mb-3">
                      <Form.Label>JWT Token</Form.Label>
                      <Form.Control 
                        type="password" 
                        name="jwtToken" 
                        value={connectionData.jwtToken} 
                        onChange={handleConnectionChange}
                        placeholder="JWT Token" 
                        disabled={isConnected}
                      />
                    </Form.Group>
                  </div>
                </div>
                
                <Button 
                  variant="primary" 
                  onClick={testConnection} 
                  disabled={isLoading || isConnected}
                  className="me-2"
                >
                  {isLoading ? <><Spinner animation="border" size="sm" /> Testing...</> : 'Test Connection'}
                </Button>
                
                {isConnected && (
                  <Button 
                    variant="outline-secondary" 
                    onClick={() => {
                      setIsConnected(false);
                      setImportResult(null);
                    }}
                  >
                    Disconnect
                  </Button>
                )}
              </Form>
            </Card.Body>
          </Card>
          
          <Card className="mb-4">
            <Card.Header>Import Configuration</Card.Header>
            <Card.Body>
              <Form>
                <Form.Group className="mb-3">
                  <Form.Label>Target Table Name</Form.Label>
                  <Form.Control 
                    type="text" 
                    name="tableName" 
                    value={connectionData.tableName} 
                    onChange={handleConnectionChange}
                    placeholder="Enter table name"
                  />
                </Form.Group>
                
                <Form.Group className="mb-3">
                  <Form.Check 
                    type="checkbox" 
                    label="Create table if not exists" 
                    name="createTable"
                    checked={connectionData.createTable}
                    onChange={handleConnectionCheckbox}
                  />
                </Form.Group>
                
                <Button 
                  variant="success" 
                  onClick={handleImport} 
                  disabled={isLoading || !selectedFile || !connectionData.tableName || selectedColumns.length === 0 || !isConnected}
                >
                  {isLoading ? <><Spinner animation="border" size="sm" /> Importing...</> : 'Import to ClickHouse'}
                </Button>
              </Form>
            </Card.Body>
          </Card>
          
          {/* Import Result */}
          {importResult && (
            <Card className="mb-4">
              <Card.Header>Import Result</Card.Header>
              <Card.Body>
                <div className="mb-3">
                  <Badge bg="success">Success</Badge>
                </div>
                <ListGroup>
                  <ListGroup.Item>
                    <strong>Records Imported:</strong> {importResult.count}
                  </ListGroup.Item>
                  <ListGroup.Item>
                    <strong>Target Table:</strong> {connectionData.tableName}
                  </ListGroup.Item>
                </ListGroup>
              </Card.Body>
            </Card>
          )}
        </div>
      </div>
      
      {/* Preview Data */}
      {previewData.length > 0 && (
        <Card className="mb-4">
          <Card.Header>Data Preview (First 100 records)</Card.Header>
          <Card.Body style={{ overflowX: 'auto' }}>
            <Table striped bordered hover>
              <thead>
                <tr>
                  {Object.keys(previewData[0]).map((key, index) => (
                    <th key={index}>{key}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {previewData.map((row, rowIndex) => (
                  <tr key={rowIndex}>
                    {Object.values(row).map((value, colIndex) => (
                      <td key={colIndex}>
                        {value === null ? 'NULL' : 
                         typeof value === 'object' ? JSON.stringify(value) : 
                         String(value)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </Table>
          </Card.Body>
        </Card>
      )}
    </div>
  );
};

export default FileToClickHouse; 