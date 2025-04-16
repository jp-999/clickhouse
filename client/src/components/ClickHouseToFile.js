import React, { useState } from 'react';
import { Card, Form, Button, Alert, Spinner, Table, ListGroup, Badge } from 'react-bootstrap';
import axios from 'axios';

const API_URL = 'http://localhost:5000/api';

const ClickHouseToFile = () => {
  // State for form inputs
  const [formData, setFormData] = useState({
    host: '',
    port: '',
    database: '',
    username: '',
    jwtToken: '',
    fileName: '',
    delimiter: ',',
    customQuery: '',
    isJoinQuery: false
  });

  // State for tables, columns, and selected items
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);

  // State for notifications and UI state
  const [isConnected, setIsConnected] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [alert, setAlert] = useState({ show: false, variant: '', message: '' });
  const [previewData, setPreviewData] = useState([]);
  const [exportResult, setExportResult] = useState(null);

  // Handle form input changes
  const handleInputChange = (e) => {
    const { name, value } = e.target;
    setFormData({
      ...formData,
      [name]: value
    });
  };

  // Handle checkbox for JOIN query
  const handleJoinQueryChange = (e) => {
    setFormData({
      ...formData,
      isJoinQuery: e.target.checked
    });
  };

  // Test connection to ClickHouse
  const testConnection = async () => {
    setIsLoading(true);
    setAlert({ show: false, variant: '', message: '' });

    try {
      const { host, port, database, username, jwtToken } = formData;
      
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
          message: 'Connection successful! You can now fetch tables.'
        });
        
        // Fetch tables after successful connection
        fetchTables();
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

  // Fetch tables from ClickHouse
  const fetchTables = async () => {
    setIsLoading(true);
    
    try {
      const { host, port, database, username, jwtToken } = formData;
      
      const response = await axios.post(`${API_URL}/clickhouse/get-tables`, {
        host, port, database, username, jwtToken
      });

      if (response.data.success && response.data.tables) {
        setTables(response.data.tables);
      } else {
        throw new Error(response.data.message || 'Failed to fetch tables');
      }
    } catch (error) {
      setAlert({ 
        show: true, 
        variant: 'danger', 
        message: `Failed to fetch tables: ${error.message}`
      });
    } finally {
      setIsLoading(false);
    }
  };

  // Handle table selection
  const handleTableSelect = async (e) => {
    const tableName = e.target.value;
    setSelectedTable(tableName);
    setSelectedColumns([]);
    setColumns([]);
    setPreviewData([]);
    
    if (!tableName) return;
    
    setIsLoading(true);
    
    try {
      const { host, port, database, username, jwtToken } = formData;
      
      const response = await axios.post(`${API_URL}/clickhouse/get-table-schema`, {
        host, port, database, username, jwtToken, table: tableName
      });

      if (response.data.success && response.data.columns) {
        setColumns(response.data.columns);
      } else {
        throw new Error(response.data.message || 'Failed to fetch columns');
      }
    } catch (error) {
      setAlert({ 
        show: true, 
        variant: 'danger', 
        message: `Failed to fetch columns: ${error.message}`
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
    const allColumnNames = columns.map(col => col.name);
    setSelectedColumns(allColumnNames);
  };

  // Deselect all columns
  const deselectAllColumns = () => {
    setSelectedColumns([]);
  };

  // Preview data
  const handlePreview = async () => {
    if (formData.isJoinQuery && !formData.customQuery) {
      setAlert({
        show: true,
        variant: 'warning',
        message: 'Please enter a custom JOIN query to preview'
      });
      return;
    }

    setIsLoading(true);
    setPreviewData([]);
    
    try {
      const { host, port, database, username, jwtToken } = formData;
      
      if (formData.isJoinQuery) {
        // Preview with custom JOIN query
        const response = await axios.post(`${API_URL}/clickhouse/execute-join-query`, {
          host, port, database, username, jwtToken, 
          query: formData.customQuery,
          fileName: 'preview', // Not used for preview, just for API validation
        });

        if (response.data.success) {
          setPreviewData(response.data.data || []);
        } else {
          throw new Error(response.data.message || 'Failed to preview data');
        }
      } else {
        // Regular table preview
        if (!selectedTable) {
          throw new Error('Please select a table first');
        }

        const columnsToFetch = selectedColumns.length > 0 ? selectedColumns : ['*'];
        
        const response = await axios.post(`${API_URL}/clickhouse/preview-data`, {
          host, port, database, username, jwtToken,
          table: selectedTable,
          columns: columnsToFetch,
          limit: 100
        });

        if (response.data.success) {
          setPreviewData(response.data.data || []);
        } else {
          throw new Error(response.data.message || 'Failed to preview data');
        }
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

  // Export data to file
  const handleExport = async () => {
    if (!formData.fileName) {
      setAlert({
        show: true,
        variant: 'warning',
        message: 'Please enter a file name for export'
      });
      return;
    }

    if (formData.isJoinQuery && !formData.customQuery) {
      setAlert({
        show: true,
        variant: 'warning',
        message: 'Please enter a custom JOIN query to export'
      });
      return;
    }

    setIsLoading(true);
    setExportResult(null);
    
    try {
      const { host, port, database, username, jwtToken, fileName, delimiter } = formData;
      
      if (formData.isJoinQuery) {
        // Export with custom JOIN query
        const response = await axios.post(`${API_URL}/clickhouse/execute-join-query`, {
          host, port, database, username, jwtToken, 
          query: formData.customQuery,
          fileName,
          delimiter
        });

        if (response.data.success) {
          setExportResult(response.data);
          setAlert({ 
            show: true, 
            variant: 'success', 
            message: `Successfully exported ${response.data.count} records to file`
          });
        } else {
          throw new Error(response.data.message || 'Export failed');
        }
      } else {
        // Regular table export
        if (!selectedTable) {
          throw new Error('Please select a table first');
        }

        const columnsToExport = selectedColumns.length > 0 ? selectedColumns : ['*'];
        
        const response = await axios.post(`${API_URL}/clickhouse/export-to-file`, {
          host, port, database, username, jwtToken,
          table: selectedTable,
          columns: columnsToExport,
          fileName,
          delimiter
        });

        if (response.data.success) {
          setExportResult(response.data);
          setAlert({ 
            show: true, 
            variant: 'success', 
            message: `Successfully exported ${response.data.count} records to file`
          });
        } else {
          throw new Error(response.data.message || 'Export failed');
        }
      }
    } catch (error) {
      setAlert({ 
        show: true, 
        variant: 'danger', 
        message: `Export failed: ${error.message}`
      });
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div>
      <h2 className="mb-4">Export from ClickHouse to Flat File</h2>
      
      {alert.show && (
        <Alert variant={alert.variant} onClose={() => setAlert({...alert, show: false})} dismissible>
          {alert.message}
        </Alert>
      )}
      
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
                    value={formData.host} 
                    onChange={handleInputChange}
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
                    value={formData.port} 
                    onChange={handleInputChange}
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
                    value={formData.database} 
                    onChange={handleInputChange}
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
                    value={formData.username} 
                    onChange={handleInputChange}
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
                    value={formData.jwtToken} 
                    onChange={handleInputChange}
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
                  setTables([]);
                  setSelectedTable('');
                  setColumns([]);
                  setSelectedColumns([]);
                  setPreviewData([]);
                  setExportResult(null);
                }}
              >
                Disconnect
              </Button>
            )}
          </Form>
        </Card.Body>
      </Card>
      
      {isConnected && (
        <>
          <Card className="mb-4">
            <Card.Header>Data Selection</Card.Header>
            <Card.Body>
              <Form>
                <Form.Group className="mb-3">
                  <Form.Check 
                    type="checkbox" 
                    label="Use Custom JOIN Query" 
                    checked={formData.isJoinQuery}
                    onChange={handleJoinQueryChange}
                  />
                </Form.Group>
                
                {formData.isJoinQuery ? (
                  <Form.Group className="mb-3">
                    <Form.Label>Custom JOIN Query</Form.Label>
                    <Form.Control 
                      as="textarea" 
                      rows={4} 
                      name="customQuery" 
                      value={formData.customQuery} 
                      onChange={handleInputChange}
                      placeholder="SELECT * FROM table1 JOIN table2 ON table1.id = table2.id"
                    />
                    <Form.Text className="text-muted">
                      Write a custom SQL query with JOIN operations
                    </Form.Text>
                  </Form.Group>
                ) : (
                  <>
                    <Form.Group className="mb-3">
                      <Form.Label>Select Table</Form.Label>
                      <Form.Select 
                        value={selectedTable} 
                        onChange={handleTableSelect}
                      >
                        <option value="">-- Select a table --</option>
                        {tables.map((table, index) => (
                          <option key={index} value={table.name}>
                            {table.name}
                          </option>
                        ))}
                      </Form.Select>
                    </Form.Group>
                    
                    {columns.length > 0 && (
                      <Form.Group className="mb-3">
                        <Form.Label>Select Columns</Form.Label>
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
                          {columns.map((column, index) => (
                            <Form.Check 
                              key={index}
                              type="checkbox" 
                              label={`${column.name} (${column.type})`}
                              checked={selectedColumns.includes(column.name)}
                              onChange={() => handleColumnSelect(column.name)}
                              className="mb-1"
                            />
                          ))}
                        </div>
                      </Form.Group>
                    )}
                  </>
                )}
                
                <div className="row">
                  <div className="col-md-6">
                    <Form.Group className="mb-3">
                      <Form.Label>Output File Name</Form.Label>
                      <Form.Control 
                        type="text" 
                        name="fileName" 
                        value={formData.fileName} 
                        onChange={handleInputChange}
                        placeholder="e.g., export_data.csv"
                      />
                    </Form.Group>
                  </div>
                  <div className="col-md-6">
                    <Form.Group className="mb-3">
                      <Form.Label>Delimiter</Form.Label>
                      <Form.Select 
                        name="delimiter" 
                        value={formData.delimiter} 
                        onChange={handleInputChange}
                      >
                        <option value=",">Comma (,)</option>
                        <option value="\t">Tab</option>
                        <option value=";">Semicolon (;)</option>
                        <option value="|">Pipe (|)</option>
                      </Form.Select>
                    </Form.Group>
                  </div>
                </div>
                
                <Button 
                  variant="secondary" 
                  onClick={handlePreview} 
                  disabled={isLoading || (!selectedTable && !formData.isJoinQuery)}
                  className="me-2"
                >
                  {isLoading ? <><Spinner animation="border" size="sm" /> Previewing...</> : 'Preview Data'}
                </Button>
                
                <Button 
                  variant="success" 
                  onClick={handleExport} 
                  disabled={isLoading || (!selectedTable && !formData.isJoinQuery) || !formData.fileName}
                >
                  {isLoading ? <><Spinner animation="border" size="sm" /> Exporting...</> : 'Export to File'}
                </Button>
              </Form>
            </Card.Body>
          </Card>
          
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
          
          {/* Export Result */}
          {exportResult && (
            <Card className="mb-4">
              <Card.Header>Export Result</Card.Header>
              <Card.Body>
                <div className="mb-3">
                  <Badge bg="success">Success</Badge>
                </div>
                <ListGroup>
                  <ListGroup.Item>
                    <strong>Records Exported:</strong> {exportResult.count}
                  </ListGroup.Item>
                  <ListGroup.Item>
                    <strong>File Name:</strong> {exportResult.fileName}
                  </ListGroup.Item>
                  <ListGroup.Item>
                    <strong>File Path:</strong> {exportResult.filePath}
                  </ListGroup.Item>
                </ListGroup>
              </Card.Body>
            </Card>
          )}
        </>
      )}
    </div>
  );
};

export default ClickHouseToFile; 