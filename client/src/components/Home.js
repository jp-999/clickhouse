import React from 'react';
import { Card, Button, Row, Col } from 'react-bootstrap';
import { Link } from 'react-router-dom';

const Home = () => {
  return (
    <div>
      <div className="text-center mb-4">
        <h1>ClickHouse & Flat File Data Ingestion Tool</h1>
        <p className="lead">
          A bidirectional data connector between ClickHouse and flat files
        </p>
      </div>

      <Row className="justify-content-center">
        <Col md={5}>
          <Card className="mb-4">
            <Card.Header as="h5">ClickHouse → Flat File</Card.Header>
            <Card.Body>
              <Card.Text>
                Export data from ClickHouse to a flat file (CSV/TSV).
                <ul className="mt-3">
                  <li>Connect to ClickHouse with JWT authentication</li>
                  <li>Select tables and columns</li>
                  <li>Export selected data to CSV/TSV</li>
                  <li>Support for JOIN operations</li>
                </ul>
              </Card.Text>
              <Link to="/clickhouse-to-file">
                <Button variant="primary">Export from ClickHouse</Button>
              </Link>
            </Card.Body>
          </Card>
        </Col>
        
        <Col md={5}>
          <Card className="mb-4">
            <Card.Header as="h5">Flat File → ClickHouse</Card.Header>
            <Card.Body>
              <Card.Text>
                Import data from a flat file (CSV/TSV) to ClickHouse.
                <ul className="mt-3">
                  <li>Upload flat files</li>
                  <li>Auto-detect schema</li>
                  <li>Select columns to import</li>
                  <li>Create new tables or import to existing ones</li>
                </ul>
              </Card.Text>
              <Link to="/file-to-clickhouse">
                <Button variant="success">Import to ClickHouse</Button>
              </Link>
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default Home; 