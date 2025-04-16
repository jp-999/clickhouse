const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs-extra');
const dotenv = require('dotenv');

// Load environment variables
dotenv.config();

// Routes
const clickhouseRoutes = require('./routes/clickhouse');
const fileRoutes = require('./routes/file');

// Initialize Express
const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Create uploads directory if it doesn't exist
const uploadsDir = path.join(__dirname, 'uploads');
fs.ensureDirSync(uploadsDir);

// Routes
app.use('/api/clickhouse', clickhouseRoutes);
app.use('/api/file', fileRoutes);

// Root route
app.get('/', (req, res) => {
  res.send('ClickHouse & Flat File Data Ingestion API');
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
}); 