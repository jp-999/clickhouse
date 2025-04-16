const express = require('express');
const router = express.Router();
const multer = require('multer');
const path = require('path');
const fileController = require('../controllers/fileController');

// Configure multer for file uploads
const storage = multer.diskStorage({
  destination: function(req, file, cb) {
    cb(null, path.join(__dirname, '..', 'uploads'));
  },
  filename: function(req, file, cb) {
    // Add timestamp to filename to avoid duplicates
    const timestamp = Date.now();
    cb(null, `${timestamp}_${file.originalname}`);
  }
});

// Create the multer instance
const upload = multer({ 
  storage: storage,
  limits: { fileSize: 10 * 1024 * 1024 },  // 10MB file size limit
  fileFilter: function(req, file, cb) {
    // Accept only CSV/TSV files
    const filetypes = /csv|tsv|txt/;
    const extname = filetypes.test(path.extname(file.originalname).toLowerCase());
    const mimetype = filetypes.test(file.mimetype);
    
    if (mimetype && extname) {
      return cb(null, true);
    } else {
      cb(new Error('Only CSV, TSV, and TXT files are allowed'));
    }
  }
});

// Get list of uploaded files
router.get('/uploaded-files', fileController.getUploadedFiles);

// Upload a file
router.post('/upload', upload.single('file'), fileController.uploadFile);

// Get file schema (detect headers and types)
router.post('/get-schema', fileController.getFileSchema);

// Preview file data
router.post('/preview-data', fileController.previewFileData);

// Import file to ClickHouse
router.post('/import-to-clickhouse', fileController.importToClickHouse);

module.exports = router; 