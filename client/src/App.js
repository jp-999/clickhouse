import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import 'bootstrap/dist/css/bootstrap.min.css';
import './App.css';

// Components
import Header from './components/Header';
import Home from './components/Home';
import ClickHouseToFile from './components/ClickHouseToFile';
import FileToClickHouse from './components/FileToClickHouse';

function App() {
  return (
    <Router>
      <div className="App">
        <Header />
        <div className="container mt-4">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/clickhouse-to-file" element={<ClickHouseToFile />} />
            <Route path="/file-to-clickhouse" element={<FileToClickHouse />} />
          </Routes>
        </div>
      </div>
    </Router>
  );
}

export default App;
