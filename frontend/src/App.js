import React, { useState } from 'react';
import Header from './components/Header';
import Navbar from './components/Navbar';
import Content from './components/Content';
import './App.css';

function App() {
  const [page, setPage] = useState('Dashboard');

  return (
    <div className="App">
      <Header title={page} />
      <div className="main-container">
        <Navbar setPage={setPage} />
        <Content page={page} />
      </div>
    </div>
  );
}

export default App;

// import React, { useEffect, useState } from "react";
// import axios from "axios";
// import {
//   LineChart,
//   Line,
//   XAxis,
//   YAxis,
//   Tooltip,
//   CartesianGrid,
//   ResponsiveContainer,
// } from "recharts";
// import "./App.css";

// function App() {
//   const [overview, setOverview] = useState({});
//   const [chartData, setChartData] = useState([]);
//   const [logs, setLogs] = useState([]);

//   // Gọi API (tạm thời mô phỏng dữ liệu nếu chưa có backend)
//   useEffect(() => {
//     // Giả lập API overview
//     setOverview({
//       tickers: 12,
//       success: 3450,
//       failed: 17,
//     });

//     // Giả lập dữ liệu biểu đồ
//     setChartData([
//       { date: "2025-11-01", price: 100 },
//       { date: "2025-11-02", price: 102 },
//       { date: "2025-11-03", price: 101 },
//       { date: "2025-11-04", price: 104 },
//       { date: "2025-11-05", price: 106 },
//     ]);

//     // Giả lập logs
//     setLogs([
//       { id: 1, step: "Extract", status: "Success", time: "2025-11-05 12:30" },
//       { id: 2, step: "Load Staging", status: "Failed", time: "2025-11-05 12:35" },
//       { id: 3, step: "Load DW", status: "Success", time: "2025-11-05 12:40" },
//     ]);
//   }, []);

//   return (
//     <div className="dashboard-container">
//       <h1>📊 Data Warehouse Dashboard</h1>

//       {/* KPI Cards */}
//       <div className="kpi-container">
//         <div className="kpi-card blue">
//           <h2>{overview.tickers}</h2>
//           <p>Tổng số Ticker</p>
//         </div>
//         <div className="kpi-card green">
//           <h2>{overview.success}</h2>
//           <p>Bản ghi thành công</p>
//         </div>
//         <div className="kpi-card red">
//           <h2>{overview.failed}</h2>
//           <p>Bản ghi lỗi</p>
//         </div>
//       </div>

//       {/* Chart */}
//       <div className="chart-container">
//         <h3>Biểu đồ giá cổ phiếu theo ngày</h3>
//         <ResponsiveContainer width="100%" height={300}>
//           <LineChart data={chartData}>
//             <CartesianGrid strokeDasharray="3 3" />
//             <XAxis dataKey="date" />
//             <YAxis />
//             <Tooltip />
//             <Line type="monotone" dataKey="price" stroke="#007BFF" />
//           </LineChart>
//         </ResponsiveContainer>
//       </div>

//       {/* Logs */}
//       <div className="logs-container">
//         <h3>ETL Logs</h3>
//         <table>
//           <thead>
//             <tr>
//               <th>ID</th>
//               <th>Bước</th>
//               <th>Trạng thái</th>
//               <th>Thời gian</th>
//             </tr>
//           </thead>
//           <tbody>
//             {logs.map((log) => (
//               <tr key={log.id} className={log.status === "Failed" ? "failed" : "success"}>
//                 <td>{log.id}</td>
//                 <td>{log.step}</td>
//                 <td>{log.status}</td>
//                 <td>{log.time}</td>
//               </tr>
//             ))}
//           </tbody>
//         </table>
//       </div>
//     </div>
//   );
// }

// export default App;
