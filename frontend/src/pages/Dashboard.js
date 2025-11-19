import React, { useState, useEffect } from "react";
import { Line } from "react-chartjs-2";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import { addDays, subDays } from "date-fns";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from "chart.js";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

// const Dashboard = () => {
//   const [startDate, setStartDate] = useState(subDays(new Date(), 7));
//   const [endDate, setEndDate] = useState(new Date());
//   const [chartData, setChartData] = useState({});

//   useEffect(() => {
//     const fetchData = async () => {
//       const start = startDate.toISOString().split('T')[0];
//       const end = endDate.toISOString().split('T')[0];

//       const res = await fetch(`http://localhost:8000/stocks?start=${start}&end=${end}`);
//       const data = await res.json();

//       setChartData({
//         labels: data.labels,
//         datasets: [
//           {
//             label: "Stock Close Price",
//             data: data.data,
//             borderColor: "rgba(75,192,192,1)",
//             backgroundColor: "rgba(75,192,192,0.2)",
//           }
//         ]
//       });
//     };

//     fetchData();
//   }, [startDate, endDate]);

//   console.log("Before Return");

//   return (
//     <div>
//       <div style={{ marginBottom: "20px" }}>
//         <label>Start Date: </label>
//         <DatePicker
//           selected={startDate}
//           onChange={(date) => setStartDate(date)}
//           maxDate={endDate}
//         />
//         <label style={{ marginLeft: "10px" }}>End Date: </label>
//         <DatePicker
//           selected={endDate}
//           onChange={(date) => setEndDate(date)}
//           minDate={startDate}
//         />
//       </div>
//       {/* <Line data={chartData} /> */}
//       {chartData.labels && chartData.datasets ? (
//         <Line data={chartData} />
//       ) : (
//         <p>Loading...</p>
//       )}
//     </div>
//   );
// };

// const Dashboard = ({ stockSk }) => {
//   const [startDate, setStartDate] = useState(subDays(new Date(), 30));
//   const [endDate, setEndDate] = useState(new Date());
//   const [chartData, setChartData] = useState({});

//   useEffect(() => {
//     async function fetchData() {
//       try {
//         const query = new URLSearchParams({
//           // stock_sk: stockSk,
//           ...(stockSk ? { stock_sk: stockSk } : {}),
//           start_date: startDate.toISOString().split("T")[0],
//           end_date: endDate.toISOString().split("T")[0],
//         });
//         const res = await fetch(`http://localhost:8000/stocks-indicators?${query}`);
//         if (!res.ok) throw new Error("Network response was not ok");
//         const json = await res.json();

//         const labels = json.data.map(item => item.full_date);
//         const closes = json.data.map(item => item.close);

//         setChartData({
//           labels,
//           datasets: [
//             {
//               label: json.data[0]?.ticker || "Stock",
//               data: closes,
//               borderColor: "rgba(75,192,192,1)",
//               backgroundColor: "rgba(75,192,192,0.2)",
//             },
//           ],
//         });
//       } catch (err) {
//         console.error(err);
//       }
//     }

//     fetchData();
//   }, [stockSk, startDate, endDate]);

//   return (
//     <div>
//       <div style={{ marginBottom: "20px" }}>
//         <label>Start Date: </label>
//         <DatePicker
//           selected={startDate}
//           onChange={(date) => setStartDate(date)}
//           maxDate={endDate}
//         />
//         <label style={{ marginLeft: "10px" }}>End Date: </label>
//         <DatePicker
//           selected={endDate}
//           onChange={(date) => setEndDate(date)}
//           minDate={startDate}
//         />
//       </div>
//       {chartData.labels ? (
//         <Line data={chartData} />
//       ) : (
//         <p>Loading chart...</p>
//       )}
//     </div>
//   );
// };

const Dashboard = () => {
  const [tickers, setTickers] = useState([]);
  const [selectedTicker, setSelectedTicker] = useState(null);
  const [startDate, setStartDate] = useState(
    new Date(new Date().setDate(new Date().getDate() - 7))
  );
  const [endDate, setEndDate] = useState(new Date());
  const [chartData, setChartData] = useState({ labels: [], datasets: [] });

  // Load ticker list
  useEffect(() => {
    fetch("http://localhost:8000/stocks") // API trả về danh sách stock
      .then((res) => res.json())
      .then((data) => {
        setTickers(data.data);
        if (data.data.length > 0) setSelectedTicker(data.data[0].stock_sk);
      })
      .catch((err) => console.error("Failed to load tickers", err));
  }, []);

  // Fetch chart data khi ticker hoặc date thay đổi
  useEffect(() => {
    if (!selectedTicker) return;

    const startStr = startDate.toISOString().split("T")[0];
    const endStr = endDate.toISOString().split("T")[0];

    console.log(selectedTicker + ";" + startStr + ";" + endDate);
    fetch(
      `http://localhost:8000/stocks-indicators?stock_sk=${selectedTicker}&start=${startStr}&end=${endStr}`
    )
      .then((res) => {
        if (!res.ok) throw new Error("Network response was not ok");
        return res.json();
      })
      .then((data) => {
        const labels = data.data.map((d) => d.full_date); // full_date từ dim_date
        const closes = data.data.map((d) => d.close);

        // Hạn chế tick label: show max 10 label
        const step = Math.ceil(labels.length / 10);

        setChartData({
          labels,
          datasets: [
            {
              label: "Close Price",
              data: closes,
              borderColor: "rgba(75,192,192,1)",
              backgroundColor: "rgba(75,192,192,0.2)",
            },
          ],
        });

        // Optionally: truncate labels for better X axis readability
        setChartData((prev) => ({
          ...prev,
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: {
                ticks: {
                  autoSkip: true,
                  maxTicksLimit: 10,
                },
              },
              y: {
                beginAtZero: false,
              },
            },
          },
        }));
      })
      .catch((err) => console.error(err));
  }, [selectedTicker, startDate, endDate]);

  return (
    // <div style={{ padding: '20px' }}>
    //   <div style={{ marginBottom: '20px', display: 'flex', alignItems: 'center', gap: '10px' }}>
    //     <label>Ticker:</label>
    //     <select value={selectedTicker || ''} onChange={e => setSelectedTicker(e.target.value)}>
    //       {tickers.map(t => (
    //         <option key={t.stock_sk} value={t.stock_sk}>
    //           {t.ticker}
    //         </option>
    //       ))}
    //     </select>

    //     <label>Start Date:</label>
    //     <DatePicker
    //       selected={startDate}
    //       onChange={date => setStartDate(date)}
    //       maxDate={endDate}
    //     />

    //     <label>End Date:</label>
    //     <DatePicker
    //       selected={endDate}
    //       onChange={date => setEndDate(date)}
    //       minDate={startDate}
    //     />
    //   </div>

    //   <div style={{ height: '400px', overflowY: 'auto' }}>
    //     {chartData?.datasets?.length > 0 ? (
    //         // <Line data={chartData} />
    //          <Line data={chartData} options={chartData.options || { responsive: true, maintainAspectRatio: false }} />
    //     ) : (
    //         <p>Loading chart...</p>
    //     )}
    //   </div>
    // </div>
    <div className="chart-container">
      <div className="chart-controls">
        <label>Ticker:</label>
        <select
          value={selectedTicker || ""}
          onChange={(e) => setSelectedTicker(e.target.value)}
          className="chart-select"
        >
          {tickers.map((t) => (
            <option key={t.stock_sk} value={t.stock_sk}>
              {t.ticker}
            </option>
          ))}
        </select>

        <label>Start Date:</label>
        <DatePicker
          selected={startDate}
          onChange={(date) => setStartDate(date)}
          maxDate={endDate}
          className="chart-date-picker"
        />

        <label>End Date:</label>
        <DatePicker
          selected={endDate}
          onChange={(date) => setEndDate(date)}
          minDate={startDate}
          className="chart-date-picker"
        />
      </div>

      <div className="chart-wrapper">
        {chartData?.datasets?.length > 0 ? (
          <Line
            data={chartData}
            options={{
              // chartData.options || {
              //   responsive: true,
              //   maintainAspectRatio: false,
              // }
              responsive: false, // tắt responsive để dùng width cố định
              maintainAspectRatio: false,
              scales: {
                x: {
                  ticks: {
                    autoSkip: true,
                    maxTicksLimit: 10, 
                    maxRotation: 0,
                    minRotation: 0,
                  },
                },
              },
            }}
            width={chartData?.labels?.length * 10} // mỗi label 50px, tự động kéo dài chart
            height={400}    
          />
        ) : (
          <p>Loading chart...</p>
        )}
      </div>
    </div>
  );
};

export default Dashboard;
