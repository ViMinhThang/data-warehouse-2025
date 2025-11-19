import React, { useState, useEffect } from 'react';
import DatePicker from 'react-datepicker';
import 'react-datepicker/dist/react-datepicker.css';

const Log = () => {
    const [selectedDate, setSelectedDate] = useState(new Date());
    const [logs, setLogs] = useState([]);

    useEffect(() => {
        // TODO: Gọi API backend lấy log theo selectedDate
        // demo data
        setLogs([
            { id: 1, type: 'error', message: 'Database connection failed' },
            { id: 2, type: 'correct', message: 'Stock data loaded successfully' },
            { id: 3, type: 'error', message: 'Failed to parse CSV' },
        ]);
    }, [selectedDate]);

    return (
        <div>
            <div style={{ marginBottom: '20px' }}>
                <label>Chọn ngày xem log: </label>
                <DatePicker 
                    selected={selectedDate} 
                    onChange={date => setSelectedDate(date)} 
                />
            </div>
            <div>
                {logs.map(log => (
                    <div key={log.id} style={{
                        padding: '10px',
                        marginBottom: '5px',
                        color: 'white',
                        backgroundColor: log.type === 'error' ? '#e74c3c' : '#2ecc71',
                        borderRadius: '4px'
                    }}>
                        {log.message}
                    </div>
                ))}
            </div>
        </div>
    );
};

export default Log;
