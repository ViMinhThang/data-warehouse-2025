import React from 'react';
import Dashboard from '../pages/Dashboard';
import Log from '../pages/Log';
import './Content.css';

const Content = ({ page }) => {
    return (
        <main className="content">
            {page === 'Dashboard' && <Dashboard />}
            {page === 'Log' && <Log />}
        </main>
    );
};

export default Content;
