import React from 'react';
import './Navbar.css';

const Navbar = ({ setPage }) => {
    return (
        <nav className="navbar">
            <ul>
                <li><button onClick={() => setPage('Dashboard')}>Dashboard</button></li>
                <li><button onClick={() => setPage('Log')}>Log</button></li>
            </ul>
        </nav>
    );
};

export default Navbar;
