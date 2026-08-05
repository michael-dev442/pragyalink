const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cookieParser = require('cookie-parser');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'pragyalink_super_secret_key_2026';

// Middleware
app.use(express.json());
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

// Database Setup
const db = new sqlite3.Database('./pragyalink.db', (err) => {
    if (err) console.error('Database connection error:', err);
    else console.log('✅ Connected to SQLite database (pragyalink.db)');
});

// Initialize Tables & Schema Migration
db.serialize(() => {
    db.run(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fullName TEXT,
            contact TEXT,
            username TEXT UNIQUE,
            password TEXT,
            role TEXT,
            vehicleType TEXT,
            vehicleColor TEXT,
            plateNumber TEXT
        )
    `);

    // Ensure missing columns exist if upgrading existing DB
    const addCol = (col, type) => {
        db.run(`ALTER TABLE users ADD COLUMN ${col} ${type}`, () => {});
    };
    addCol('fullName', 'TEXT');
    addCol('contact', 'TEXT');
    addCol('vehicleType', 'TEXT');
    addCol('vehicleColor', 'TEXT');
    addCol('plateNumber', 'TEXT');
});

// Authentication Middleware
const authenticateToken = (req, res, next) => {
    const token = req.cookies.token;
    if (!token) return res.status(401).json({ success: false, error: 'Unauthorized' });

    jwt.verify(token, JWT_SECRET, (err, user) => {
        if (err) return res.status(403).json({ success: false, error: 'Invalid Session' });
        req.user = user;
        next();
    });
};

// Routes - Serve HTML Views
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'views/index.html')));
app.get('/passenger', (req, res) => res.sendFile(path.join(__dirname, 'views/passenger.html')));
app.get('/rider', (req, res) => res.sendFile(path.join(__dirname, 'views/rider.html')));
app.get('/mechanic', (req, res) => res.sendFile(path.join(__dirname, 'views/mechanic.html')));

// API Routes
app.post('/api/signup', async (req, res) => {
    const { fullName, contact, username, password, role, vehicleType, vehicleColor, plateNumber } = req.body;
    try {
        const hashedPassword = await bcrypt.hash(password, 10);
        db.run(
            `INSERT INTO users (fullName, contact, username, password, role, vehicleType, vehicleColor, plateNumber) 
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [fullName, contact, username, hashedPassword, role, vehicleType || 'Bajaj RE', vehicleColor || 'Yellow', plateNumber || 'N/A'],
            function (err) {
                if (err) return res.status(400).json({ success: false, error: 'Username already exists' });
                
                const token = jwt.sign({ id: this.lastID, username, role }, JWT_SECRET, { expiresIn: '24h' });
                res.cookie('token', token, { httpOnly: true });
                res.json({ success: true, user: { username, role } });
            }
        );
    } catch (e) {
        res.status(500).json({ success: false, error: 'Registration failed' });
    }
});

app.post('/api/login', (req, res) => {
    const { username, password } = req.body;
    db.get(`SELECT * FROM users WHERE username = ?`, [username], async (err, user) => {
        if (err || !user) return res.status(400).json({ success: false, error: 'User not found' });

        const valid = await bcrypt.compare(password, user.password);
        if (!valid) return res.status(400).json({ success: false, error: 'Invalid credentials' });

        const token = jwt.sign({ 
            id: user.id, 
            username: user.username, 
            role: user.role,
            fullName: user.fullName,
            contact: user.contact,
            vehicleType: user.vehicleType,
            vehicleColor: user.vehicleColor,
            plateNumber: user.plateNumber
        }, JWT_SECRET, { expiresIn: '24h' });

        res.cookie('token', token, { httpOnly: true });
        res.json({ success: true, user });
    });
});

app.get('/api/me', authenticateToken, (req, res) => {
    db.get(`SELECT id, fullName, contact, username, role, vehicleType, vehicleColor, plateNumber FROM users WHERE id = ?`, [req.user.id], (err, user) => {
        if (err || !user) return res.status(404).json({ success: false });
        res.json({ success: true, user });
    });
});

app.post('/api/logout', (req, res) => {
    res.clearCookie('token');
    res.json({ success: true });
});

// Socket.io Real-Time Tracking & Client Info Stream
let activeRiders = {};

io.on('connection', (socket) => {
    // Send existing active riders to newly connected passenger
    socket.emit('current-riders', Object.values(activeRiders));

    socket.on('update-location', (data) => {
        // Fetch full profile info for rider broadcasting location
        db.get(`SELECT fullName, contact, vehicleType, vehicleColor, plateNumber FROM users WHERE username = ?`, [data.name], (err, user) => {
            const riderPayload = {
                socketId: socket.id,
                username: data.name,
                fullName: user ? user.fullName : data.name,
                contact: user ? user.contact : 'N/A',
                vehicleType: user ? user.vehicleType : 'Bajaj RE',
                vehicleColor: user ? user.vehicleColor : 'Yellow',
                plateNumber: user ? user.plateNumber : 'N/A',
                lat: data.lat,
                lng: data.lng
            };

            activeRiders[socket.id] = riderPayload;
            io.emit('rider-location-updated', riderPayload);
        });
    });

    socket.on('disconnect', () => {
        if (activeRiders[socket.id]) {
            delete activeRiders[socket.id];
            io.emit('rider-disconnected', socket.id);
        }
    });
});

server.listen(PORT, () => {
    console.log(`🚀 PragyaLink Server running on port ${PORT}`);
});
