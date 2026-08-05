const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const cookieParser = require('cookie-parser');
const rateLimit = require('express-rate-limit');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const JWT_SECRET = process.env.JWT_SECRET || 'pragyalink_secure_key_2026_ashanti';
const PORT = process.env.PORT || 3000;

// -----------------------------------------------------------
// MIDDLEWARE CONFIGURATION
// -----------------------------------------------------------
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

// Rate limiter to prevent brute force login attempts
const authLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100, // limit each IP to 100 requests per windowMs
    message: { success: false, error: 'Too many requests. Please try again later.' }
});

app.use('/api/login', authLimiter);
app.use('/api/signup', authLimiter);

// -----------------------------------------------------------
// SQLITE DATABASE INITIALIZATION
// -----------------------------------------------------------
const db = new sqlite3.Database('./pragyalink.db', (err) => {
    if (err) {
        console.error('❌ Database connection error:', err.message);
    } else {
        console.log('✅ Connected to SQLite database (pragyalink.db)');
    }
});

db.serialize(() => {
    db.run(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fullName TEXT NOT NULL,
            contact TEXT NOT NULL,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('passenger', 'rider', 'mechanic')),
            createdAt DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `);
});

// -----------------------------------------------------------
// AUTHENTICATION MIDDLEWARE
// -----------------------------------------------------------
function authenticateToken(req, res, next) {
    const token = req.cookies.token;
    if (!token) return res.status(401).json({ success: false, error: 'Access denied. Please log in.' });

    jwt.verify(token, JWT_SECRET, (err, user) => {
        if (err) return res.status(403).json({ success: false, error: 'Session expired or invalid token.' });
        req.user = user;
        next();
    });
}

// -----------------------------------------------------------
// AUTHENTICATION API ROUTES
// -----------------------------------------------------------

// POST /api/signup
app.post('/api/signup', async (req, res) => {
    const { fullName, contact, username, password, role } = req.body;

    if (!fullName || !contact || !username || !password || !role) {
        return res.status(400).json({ success: false, error: 'All fields are required.' });
    }

    try {
        const hashedPassword = await bcrypt.hash(password, 10);
        const query = `INSERT INTO users (fullName, contact, username, password, role) VALUES (?, ?, ?, ?, ?)`;

        db.run(query, [fullName, contact, username, hashedPassword, role], function(err) {
            if (err) {
                if (err.message.includes('UNIQUE constraint failed')) {
                    return res.status(400).json({ success: false, error: 'Username already taken.' });
                }
                return res.status(500).json({ success: false, error: 'Failed to create user account.' });
            }

            const token = jwt.sign({ id: this.lastID, username, role }, JWT_SECRET, { expiresIn: '24h' });
            res.cookie('token', token, { httpOnly: true, secure: false, maxAge: 24 * 60 * 60 * 1000 });
            
            res.json({ success: true, user: { id: this.lastID, username, role } });
        });
    } catch (err) {
        res.status(500).json({ success: false, error: 'Server error during registration.' });
    }
});

// POST /api/login
app.post('/api/login', (req, res) => {
    const { username, password } = req.body;

    if (!username || !password) {
        return res.status(400).json({ success: false, error: 'Username and password required.' });
    }

    const query = `SELECT * FROM users WHERE username = ?`;
    db.get(query, [username], async (err, user) => {
        if (err || !user) {
            return res.status(400).json({ success: false, error: 'Invalid username or password.' });
        }

        const validPassword = await bcrypt.compare(password, user.password);
        if (!validPassword) {
            return res.status(400).json({ success: false, error: 'Invalid username or password.' });
        }

        const token = jwt.sign({ id: user.id, username: user.username, role: user.role }, JWT_SECRET, { expiresIn: '24h' });
        res.cookie('token', token, { httpOnly: true, secure: false, maxAge: 24 * 60 * 60 * 1000 });

        res.json({ success: true, user: { id: user.id, username: user.username, role: user.role } });
    });
});

// GET /api/me (Check active session)
app.get('/api/me', authenticateToken, (req, res) => {
    res.json({ success: true, user: req.user });
});

// POST /api/logout
app.post('/api/logout', (req, res) => {
    res.clearCookie('token');
    res.json({ success: true, message: 'Logged out successfully.' });
});

// -----------------------------------------------------------
// HTML VIEW ROUTES
// -----------------------------------------------------------
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'views/index.html')));
app.get('/passenger', (req, res) => res.sendFile(path.join(__dirname, 'views/passenger.html')));
app.get('/rider', (req, res) => res.sendFile(path.join(__dirname, 'views/rider.html')));
app.get('/mechanic', (req, res) => res.sendFile(path.join(__dirname, 'views/mechanic.html')));

// -----------------------------------------------------------
// REAL-TIME SOCKET.IO GPS BROADCASTING ENGINE
// -----------------------------------------------------------
const activeDrivers = {};

io.on('connection', (socket) => {
    console.log(`🔌 Client connected: ${socket.id}`);

    // Send currently active drivers to a newly connected passenger
    socket.emit('initial-drivers', Object.values(activeDrivers));

    // Listen for live driver GPS coordinates
    socket.on('update-location', (driverData) => {
        activeDrivers[socket.id] = {
            id: socket.id,
            name: driverData.name || 'Pragya Rider',
            vehicle: driverData.vehicle || 'Bajaj RE',
            phone: driverData.phone || '0240000000',
            lat: driverData.lat,
            lng: driverData.lng,
            updatedAt: Date.now()
        };

        // Broadcast driver position to all connected clients
        io.emit('driver-moved', activeDrivers[socket.id]);
    });

    // Handle rider disconnect / offline
    socket.on('disconnect', () => {
        if (activeDrivers[socket.id]) {
            delete activeDrivers[socket.id];
            io.emit('driver-offline', socket.id);
            console.log(`❌ Driver went offline: ${socket.id}`);
        }
    });
});

// -----------------------------------------------------------
// START HTTP & WEBSOCKET SERVER
// -----------------------------------------------------------
server.listen(PORT, () => {
    console.log(`🚀 PragyaLink Server running on http://localhost:${PORT}`);
});
