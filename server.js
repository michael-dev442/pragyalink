const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cookieParser = require('cookie-parser');
const rateLimit = require('express-rate-limit');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'pragyalink_super_secret_key_2026';

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());
app.use(express.static('public'));

// 1. RATE LIMITING: Protect against brute-force attacks
const authLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 10, // Limit each IP to 10 requests per window
    message: { success: false, error: 'Too many login attempts. Please try again in 15 minutes.' }
});

// Database Initialization
const db = new sqlite3.Database('./database.db', (err) => {
    if (err) console.error('Database connection error:', err);
    else console.log('🔒 Secure SQLite Database Connected');
});

// Ensure Users Table Exists
db.run(`
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fullName TEXT,
        contact TEXT,
        username TEXT UNIQUE,
        password TEXT,
        role TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
`);

// SECURITY MIDDLEWARE: Verify JWT Token
function authenticateToken(req, res, next) {
    const token = req.cookies.authToken;
    if (!token) return res.status(401).json({ success: false, error: 'Access Denied. Please log in.' });

    jwt.verify(token, JWT_SECRET, (err, user) => {
        if (err) return res.status(403).json({ success: false, error: 'Invalid or expired session.' });
        req.user = user;
        next();
    });
}

// -----------------------------------------------------------
// AUTHENTICATION API ROUTES
// -----------------------------------------------------------

// SECURE SIGNUP ROUTE
app.post('/api/signup', async (req, res) => {
    const { fullName, contact, username, password, role } = req.body;

    if (!username || !password || !role) {
        return res.status(400).json({ success: false, error: 'All fields are required.' });
    }

    try {
        // Hash password with salt rounds (10)
        const hashedPassword = await bcrypt.hash(password, 10);

        const sql = `INSERT INTO users (fullName, contact, username, password, role) VALUES (?, ?, ?, ?, ?)`;
        db.run(sql, [fullName, contact, username, hashedPassword, role], function (err) {
            if (err) {
                if (err.message.includes('UNIQUE')) {
                    return res.status(400).json({ success: false, error: 'Username already taken.' });
                }
                return res.status(500).json({ success: false, error: 'Database error.' });
            }

            // Create JWT Token
            const token = jwt.sign({ id: this.lastID, username, role }, JWT_SECRET, { expiresIn: '24h' });

            // Set Secure HTTP-Only Cookie
            res.cookie('authToken', token, {
                httpOnly: true, // Prevents XSS script access
                secure: false,  // Set to true in HTTPS production
                maxAge: 24 * 60 * 60 * 1000
            });

            res.json({ success: true, user: { id: this.lastID, username, role } });
        });
    } catch (error) {
        res.status(500).json({ success: false, error: 'Server encryption error.' });
    }
});

// SECURE LOGIN ROUTE (Rate Limited)
app.post('/api/login', authLimiter, (req, res) => {
    const { username, password } = req.body;

    const sql = `SELECT * FROM users WHERE username = ?`;
    db.get(sql, [username], async (err, user) => {
        if (err || !user) {
            return res.status(401).json({ success: false, error: 'Invalid username or password.' });
        }

        // Compare hashed password
        const validPassword = await bcrypt.compare(password, user.password);
        if (!validPassword) {
            return res.status(401).json({ success: false, error: 'Invalid username or password.' });
        }

        // Create JWT Token
        const token = jwt.sign({ id: user.id, username: user.username, role: user.role }, JWT_SECRET, { expiresIn: '24h' });

        // Set Secure Cookie
        res.cookie('authToken', token, {
            httpOnly: true,
            secure: false,
            maxAge: 24 * 60 * 60 * 1000
        });

        res.json({ success: true, user: { id: user.id, username: user.username, role: user.role } });
    });
});

// SESSION CHECK ROUTE
app.get('/api/me', authenticateToken, (req, res) => {
    res.json({ success: true, user: req.user });
});

// LOGOUT ROUTE
app.post('/api/logout', (req, res) => {
    res.clearCookie('authToken');
    res.json({ success: true, message: 'Logged out securely.' });
});

// -----------------------------------------------------------
// VIEW ROUTES
// -----------------------------------------------------------
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'views', 'index.html')));
app.get('/passenger', (req, res) => res.sendFile(path.join(__dirname, 'views', 'passenger.html')));
app.get('/rider', (req, res) => res.sendFile(path.join(__dirname, 'views', 'rider.html')));
app.get('/mechanic', (req, res) => res.sendFile(path.join(__dirname, 'views', 'mechanic.html')));

app.listen(PORT, () => {
    console.log(`🚀 Secure PragyaLink Server running on http://localhost:${PORT}`);
});
