const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// Persistent JSON File Storage Setup
const DB_FILE = path.join(__dirname, 'database.json');

// Initialize DB if it doesn't exist
function loadDatabase() {
    if (!fs.existsSync(DB_FILE)) {
        const initialData = { servicePassports: {}, rideHistory: [], registeredFleet: {} };
        fs.writeFileSync(DB_FILE, JSON.stringify(initialData, null, 2));
        return initialData;
    }
    try {
        const data = fs.readFileSync(DB_FILE, 'utf8');
        return JSON.parse(data);
    } catch (e) {
        console.error("Error reading database.json, re-initializing...", e);
        return { servicePassports: {}, rideHistory: [], registeredFleet: {} };
    }
}

function saveDatabase(data) {
    fs.writeFileSync(DB_FILE, JSON.stringify(data, null, 2));
}

let db = loadDatabase();

// Temporary Runtime State (re-hydrated from database.json)
const activeRiders = {};     
const activeWorkshops = {};  
const fleetOwnerSockets = {}; 

function calculateDistanceKm(lat1, lon1, lat2, lon2) {
    const R = 6371; 
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLon / 2) * Math.sin(dLon / 2);
    return R * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
}

// Filtered Fleet Emission to Authorized Owners
function emitFleetToOwner(ownerSocketId) {
    const fleetCode = fleetOwnerSockets[ownerSocketId];
    if (!fleetCode) return;

    // Get live drivers or registered offline vehicles tied to this fleet key
    const liveFleet = Object.values(activeRiders).filter(
        rider => rider.ownerCode && rider.ownerCode.trim() === fleetCode.trim()
    );

    const savedFleetVehicles = db.registeredFleet[fleetCode] || [];

    io.to(ownerSocketId).emit('current-riders', {
        activeDrivers: liveFleet,
        registeredVehicles: savedFleetVehicles
    });
}

function broadcastFleetUpdates() {
    Object.keys(fleetOwnerSockets).forEach(socketId => {
        emitFleetToOwner(socketId);
    });
}

// REST API Endpoints
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'views', 'index.html')));
app.get('/passenger', (req, res) => res.sendFile(path.join(__dirname, 'views', 'passenger.html')));
app.get('/rider', (req, res) => res.sendFile(path.join(__dirname, 'views', 'rider.html')));
app.get('/mechanic', (req, res) => res.sendFile(path.join(__dirname, 'views', 'mechanic.html')));
app.get('/owner', (req, res) => res.sendFile(path.join(__dirname, 'views', 'owner.html')));

// API: Get Passenger Previous Trip History
app.get('/api/passenger/history/:phone', (req, res) => {
    const phone = req.params.phone;
    const history = db.rideHistory.filter(ride => ride.passengerPhone === phone);
    res.json(history);
});

// WebSocket Realtime Engine
io.on('connection', (socket) => {
    console.log(`🔌 Client connected: ${socket.id}`);

    // Fleet Owner Authentication
    socket.on('register-owner-session', (ownerCode) => {
        fleetOwnerSockets[socket.id] = ownerCode.trim();
        emitFleetToOwner(socket.id);
    });

    // Register / Update Single Fleet Vehicle
    socket.on('owner-register-vehicle', (vData) => {
        const ownerCode = vData.ownerCode.trim();
        const plate = vData.plateNumber.toUpperCase();

        if (!db.registeredFleet[ownerCode]) {
            db.registeredFleet[ownerCode] = [];
        }

        const existingIdx = db.registeredFleet[ownerCode].findIndex(v => v.plateNumber === plate);
        if (existingIdx >= 0) {
            db.registeredFleet[ownerCode][existingIdx] = vData;
        } else {
            db.registeredFleet[ownerCode].push(vData);
        }

        if (!db.servicePassports[plate]) {
            db.servicePassports[plate] = {
                currentServiceKm: 0.0,
                totalLifetimeKm: 0.0,
                vin: vData.vin,
                model: vData.model,
                dailyLeaseTarget: vData.dailyTargetGhs || 120,
                ownerCode: ownerCode,
                serviceHistory: []
            };
        } else {
            db.servicePassports[plate].vin = vData.vin;
            db.servicePassports[plate].dailyLeaseTarget = vData.dailyTargetGhs;
            db.servicePassports[plate].ownerCode = ownerCode;
        }

        saveDatabase(db);
        emitFleetToOwner(socket.id);
    });

    // Edit Existing Vehicle Entry
    socket.on('owner-edit-vehicle', (vData) => {
        const ownerCode = vData.ownerCode.trim();
        const plate = vData.plateNumber.toUpperCase();

        if (db.registeredFleet[ownerCode]) {
            const idx = db.registeredFleet[ownerCode].findIndex(v => v.plateNumber === plate);
            if (idx >= 0) {
                db.registeredFleet[ownerCode][idx] = vData;
            }
        }

        if (db.servicePassports[plate]) {
            db.servicePassports[plate].dailyLeaseTarget = vData.dailyTargetGhs;
            db.servicePassports[plate].assignedDriverPhone = vData.assignedDriverPhone;
        }

        saveDatabase(db);
        emitFleetToOwner(socket.id);
    });

    // Driver Connection / Session Recovery
    socket.on('register-rider', (riderData) => {
        const plate = riderData.plateNumber.toUpperCase();
        
        if (!db.servicePassports[plate]) {
            db.servicePassports[plate] = {
                currentServiceKm: 0.0,
                totalLifetimeKm: 0.0,
                dailyLeaseTarget: 120,
                serviceHistory: []
            };
            saveDatabase(db);
        }

        // Restore existing earnings or start fresh
        const existingShiftEarnings = activeRiders[socket.id] ? activeRiders[socket.id].shiftEarnings : 0.00;

        activeRiders[socket.id] = { 
            ...riderData, 
            socketId: socket.id,
            status: 'IDLE',
            shiftEarnings: existingShiftEarnings,
            passport: db.servicePassports[plate]
        };

        socket.emit('passport-update', db.servicePassports[plate]);
        io.emit('rider-location-updated', activeRiders[socket.id]);
        broadcastFleetUpdates();
    });

    socket.on('update-rider-location', (riderData) => {
        const rider = activeRiders[socket.id];
        
        if (rider && rider.lat && rider.lng) {
            const deltaKm = calculateDistanceKm(rider.lat, rider.lng, riderData.lat, riderData.lng);
            
            if (deltaKm > 0.005 && deltaKm < 2.0) {
                const plate = rider.plateNumber.toUpperCase();
                if (db.servicePassports[plate]) {
                    db.servicePassports[plate].currentServiceKm += deltaKm;
                    db.servicePassports[plate].totalLifetimeKm += deltaKm;
                    saveDatabase(db);
                    socket.emit('passport-update', db.servicePassports[plate]);
                }
            }

            rider.lat = riderData.lat;
            rider.lng = riderData.lng;
        } else {
            const plate = riderData.plateNumber ? riderData.plateNumber.toUpperCase() : 'UNKNOWN';
            activeRiders[socket.id] = { 
                ...riderData, 
                socketId: socket.id, 
                status: 'IDLE', 
                shiftEarnings: 0.00,
                passport: db.servicePassports[plate] || { currentServiceKm: 0, totalLifetimeKm: 0, dailyLeaseTarget: 120 }
            };
        }
        
        io.emit('rider-location-updated', activeRiders[socket.id]);
        broadcastFleetUpdates();
    });

    socket.on('request-ride', (reqData) => {
        let targetSocketId = Object.keys(activeRiders).find(
            id => activeRiders[id].username === reqData.targetRiderUsername
        );
        if (targetSocketId) {
            io.to(targetSocketId).emit('incoming-ride-request', { ...reqData, passengerSocketId: socket.id });
        }
    });

    socket.on('accept-ride-request', (resData) => {
        const rider = activeRiders[socket.id];
        if (rider) {
            rider.status = 'EN_ROUTE';
            io.emit('rider-location-updated', rider);
            broadcastFleetUpdates();
        }
        io.to(resData.passengerSocketId).emit('ride-request-response', resData);
    });

    socket.on('start-trip', (tripData) => {
        const rider = activeRiders[socket.id];
        if (rider) {
            rider.status = 'ON_TRIP';
            io.emit('rider-location-updated', rider);
            broadcastFleetUpdates();
        }
    });

    socket.on('end-trip', (tripData) => {
        const rider = activeRiders[socket.id];
        if (rider) {
            const fareGhs = parseFloat(tripData.fareGhs || 0);
            rider.shiftEarnings += fareGhs;
            rider.status = 'IDLE';

            // Record trip permanently in DB
            const tripRecord = {
                tripId: 'TRIP-' + Date.now(),
                riderName: rider.fullName,
                plateNumber: rider.plateNumber,
                passengerPhone: tripData.passengerPhone || '0240000000',
                fareGhs: fareGhs,
                timestamp: new Date().toISOString()
            };

            db.rideHistory.push(tripRecord);
            saveDatabase(db);

            socket.emit('trip-completed-summary', {
                fareGhs: fareGhs,
                totalShiftEarnings: rider.shiftEarnings,
                passport: rider.passport
            });

            if (tripData.passengerSocketId) {
                io.to(tripData.passengerSocketId).emit('passenger-trip-ended', { 
                    fareGhs: fareGhs, 
                    tripRecord: tripRecord 
                });
            }

            io.emit('rider-location-updated', rider);
            broadcastFleetUpdates();
        }
    });

    socket.on('disconnect', () => {
        if (activeRiders[socket.id]) {
            delete activeRiders[socket.id];
            io.emit('rider-disconnected', socket.id);
            broadcastFleetUpdates();
        }
        if (fleetOwnerSockets[socket.id]) {
            delete fleetOwnerSockets[socket.id];
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 PragyaLink Persistent Server running on port ${PORT}`));
// =========================================================
// 📡 FLEET TELEMETRY ROOM ROUTER
// =========================================================
io.on('connection', (socket) => {

    // Driver or Fleet Owner joins their specific Fleet Room
    socket.on('join-fleet-room', (fleetCode) => {
        if (fleetCode) {
            socket.join(`fleet_${fleetCode}`);
            console.log(`[Socket] ${socket.id} joined room: fleet_${fleetCode}`);
        }
    });

    // Handle incoming driver updates and broadcast to Fleet Manager
    socket.on('update-rider-location', (data) => {
        const fleetCode = data.fleetOwnerCode || 'INDEPENDENT';
        
        // Broadcast telemetry to everyone in that specific fleet room (including Fleet Dashboard)
        io.to(`fleet_${fleetCode}`).emit('rider-location-updated', {
            ...data,
            isOnline: true,
            socketId: socket.id
        });
    });
});
// =========================================================
// 📡 TRIP METER & REAL ROAD ROUTING EVENT HANDLERS
// =========================================================
io.on('connection', (socket) => {
    socket.on('start-trip-meter', (data) => {
        console.log(`[Trip] Meter started for request: ${data.requestId}`);
        // Broadcast to passenger that the driver has tapped "Start Trip"
        io.emit('trip-meter-started', data);
    });
});
// =========================================================
// 📡 DISPATCH DRIVER DETAILS TO PASSENGER
// =========================================================
io.on('connection', (socket) => {
    socket.on('accept-ride-payload', (payload) => {
        io.emit('driver-accepted-request', payload);
    });
});
// =========================================================
// 📡 GLOBAL DRIVER ONLINE REGISTRY & HEARTBEAT BROADCAST
// =========================================================
const activeOnlineDrivers = new Map();

io.on('connection', (socket) => {

    // When a driver goes online or updates location
    socket.on('driver-go-online', (driverData) => {
        activeOnlineDrivers.set(socket.id, {
            socketId: socket.id,
            driverName: driverData.driverName || 'Kofi Mensah',
            driverPhone: driverData.driverPhone || '0550000000',
            plateNumber: driverData.plateNumber || 'M-24-AS',
            lat: driverData.lat || 5.6037, // Default Accra/Kumasi center fallback
            lng: driverData.lng || -0.1870,
            isOnline: true,
            lastSeen: new Date()
        });

        console.log(`[Driver Online] ${driverData.driverName} (${driverData.plateNumber}) is now active.`);
        
        // Broadcast total online drivers list to ALL passengers instantly
        io.emit('all-online-drivers', Array.from(activeOnlineDrivers.values()));
    });

    // Handle periodic driver location ping
    socket.on('driver-location-ping', (coords) => {
        if (activeOnlineDrivers.has(socket.id)) {
            const driver = activeOnlineDrivers.get(socket.id);
            driver.lat = coords.lat;
            driver.lng = coords.lng;
            driver.lastSeen = new Date();
            activeOnlineDrivers.set(socket.id, driver);

            io.emit('all-online-drivers', Array.from(activeOnlineDrivers.values()));
        }
    });

    // Cleanup on disconnect
    socket.on('disconnect', () => {
        if (activeOnlineDrivers.has(socket.id)) {
            activeOnlineDrivers.delete(socket.id);
            console.log(`[Driver Offline] Socket ${socket.id} disconnected.`);
            io.emit('all-online-drivers', Array.from(activeOnlineDrivers.values()));
        }
    });
});
// =========================================================
// 📡 CRITICAL FIX: FLEET ROOM TELEMETRY BRIDGE
// =========================================================
io.on('connection', (socket) => {

    // When a driver goes online, automatically bridge their data to their Fleet Owner Room
    socket.on('driver-go-online', (driverData) => {
        const fleetCode = driverData.fleetOwnerCode || localStorage?.getItem('pragya_fleet_code') || 'KUMASI_FLEET_01';
        
        const telemetryPayload = {
            username: driverData.driverName || 'Kofi Mensah',
            plateNumber: driverData.plateNumber || 'M-24-AS',
            fleetOwnerCode: fleetCode,
            operatingMode: 'FLEET',
            lat: driverData.lat,
            lng: driverData.lng,
            isOnline: true,
            engineHealth: {
                odometerKm: driverData.odometerKm || '0.0',
                distSinceServiceKm: driverData.distSinceServiceKm || '0.0',
                serviceOverdue: (driverData.distSinceServiceKm || 0) >= 1500,
                vehicleStatus: (driverData.distSinceServiceKm || 0) >= 1500 ? 'MAINTENANCE_REQUIRED' : 'HEALTHY'
            }
        };

        // 1. Broadcast to Fleet Manager inside the specific fleet room
        io.to(`fleet_${fleetCode}`).emit('rider-location-updated', telemetryPayload);
        
        // 2. Broadcast globally so passenger app can also see available tricycles
        io.emit('rider-location-updated', telemetryPayload);
    });
});
// =========================================================
// 📡 CRITICAL FIX: AUTOMATIC RE-ROOMING ON RECONNECT
// =========================================================
io.on('connection', (socket) => {
    socket.on('ping-fleet-status', (data) => {
        const code = data.fleetCode || 'KUMASI_FLEET_01';
        socket.join(`fleet_${code}`);
        io.to(`fleet_${code}`).emit('fleet-presence-confirmed', { socketId: socket.id, timestamp: new Date() });
    });
});
