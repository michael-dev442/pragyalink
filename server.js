const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// HTML Views
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'views', 'index.html')));
app.get('/passenger', (req, res) => res.sendFile(path.join(__dirname, 'views', 'passenger.html')));
app.get('/rider', (req, res) => res.sendFile(path.join(__dirname, 'views', 'rider.html')));
app.get('/mechanic', (req, res) => res.sendFile(path.join(__dirname, 'views', 'mechanic.html')));
app.get('/owner', (req, res) => res.sendFile(path.join(__dirname, 'views', 'owner.html')));

// Data Stores
const activeRiders = {};     
const activeWorkshops = {};  
const servicePassports = {}; 
const fleetOwnerSockets = {}; // Tracks socketId -> fleetOwnerCode

function calculateDistanceKm(lat1, lon1, lat2, lon2) {
    const R = 6371; 
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLon / 2) * Math.sin(dLon / 2);
    return R * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
}

// Broadcasts ONLY relevant vehicles to a specific Fleet Owner
function emitFleetToOwner(ownerSocketId) {
    const fleetCode = fleetOwnerSockets[ownerSocketId];
    if (!fleetCode) return;

    const myFleet = Object.values(activeRiders).filter(
        rider => rider.ownerCode && rider.ownerCode.trim() === fleetCode.trim()
    );

    io.to(ownerSocketId).emit('current-riders', myFleet);
}

function broadcastFleetUpdates() {
    Object.keys(fleetOwnerSockets).forEach(socketId => {
        emitFleetToOwner(socketId);
    });
}

io.on('connection', (socket) => {
    console.log(`🔌 Client connected: ${socket.id}`);

    // Fleet Owner Authentication / Filter Registration
    socket.on('register-owner-session', (ownerCode) => {
        fleetOwnerSockets[socket.id] = ownerCode.trim();
        console.log(`👑 Fleet Owner Session Authenticated: ${ownerCode.trim()} [Socket: ${socket.id}]`);
        emitFleetToOwner(socket.id);
    });

    // Fleet Owner Asset Pre-Registration
    socket.on('owner-register-vehicle', (vData) => {
        const plate = vData.plateNumber.toUpperCase();
        if (!servicePassports[plate]) {
            servicePassports[plate] = {
                currentServiceKm: 0.0,
                totalLifetimeKm: 0.0,
                vin: vData.vin,
                model: vData.model,
                dailyLeaseTarget: vData.dailyTargetGhs || 120,
                ownerCode: vData.ownerCode || '',
                serviceHistory: []
            };
        } else {
            servicePassports[plate].vin = vData.vin;
            servicePassports[plate].dailyLeaseTarget = vData.dailyTargetGhs;
            servicePassports[plate].ownerCode = vData.ownerCode;
        }
        console.log(`📋 Fleet Vehicle Registered: ${plate} [Owner Code: ${vData.ownerCode}]`);
    });

    socket.on('register-rider', (riderData) => {
        const plate = riderData.plateNumber.toUpperCase();
        
        if (!servicePassports[plate]) {
            servicePassports[plate] = {
                currentServiceKm: 0.0,
                totalLifetimeKm: 0.0,
                dailyLeaseTarget: 120,
                serviceHistory: []
            };
        }

        activeRiders[socket.id] = { 
            ...riderData, 
            socketId: socket.id,
            status: 'IDLE',
            shiftEarnings: 0.00,
            passport: servicePassports[plate]
        };

        console.log(`🛺 Driver active: ${riderData.fullName} [Plate: ${plate}] [Fleet Code: ${riderData.ownerCode}]`);
        
        socket.emit('passport-update', servicePassports[plate]);
        
        // Broadcast to passengers (all drivers) & filtered to owners
        io.emit('rider-location-updated', activeRiders[socket.id]);
        broadcastFleetUpdates();
    });

    socket.on('update-rider-location', (riderData) => {
        const rider = activeRiders[socket.id];
        
        if (rider && rider.lat && rider.lng) {
            const deltaKm = calculateDistanceKm(rider.lat, rider.lng, riderData.lat, riderData.lng);
            
            if (deltaKm > 0.005 && deltaKm < 2.0) {
                const plate = rider.plateNumber.toUpperCase();
                if (servicePassports[plate]) {
                    servicePassports[plate].currentServiceKm += deltaKm;
                    servicePassports[plate].totalLifetimeKm += deltaKm;
                    socket.emit('passport-update', servicePassports[plate]);
                }
            }

            rider.lat = riderData.lat;
            rider.lng = riderData.lng;
        } else {
            activeRiders[socket.id] = { ...riderData, socketId: socket.id, status: 'IDLE', shiftEarnings: 0.00 };
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

            socket.emit('trip-completed-summary', {
                fareGhs: fareGhs,
                totalShiftEarnings: rider.shiftEarnings,
                passport: rider.passport
            });

            if (tripData.passengerSocketId) {
                io.to(tripData.passengerSocketId).emit('passenger-trip-ended', { fareGhs: fareGhs });
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
server.listen(PORT, () => console.log(`🚀 PragyaLink Server running on port ${PORT}`));
