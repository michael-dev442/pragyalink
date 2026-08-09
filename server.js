const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" }
});

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// Serve HTML Views
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'views', 'passenger.html')));
app.get('/passenger', (req, res) => res.sendFile(path.join(__dirname, 'views', 'passenger.html')));
app.get('/rider', (req, res) => res.sendFile(path.join(__dirname, 'views', 'rider.html')));
app.get('/mechanic', (req, res) => res.sendFile(path.join(__dirname, 'views', 'mechanic.html')));

// In-Memory Data Stores
const activeRiders = {};     // { socketId: { username, fullName, contact, vehicleType, plateNumber, lat, lng } }
const activeWorkshops = {};  // { socketId: { shopName, ownerName, emergencyContact, specialty, lat, lng } }

io.on('connection', (socket) => {
    console.log(`🔌 New client connected: ${socket.id}`);

    // Send current active riders immediately to newly connected passengers
    socket.emit('current-riders', Object.values(activeRiders));

    // 1. Driver Registration
    socket.on('register-rider', (riderData) => {
        activeRiders[socket.id] = { ...riderData, socketId: socket.id };
        console.log(`🛺 Driver registered: ${riderData.fullName || riderData.username} (${socket.id})`);
        
        // Broadcast new driver to all passengers
        io.emit('rider-location-updated', activeRiders[socket.id]);
    });

    // 2. Driver Continuous GPS Location Update
    socket.on('update-rider-location', (riderData) => {
        if (activeRiders[socket.id]) {
            activeRiders[socket.id].lat = riderData.lat;
            activeRiders[socket.id].lng = riderData.lng;
        } else {
            activeRiders[socket.id] = { ...riderData, socketId: socket.id };
        }
        
        // Broadcast updated position to all connected passengers in real-time
        io.emit('rider-location-updated', activeRiders[socket.id]);
    });

    // 3. Passenger Ride Request Dispatch
    socket.on('request-ride', (requestData) => {
        console.log(`📡 Passenger requesting ride from: ${requestData.targetRiderUsername}`);
        
        // Find targeted rider by socket or username
        let targetSocketId = Object.keys(activeRiders).find(
            id => activeRiders[id].username === requestData.targetRiderUsername
        );

        if (targetSocketId) {
            io.to(targetSocketId).emit('incoming-ride-request', {
                ...requestData,
                passengerSocketId: socket.id
            });
        }
    });

    // 4. Driver Acceptance/Declination Response
    socket.on('accept-ride-request', (responseData) => {
        io.to(responseData.passengerSocketId).emit('ride-request-response', responseData);
    });

    // 5. Workshop Registration
    socket.on('register-workshop', (shopData) => {
        activeWorkshops[socket.id] = { ...shopData, socketId: socket.id };
        console.log(`🛠️ Workshop registered: ${shopData.shopName}`);
        io.emit('workshop-registered', activeWorkshops[socket.id]);
    });

    // 6. Driver Breakdown SOS
    socket.on('driver-sos', (sosData) => {
        console.log(`🚨 SOS Alert from driver: ${sosData.fullName || sosData.username}`);
        io.emit('driver-sos-alert', { ...sosData, socketId: socket.id });
    });

    // 7. Handle Disconnections
    socket.on('disconnect', () => {
        console.log(`❌ Client disconnected: ${socket.id}`);
        if (activeRiders[socket.id]) {
            delete activeRiders[socket.id];
            io.emit('rider-disconnected', socket.id);
        }
        if (activeWorkshops[socket.id]) {
            delete activeWorkshops[socket.id];
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 PragyaLink Server running on port ${PORT}`);
});
