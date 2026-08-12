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

function calculateDistanceKm(lat1, lon1, lat2, lon2) {
    const R = 6371; 
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLon / 2) * Math.sin(dLon / 2);
    return R * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
}

io.on('connection', (socket) => {
    console.log(`🔌 Client connected: ${socket.id}`);

    socket.emit('current-riders', Object.values(activeRiders));

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
                assignedDriverPhone: vData.assignedDriverPhone,
                serviceHistory: []
            };
        } else {
            servicePassports[plate].vin = vData.vin;
            servicePassports[plate].dailyLeaseTarget = vData.dailyTargetGhs;
        }
        console.log(`📋 Fleet Vehicle Registered: ${plate} [Target: GH₵ ${vData.dailyTargetGhs}]`);
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
            passport: servicePassports[plate]
        };

        console.log(`🛺 Driver active: ${riderData.fullName} [Plate: ${plate}]`);
        
        socket.emit('passport-update', servicePassports[plate]);
        io.emit('rider-location-updated', activeRiders[socket.id]);
        io.emit('current-riders', Object.values(activeRiders));
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
            activeRiders[socket.id] = { ...riderData, socketId: socket.id };
        }
        
        io.emit('rider-location-updated', activeRiders[socket.id]);
        io.emit('current-riders', Object.values(activeRiders));
    });

    socket.on('verify-service-reset', (data) => {
        const plate = data.plateNumber.toUpperCase();
        if (servicePassports[plate]) {
            const serviceEntry = {
                date: new Date().toLocaleDateString(),
                mileageAtService: servicePassports[plate].currentServiceKm.toFixed(1),
                workshopName: data.workshopName || 'Authorized Workshop',
                type: data.serviceType || 'Oil & Filter Routine Service'
            };

            servicePassports[plate].serviceHistory.push(serviceEntry);
            servicePassports[plate].currentServiceKm = 0.0;

            const riderSocketId = Object.keys(activeRiders).find(
                id => activeRiders[id].plateNumber.toUpperCase() === plate
            );

            if (riderSocketId) {
                io.to(riderSocketId).emit('passport-update', servicePassports[plate]);
                io.to(riderSocketId).emit('service-verified-alert', serviceEntry);
            }
            
            io.emit('current-riders', Object.values(activeRiders));
        }
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
        io.to(resData.passengerSocketId).emit('ride-request-response', resData);
    });

    socket.on('register-workshop', (shopData) => {
        activeWorkshops[socket.id] = { ...shopData, socketId: socket.id };
        io.emit('workshop-registered', activeWorkshops[socket.id]);
    });

    socket.on('driver-sos', (sosData) => {
        io.emit('driver-sos-alert', { ...sosData, socketId: socket.id });
    });

    socket.on('disconnect', () => {
        if (activeRiders[socket.id]) {
            delete activeRiders[socket.id];
            io.emit('rider-disconnected', socket.id);
            io.emit('current-riders', Object.values(activeRiders));
        }
        if (activeWorkshops[socket.id]) {
            delete activeWorkshops[socket.id];
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 PragyaLink Server running on port ${PORT}`));
