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

// =========================================================
// 📦 PERSISTENT JSON FILE STORAGE
// =========================================================
const DB_FILE = path.join(__dirname, 'database.json');

function loadDatabase() {
    if (!fs.existsSync(DB_FILE)) {
        const initialData = {
            servicePassports: {}, rideHistory: [], registeredFleet: {},
            driverStats: {}, safetyAlerts: [], persistedPoolTrips: {}
        };
        fs.writeFileSync(DB_FILE, JSON.stringify(initialData, null, 2));
        return initialData;
    }
    try {
        const data = fs.readFileSync(DB_FILE, 'utf8');
        const parsed = JSON.parse(data);
        // Guard against a partially-shaped file from an older version
        parsed.servicePassports = parsed.servicePassports || {};
        parsed.rideHistory = parsed.rideHistory || [];
        parsed.registeredFleet = parsed.registeredFleet || {};
        parsed.driverStats = parsed.driverStats || {};
        parsed.safetyAlerts = parsed.safetyAlerts || [];
        parsed.persistedPoolTrips = parsed.persistedPoolTrips || {};
        return parsed;
    } catch (e) {
        console.error("Error reading database.json, re-initializing...", e);
        return { servicePassports: {}, rideHistory: [], registeredFleet: {}, driverStats: {}, safetyAlerts: [], persistedPoolTrips: {} };
    }
}

function saveDatabase(data) {
    // Atomic write: write to a temp file then rename, so a crash mid-write can never
    // leave database.json half-written/corrupted.
    const tmpFile = DB_FILE + '.tmp';
    try {
        fs.writeFileSync(tmpFile, JSON.stringify(data, null, 2));
        fs.renameSync(tmpFile, DB_FILE);
    } catch (e) {
        console.error('❌ Failed to save database.json:', e);
    }
}

let db = loadDatabase();

// =========================================================
// 🛡️ CRASH SAFETY
// =========================================================
// Wraps every socket event handler so a malformed payload or unexpected null throws
// inside ONE handler for ONE client, instead of crashing the process for every
// connected driver, passenger, and dashboard. Use this instead of socket.on() directly.
function safeOn(socket, eventName, handler) {
    socket.on(eventName, (...args) => {
        try {
            handler(...args);
        } catch (err) {
            console.error(`⚠️ Error handling '${eventName}' from ${socket.id}:`, err);
        }
    });
}

// Last-resort net: log and keep the process alive rather than taking every connected
// client down over one unexpected error that slipped past safeOn (e.g. inside a timer).
process.on('uncaughtException', (err) => console.error('🔥 Uncaught exception (server kept alive):', err));
process.on('unhandledRejection', (err) => console.error('🔥 Unhandled rejection (server kept alive):', err));

// =========================================================
// 📲 SMS GATEWAY HOOK (pluggable)
// =========================================================
// This project has no telecom SMS account wired in. A website cannot send/receive SMS
// directly — this hook is the correct integration point for a real provider
// (Twilio / Africa's Talking / Hubtel). Plug the provider's API call in here; every
// call site in this file already calls this function at the right moment.
function sendSmsNotification(phoneNumber, message) {
    if (!phoneNumber) return;
    console.log(`📲 [SMS STUB — no gateway configured] Would send to ${phoneNumber}: "${message}"`);
    // Example real integration (Twilio):
    // twilioClient.messages.create({ to: phoneNumber, from: TWILIO_NUMBER, body: message });
}

// =========================================================
// 🧠 RUNTIME STATE
// =========================================================
const activeRiders = {};        // socket.id -> live driver telemetry
const activeWorkshops = {};     // socket.id -> registered mechanic workshop
const fleetOwnerSockets = {};   // socket.id -> fleet owner code (dashboard viewers)
const safetyMonitorSockets = new Set(); // sockets watching passenger-sos-alert (owner/mechanic dashboards)
const activePoolTrips = {};     // driver socket.id -> pool trip manifest
const pendingDispatchQueue = {}; // username -> [ reqData, ... ] queued while driver was offline

const SERVICE_INTERVAL_KM = 1500;
const INDEPENDENT_CODE = 'INDEPENDENT';
const POOL_MATCH_RADIUS_KM = 0.7;   // how close a new pickup must be to the driver's current position
const POOL_BEARING_TOLERANCE_DEG = 35; // how "on the way" a new dropoff must be
const POOL_MAX_PASSENGERS = 3;
const BASE_FARE_GHS = 5.00;
const RATE_PER_KM_GHS = 3.00;

function calculateDistanceKm(lat1, lon1, lat2, lon2) {
    const R = 6371;
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLon / 2) * Math.sin(dLon / 2);
    return R * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
}

// Compass bearing from point A to point B, in degrees (0-360)
function calculateBearingDeg(lat1, lon1, lat2, lon2) {
    const toRad = d => d * Math.PI / 180;
    const y = Math.sin(toRad(lon2 - lon1)) * Math.cos(toRad(lat2));
    const x = Math.cos(toRad(lat1)) * Math.sin(toRad(lat2)) -
              Math.sin(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.cos(toRad(lon2 - lon1));
    return (Math.atan2(y, x) * 180 / Math.PI + 360) % 360;
}

function angleDiffDeg(a, b) {
    const diff = Math.abs(a - b) % 360;
    return diff > 180 ? 360 - diff : diff;
}

function isFleetLinked(fleetOwnerCode) {
    return !!fleetOwnerCode && fleetOwnerCode.trim().toUpperCase() !== INDEPENDENT_CODE;
}

// Is this plate present in the owner's registered vehicle list?
function isPlateRegisteredToFleet(fleetOwnerCode, plateNumber) {
    const list = db.registeredFleet[fleetOwnerCode] || [];
    return list.some(v => v.plateNumber === plateNumber);
}

function ensurePassport(plate, defaults = {}) {
    if (!db.servicePassports[plate]) {
        db.servicePassports[plate] = {
            currentServiceKm: 0.0,
            totalLifetimeKm: 0.0,
            dailyLeaseTarget: defaults.dailyTargetGhs || 120,
            vin: defaults.vin || null,
            model: defaults.model || null,
            ownerCode: defaults.ownerCode || null,
            serviceHistory: []
        };
    }
    return db.servicePassports[plate];
}

function ensureDriverStats(plate) {
    if (!db.driverStats[plate]) {
        db.driverStats[plate] = {
            fullName: null,
            totalTrips: 0,
            ratingSum: 0,
            ratingCount: 0,
            smoothnessSum: 0,
            smoothnessCount: 0,
            badge: 'New Driver'
        };
    }
    return db.driverStats[plate];
}

function recomputeBadge(stats) {
    const avgRating = stats.ratingCount > 0 ? stats.ratingSum / stats.ratingCount : 0;
    const avgSmoothness = stats.smoothnessCount > 0 ? stats.smoothnessSum / stats.smoothnessCount : 0;

    if (stats.totalTrips >= 50 && avgRating >= 4.5 && avgSmoothness >= 85) {
        stats.badge = 'Gold';
    } else if (stats.totalTrips >= 20 && avgRating >= 4.0 && avgSmoothness >= 70) {
        stats.badge = 'Silver';
    } else if (stats.totalTrips >= 1) {
        stats.badge = 'Bronze';
    } else {
        stats.badge = 'New Driver';
    }
}

// Build the filtered payload a fleet-owner dashboard (owner.html / fleet.html) needs:
// every registered vehicle, cross-referenced with whichever of them are currently online.
function emitFleetToOwner(ownerSocketId) {
    const fleetCode = fleetOwnerSockets[ownerSocketId];
    if (!fleetCode) return;

    const liveFleet = Object.values(activeRiders).filter(
        rider => rider.fleetOwnerCode && rider.fleetOwnerCode.trim() === fleetCode.trim()
    );

    const savedFleetVehicles = (db.registeredFleet[fleetCode] || []).map(v => {
        const passport = db.servicePassports[v.plateNumber] || null;
        const stats = db.driverStats[v.plateNumber] || null;
        return { ...v, passport, stats };
    });

    io.to(ownerSocketId).emit('current-riders', {
        activeDrivers: liveFleet,
        registeredVehicles: savedFleetVehicles,
        fleetCode
    });
}

function broadcastFleetUpdates() {
    Object.keys(fleetOwnerSockets).forEach(socketId => emitFleetToOwner(socketId));
}

// Pool trips live in `activePoolTrips` keyed by socket.id, which is meaningless after a
// restart or a driver's page refresh (new socket.id every reconnect). Mirror each trip into
// the persisted database keyed by plate number instead, so it can be restored the moment
// that driver reconnects — covers both a server restart AND the driver just refreshing mid-trip.
function persistPoolTrip(plateNumber, trip) {
    if (!plateNumber) return;
    if (trip) {
        db.persistedPoolTrips[plateNumber] = { ...trip, persistedAt: new Date().toISOString() };
    } else {
        delete db.persistedPoolTrips[plateNumber];
    }
    saveDatabase(db);
}

const POOL_TRIP_RESTORE_WINDOW_MS = 2 * 60 * 60 * 1000; // 2 hours

function buildLeaderboard(fleetCode) {
    let plates;
    if (fleetCode) {
        plates = (db.registeredFleet[fleetCode] || []).map(v => v.plateNumber);
    } else {
        plates = Object.keys(db.driverStats);
    }

    return plates
        .map(plate => {
            const stats = db.driverStats[plate];
            if (!stats) return null;
            const avgRating = stats.ratingCount > 0 ? (stats.ratingSum / stats.ratingCount) : 0;
            const avgSmoothness = stats.smoothnessCount > 0 ? (stats.smoothnessSum / stats.smoothnessCount) : 0;
            return {
                plateNumber: plate,
                fullName: stats.fullName || 'Driver',
                totalTrips: stats.totalTrips,
                avgRating: Math.round(avgRating * 10) / 10,
                avgSmoothness: Math.round(avgSmoothness),
                badge: stats.badge
            };
        })
        .filter(Boolean)
        .sort((a, b) => (b.totalTrips * 1 + b.avgRating * 20 + b.avgSmoothness) - (a.totalTrips * 1 + a.avgRating * 20 + a.avgSmoothness))
        .slice(0, 20);
}

// =========================================================
// 🌐 REST API
// =========================================================
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'views', 'index.html')));
app.get('/passenger', (req, res) => res.sendFile(path.join(__dirname, 'views', 'passenger.html')));
app.get('/rider', (req, res) => res.sendFile(path.join(__dirname, 'views', 'rider.html')));
app.get('/mechanic', (req, res) => res.sendFile(path.join(__dirname, 'views', 'mechanic.html')));
app.get('/owner', (req, res) => res.sendFile(path.join(__dirname, 'views', 'owner.html')));
app.get('/fleet', (req, res) => res.sendFile(path.join(__dirname, 'views', 'fleet.html')));

app.get('/api/passenger/history/:phone', (req, res) => {
    const phone = req.params.phone;
    const history = db.rideHistory.filter(ride => ride.passengerPhone === phone);
    res.json(history);
});

// =========================================================
// 📡 REALTIME ENGINE — single connection handler
// =========================================================
io.on('connection', (socket) => {
    console.log(`🔌 Client connected: ${socket.id}`);

    // Passenger app requests a full snapshot on connect (fixes the "cold start" gap where
    // drivers already online wouldn't appear until their next telemetry tick)
    safeOn(socket, 'request-online-drivers', () => {
        socket.emit('current-riders', Object.values(activeRiders));
    });

    // ---- Fleet owner dashboard session (used by owner.html AND fleet.html) ----
    safeOn(socket, 'register-owner-session', (ownerCode) => {
        if (!ownerCode || !ownerCode.trim()) return;
        fleetOwnerSockets[socket.id] = ownerCode.trim();
        safetyMonitorSockets.add(socket.id); // fleet owners also see passenger safety alerts for their drivers
        emitFleetToOwner(socket.id);
    });

    safeOn(socket, 'owner-register-vehicle', (vData) => {
        if (!vData || !vData.ownerCode || !vData.plateNumber) return;
        const ownerCode = vData.ownerCode.trim();
        const plate = vData.plateNumber.toUpperCase();
        const record = { ...vData, plateNumber: plate, ownerCode };

        if (!db.registeredFleet[ownerCode]) db.registeredFleet[ownerCode] = [];
        const idx = db.registeredFleet[ownerCode].findIndex(v => v.plateNumber === plate);
        if (idx >= 0) db.registeredFleet[ownerCode][idx] = record;
        else db.registeredFleet[ownerCode].push(record);

        const passport = ensurePassport(plate, record);
        passport.vin = record.vin;
        passport.model = record.model;
        passport.dailyLeaseTarget = record.dailyTargetGhs || passport.dailyLeaseTarget;
        passport.ownerCode = ownerCode;
        passport.assignedDriverPhone = record.assignedDriverPhone || passport.assignedDriverPhone;

        saveDatabase(db);
        emitFleetToOwner(socket.id);

        // If a driver for this plate is already online, refresh their registration flag live
        const liveEntry = Object.values(activeRiders).find(r => r.plateNumber === plate);
        if (liveEntry) {
            liveEntry.isRegisteredVehicle = true;
            broadcastFleetUpdates();
        }
    });

    safeOn(socket, 'owner-edit-vehicle', (vData) => {
        if (!vData || !vData.ownerCode || !vData.plateNumber) return;
        const ownerCode = vData.ownerCode.trim();
        const plate = vData.plateNumber.toUpperCase();

        if (db.registeredFleet[ownerCode]) {
            const idx = db.registeredFleet[ownerCode].findIndex(v => v.plateNumber === plate);
            if (idx >= 0) db.registeredFleet[ownerCode][idx] = { ...vData, plateNumber: plate, ownerCode };
        }

        if (db.servicePassports[plate]) {
            db.servicePassports[plate].dailyLeaseTarget = vData.dailyTargetGhs;
            db.servicePassports[plate].assignedDriverPhone = vData.assignedDriverPhone;
        }

        saveDatabase(db);
        emitFleetToOwner(socket.id);
    });

    // ---- Driver presence & telemetry ----

    // One-shot "I am now online" announcement (also used for session-restore-on-refresh)
    safeOn(socket, 'driver-go-online', (driverData) => {
        registerDriverTelemetry(socket, driverData, { isGoOnlineEvent: true });
    });

    // Continuous GPS + engine-health telemetry stream (also doubles as the heartbeat)
    safeOn(socket, 'update-rider-location', (riderData) => {
        registerDriverTelemetry(socket, riderData, { isGoOnlineEvent: false });
    });

    // Legacy driver session recovery hook — kept for backward compatibility
    safeOn(socket, 'register-rider', (riderData) => {
        registerDriverTelemetry(socket, riderData, { isGoOnlineEvent: true });
    });

    function registerDriverTelemetry(socket, data, opts) {
        if (!data || !data.plateNumber) return;
        const plate = data.plateNumber.toUpperCase();
        const fleetOwnerCode = (data.fleetOwnerCode || INDEPENDENT_CODE).trim();
        const fleetLinked = isFleetLinked(fleetOwnerCode);

        const passport = ensurePassport(plate);
        const stats = ensureDriverStats(plate);
        if (data.fullName || data.driverName) stats.fullName = data.fullName || data.driverName;

        // Track distance since the last known position, feed the service passport
        const prev = activeRiders[socket.id];
        if (prev && prev.lat != null && prev.lng != null && data.lat != null && data.lng != null) {
            const deltaKm = calculateDistanceKm(prev.lat, prev.lng, data.lat, data.lng);
            if (deltaKm > 0.005 && deltaKm < 2.0) {
                passport.currentServiceKm += deltaKm;
                passport.totalLifetimeKm += deltaKm;
                saveDatabase(db);
            }
        }

        // Driving-smoothness score (0-100) contributed by the client's own accel/decel sampling
        if (typeof data.smoothnessSample === 'number' && data.smoothnessSample >= 0 && data.smoothnessSample <= 100) {
            stats.smoothnessSum += data.smoothnessSample;
            stats.smoothnessCount += 1;
            recomputeBadge(stats);
        }

        const existingShiftEarnings = prev ? prev.shiftEarnings : 0.00;
        const wasOffline = !prev;

        activeRiders[socket.id] = {
            username: data.username || data.driverName || prev?.username || 'Driver',
            fullName: data.fullName || data.driverName || prev?.fullName || 'Driver',
            contact: data.contact || data.driverPhone || prev?.contact || null,
            plateNumber: plate,
            fleetOwnerCode,
            operatingMode: data.operatingMode || (fleetLinked ? 'FLEET' : 'OWNER'),
            lat: data.lat != null ? data.lat : prev?.lat,
            lng: data.lng != null ? data.lng : prev?.lng,
            socketId: socket.id,
            status: prev?.status || 'IDLE',
            shiftEarnings: existingShiftEarnings,
            engineHealth: data.engineHealth || prev?.engineHealth || null,
            isRegisteredVehicle: fleetLinked ? isPlateRegisteredToFleet(fleetOwnerCode, plate) : true,
            passport,
            driverStats: stats,
            lastSeen: new Date().toISOString()
        };

        // Flush any dispatch requests that arrived while this driver was offline
        const username = activeRiders[socket.id].username;
        if (wasOffline && pendingDispatchQueue[username] && pendingDispatchQueue[username].length > 0) {
            pendingDispatchQueue[username].forEach(reqData => {
                io.to(socket.id).emit('incoming-ride-request', reqData);
            });
            pendingDispatchQueue[username] = [];
        }

        // Restore an in-progress pool trip that survived a server restart or the driver's
        // own page refresh — the server would otherwise have no memory of it under the new
        // socket.id, even though the driver (and their passengers) were mid-trip.
        // Only within a short window: an old abandoned trip (driver ended their shift
        // mid-trip rather than dropped connection) shouldn't resurrect on some future shift
        // with passengers long gone.
        if (wasOffline && !activePoolTrips[socket.id] && db.persistedPoolTrips[plate]) {
            const restoredTrip = db.persistedPoolTrips[plate];
            const persistedAgeMs = Date.now() - new Date(restoredTrip.persistedAt || 0).getTime();
            if (persistedAgeMs <= POOL_TRIP_RESTORE_WINDOW_MS) {
                restoredTrip.driverSocketId = socket.id;
                activePoolTrips[socket.id] = restoredTrip;
                activeRiders[socket.id].status = restoredTrip.stops.some(s => s.status === 'ONBOARD') ? 'ON_TRIP' : 'EN_ROUTE';
                socket.emit('pool-trip-updated', restoredTrip);
                console.log(`♻️ Restored pool trip for ${plate} after reconnect`);
            } else {
                delete db.persistedPoolTrips[plate];
                saveDatabase(db);
                console.log(`🗑️ Discarded stale pool trip for ${plate} (older than restore window)`);
            }
        }

        socket.emit('passport-update', passport);
        io.emit('rider-location-updated', activeRiders[socket.id]);
        broadcastFleetUpdates();
    }

    // Driver explicitly goes offline without disconnecting the socket
    safeOn(socket, 'driver-go-offline', () => {
        if (activeRiders[socket.id]) {
            delete activeRiders[socket.id];
            delete activePoolTrips[socket.id];
            io.emit('rider-disconnected', socket.id);
            broadcastFleetUpdates();
        }
    });

    // ---- Dispatch lifecycle (private rides) ----

    safeOn(socket, 'request-ride', (reqData) => {
        const targetSocketId = Object.keys(activeRiders).find(
            id => activeRiders[id].username === reqData.targetRiderUsername
        );
        if (targetSocketId) {
            io.to(targetSocketId).emit('incoming-ride-request', { ...reqData, passengerSocketId: socket.id });
        } else {
            // Driver isn't currently connected — queue the request and try to reach them by SMS.
            // This is the "offline dispatch fallback": real delivery requires a telecom SMS
            // gateway (see sendSmsNotification above); until one is configured this only logs.
            if (!pendingDispatchQueue[reqData.targetRiderUsername]) pendingDispatchQueue[reqData.targetRiderUsername] = [];
            pendingDispatchQueue[reqData.targetRiderUsername].push({ ...reqData, passengerSocketId: socket.id });

            const lastKnownPlate = reqData.targetPlateNumber;
            const lastKnownPhone = lastKnownPlate ? (db.servicePassports[lastKnownPlate]?.assignedDriverPhone) : null;
            sendSmsNotification(lastKnownPhone, `PragyaLink: New ride request waiting for you near ${reqData.pickupLabel || 'your last known area'}. Open the app to accept.`);
        }
    });

    // Driver accepts OR declines a dispatch (status: 'accepted' | 'declined')
    safeOn(socket, 'respond-ride-request', (resData) => {
        const rider = activeRiders[socket.id];
        if (rider && resData.status === 'accepted') {
            rider.status = 'EN_ROUTE';
            io.emit('rider-location-updated', rider);
            broadcastFleetUpdates();
        }
        if (resData.passengerSocketId) {
            io.to(resData.passengerSocketId).emit('ride-request-response', resData);
        }
    });

    safeOn(socket, 'start-trip-meter', (data) => {
        const rider = activeRiders[socket.id];
        if (rider) {
            rider.status = 'ON_TRIP';
            io.emit('rider-location-updated', rider);
            broadcastFleetUpdates();
        }
        io.emit('trip-meter-started', data);
    });

    safeOn(socket, 'end-trip', (tripData) => {
        const rider = activeRiders[socket.id];
        if (rider) {
            const fareGhs = parseFloat(tripData.fareGhs || 0);
            rider.shiftEarnings += fareGhs;
            rider.status = 'IDLE';

            const stats = ensureDriverStats(rider.plateNumber);
            stats.totalTrips += 1;
            recomputeBadge(stats);

            const tripRecord = {
                tripId: 'TRIP-' + Date.now(),
                riderName: rider.fullName,
                plateNumber: rider.plateNumber,
                passengerPhone: tripData.passengerPhone || null,
                fareGhs,
                distanceDrivenKm: tripData.distanceDrivenKm || null,
                timestamp: new Date().toISOString()
            };
            db.rideHistory.push(tripRecord);
            saveDatabase(db);

            socket.emit('trip-completed-summary', {
                fareGhs,
                totalShiftEarnings: rider.shiftEarnings,
                passport: rider.passport
            });

            if (tripData.passengerSocketId) {
                io.to(tripData.passengerSocketId).emit('passenger-trip-ended', { fareGhs, tripRecord, plateNumber: rider.plateNumber });
            }

            io.emit('rider-location-updated', rider);
            broadcastFleetUpdates();
        }
    });

    // Driver confirms an oil change / service was completed
    safeOn(socket, 'confirm-service-reset', (data) => {
        if (!data || !data.plateNumber) return;
        const plate = data.plateNumber.toUpperCase();
        const passport = ensurePassport(plate);

        passport.serviceHistory.push({
            resetAtKm: passport.currentServiceKm,
            timestamp: new Date().toISOString()
        });
        passport.currentServiceKm = 0.0;
        saveDatabase(db);

        socket.emit('passport-update', passport);
        broadcastFleetUpdates();
    });

    // =========================================================
    // 🚏 PRAGYA POOL — shared-ride matching & distance-proportional fare split
    // =========================================================
    safeOn(socket, 'request-pool-ride', (reqData) => {
        // 1) Try to match into an already-active pool trip heading the same way
        const candidateEntry = Object.entries(activePoolTrips).find(([driverSocketId, trip]) => {
            if (trip.stops.length >= POOL_MAX_PASSENGERS) return false;
            const driver = activeRiders[driverSocketId];
            if (!driver) return false;

            const distToPickup = calculateDistanceKm(driver.lat, driver.lng, reqData.pickupLat, reqData.pickupLng);
            if (distToPickup > POOL_MATCH_RADIUS_KM) return false;

            const finalStop = trip.stops[trip.stops.length - 1];
            const existingBearing = calculateBearingDeg(driver.lat, driver.lng, finalStop.dropoffLat, finalStop.dropoffLng);
            const newBearing = calculateBearingDeg(driver.lat, driver.lng, reqData.dropoffLat, reqData.dropoffLng);
            return angleDiffDeg(existingBearing, newBearing) <= POOL_BEARING_TOLERANCE_DEG;
        });

        if (candidateEntry) {
            const [driverSocketId, trip] = candidateEntry;
            const driver = activeRiders[driverSocketId];

            trip.stops.push({
                passengerSocketId: socket.id,
                passengerPhone: reqData.passengerPhone,
                pickupLat: reqData.pickupLat, pickupLng: reqData.pickupLng,
                dropoffLat: reqData.dropoffLat, dropoffLng: reqData.dropoffLng,
                boardedAtOdometerKm: null, droppedAtOdometerKm: null,
                status: 'WAITING_PICKUP'
            });

            io.to(driverSocketId).emit('pool-passenger-added', { trip, driverSocketId });
            persistPoolTrip(driver.plateNumber, trip);
            socket.emit('ride-request-response', {
                status: 'accepted', isPool: true,
                riderName: driver.fullName, plateNumber: driver.plateNumber, riderContact: driver.contact
            });
            return;
        }

        // 2) No compatible active pool trip — broadcast as a poolable dispatch to nearby idle drivers
        const nearbyIdleDrivers = Object.values(activeRiders).filter(r =>
            r.status === 'IDLE' && calculateDistanceKm(r.lat, r.lng, reqData.pickupLat, reqData.pickupLng) <= 3.0
        );

        if (nearbyIdleDrivers.length === 0) {
            socket.emit('ride-request-response', { status: 'declined', isPool: true, reason: 'No nearby drivers available for pooling right now.' });
            return;
        }

        nearbyIdleDrivers.forEach(driver => {
            io.to(driver.socketId).emit('incoming-ride-request', {
                ...reqData, passengerSocketId: socket.id, isPool: true
            });
        });
    });

    // Driver marks a specific pool passenger as picked up
    safeOn(socket, 'pool-mark-picked-up', ({ passengerSocketId }) => {
        const trip = activePoolTrips[socket.id];
        const rider = activeRiders[socket.id];
        if (!trip || !rider) return;
        const stop = trip.stops.find(s => s.passengerSocketId === passengerSocketId);
        if (!stop) return;

        stop.status = 'ONBOARD';
        stop.boardedAtOdometerKm = parseFloat(rider.engineHealth?.odometerKm || 0);
        persistPoolTrip(rider.plateNumber, trip);
        io.to(passengerSocketId).emit('ride-request-response', { status: 'accepted', isPool: true, boarded: true });
        io.to(socket.id).emit('pool-trip-updated', trip);
    });

    // Driver marks a specific pool passenger as dropped off — settles that passenger's fare share
    safeOn(socket, 'pool-mark-dropped-off', ({ passengerSocketId }) => {
        const trip = activePoolTrips[socket.id];
        const rider = activeRiders[socket.id];
        if (!trip || !rider) return;
        const stop = trip.stops.find(s => s.passengerSocketId === passengerSocketId);
        if (!stop) return;

        stop.status = 'DROPPED';
        stop.droppedAtOdometerKm = parseFloat(rider.engineHealth?.odometerKm || 0);

        // Settle this passenger's fare once the whole trip's total distance is known so far:
        // proportional to their own onboard distance vs. everyone's onboard distance to date.
        const settledStops = trip.stops.filter(s => s.droppedAtOdometerKm != null);
        const totalOnboardDistanceKm = settledStops.reduce((sum, s) => sum + Math.max(0.1, s.droppedAtOdometerKm - s.boardedAtOdometerKm), 0);
        const tripTotalDistanceKm = Math.max(0.5, rider.engineHealth?.odometerKm - trip.tripStartOdometerKm);

        const myDistance = Math.max(0.1, stop.droppedAtOdometerKm - stop.boardedAtOdometerKm);
        const myShareGhs = totalOnboardDistanceKm > 0
            ? ((myDistance / totalOnboardDistanceKm) * (BASE_FARE_GHS + tripTotalDistanceKm * RATE_PER_KM_GHS))
            : (BASE_FARE_GHS + tripTotalDistanceKm * RATE_PER_KM_GHS) / trip.stops.length;

        stop.fareShareGhs = Math.round(myShareGhs * 100) / 100;

        rider.shiftEarnings += stop.fareShareGhs;
        db.rideHistory.push({
            tripId: 'POOL-' + Date.now(),
            riderName: rider.fullName,
            plateNumber: rider.plateNumber,
            passengerPhone: stop.passengerPhone,
            fareGhs: stop.fareShareGhs,
            distanceDrivenKm: myDistance.toFixed(2),
            timestamp: new Date().toISOString(),
            isPool: true
        });
        saveDatabase(db);

        io.to(passengerSocketId).emit('passenger-trip-ended', {
            fareGhs: stop.fareShareGhs,
            plateNumber: rider.plateNumber,
            tripRecord: { riderName: rider.fullName, plateNumber: rider.plateNumber }
        });
        io.to(socket.id).emit('pool-trip-updated', trip);
        io.emit('rider-location-updated', rider);
        broadcastFleetUpdates();

        // If every stop is dropped, close out the pool trip
        if (trip.stops.every(s => s.status === 'DROPPED')) {
            const stats = ensureDriverStats(rider.plateNumber);
            stats.totalTrips += 1;
            recomputeBadge(stats);
            delete activePoolTrips[socket.id];
            persistPoolTrip(rider.plateNumber, null);
            rider.status = 'IDLE';
            io.emit('rider-location-updated', rider);
            broadcastFleetUpdates();
        } else {
            persistPoolTrip(rider.plateNumber, trip);
        }
    });

    // Driver accepting a pool-flagged incoming-ride-request creates the pool trip container
    safeOn(socket, 'respond-pool-ride-request', (resData) => {
        const rider = activeRiders[socket.id];
        if (!rider) return;

        if (resData.status === 'accepted') {
            rider.status = 'EN_ROUTE';
            activePoolTrips[socket.id] = {
                driverSocketId: socket.id,
                plateNumber: rider.plateNumber,
                tripStartOdometerKm: parseFloat(rider.engineHealth?.odometerKm || 0),
                stops: [{
                    passengerSocketId: resData.passengerSocketId,
                    passengerPhone: resData.passengerPhone,
                    pickupLat: resData.pickupLat, pickupLng: resData.pickupLng,
                    dropoffLat: resData.dropoffLat, dropoffLng: resData.dropoffLng,
                    boardedAtOdometerKm: null, droppedAtOdometerKm: null,
                    status: 'WAITING_PICKUP'
                }]
            };
            persistPoolTrip(rider.plateNumber, activePoolTrips[socket.id]);
            io.emit('rider-location-updated', rider);
            broadcastFleetUpdates();
        }

        if (resData.passengerSocketId) {
            io.to(resData.passengerSocketId).emit('ride-request-response', {
                status: resData.status, isPool: true,
                riderName: rider.fullName, plateNumber: rider.plateNumber, riderContact: rider.contact
            });
        }
    });

    // =========================================================
    // ⭐ DRIVER RATINGS & LEADERBOARD
    // =========================================================
    safeOn(socket, 'submit-driver-rating', ({ plateNumber, rating }) => {
        if (!plateNumber || typeof rating !== 'number' || rating < 1 || rating > 5) return;
        const stats = ensureDriverStats(plateNumber.toUpperCase());
        stats.ratingSum += rating;
        stats.ratingCount += 1;
        recomputeBadge(stats);
        saveDatabase(db);
    });

    safeOn(socket, 'request-leaderboard', (fleetCode) => {
        socket.emit('leaderboard-update', buildLeaderboard(fleetCode));
    });

    safeOn(socket, 'request-driver-stats', (plateNumber) => {
        if (!plateNumber) return;
        socket.emit('driver-stats-update', db.driverStats[plateNumber.toUpperCase()] || null);
    });

    // ---- Roadside assistance (mechanic <-> driver) ----

    safeOn(socket, 'register-workshop', (workshopData) => {
        if (!workshopData || !workshopData.shopName) return;
        activeWorkshops[socket.id] = { ...workshopData, socketId: socket.id, isOnline: true };
        safetyMonitorSockets.add(socket.id); // workshops also act as safety-alert monitors
        console.log(`[Workshop Online] ${workshopData.shopName}`);
    });

    safeOn(socket, 'workshop-status-update', (data) => {
        if (activeWorkshops[socket.id]) {
            activeWorkshops[socket.id].isOnline = !!(data && data.isOnline);
        }
    });

    safeOn(socket, 'driver-sos-alert', (sosData) => {
        const rider = activeRiders[socket.id];
        const payload = {
            username: sosData.username || rider?.username,
            fullName: sosData.fullName || rider?.fullName,
            contact: sosData.contact || rider?.contact,
            plateNumber: sosData.plateNumber || rider?.plateNumber,
            issue: sosData.issue || 'Breakdown reported',
            lat: sosData.lat != null ? sosData.lat : rider?.lat,
            lng: sosData.lng != null ? sosData.lng : rider?.lng
        };
        // Broadcast to all currently-online registered workshops
        Object.keys(activeWorkshops).forEach(wsSocketId => {
            if (activeWorkshops[wsSocketId].isOnline !== false) {
                io.to(wsSocketId).emit('driver-sos-alert', payload);
            }
        });
    });

    // =========================================================
    // 🆘 PASSENGER PANIC BUTTON — logged to operator dashboards (owner/mechanic)
    // =========================================================
    safeOn(socket, 'passenger-sos-alert', (sosData) => {
        const alertRecord = {
            alertId: 'SOS-' + Date.now(),
            passengerPhone: sosData.passengerPhone || 'Unknown',
            plateNumber: sosData.plateNumber || null,
            driverName: sosData.driverName || null,
            lat: sosData.lat, lng: sosData.lng,
            timestamp: new Date().toISOString()
        };
        db.safetyAlerts.push(alertRecord);
        saveDatabase(db);

        // Every connected owner/fleet dashboard and every registered workshop sees it live
        safetyMonitorSockets.forEach(monitorSocketId => {
            io.to(monitorSocketId).emit('passenger-sos-alert', alertRecord);
        });
        console.log(`🆘 PASSENGER SOS logged: ${alertRecord.alertId}`);
    });

    // ---- Cleanup ----

    safeOn(socket, 'disconnect', () => {
        if (activeRiders[socket.id]) {
            delete activeRiders[socket.id];
            delete activePoolTrips[socket.id];
            io.emit('rider-disconnected', socket.id);
            broadcastFleetUpdates();
        }
        if (fleetOwnerSockets[socket.id]) {
            delete fleetOwnerSockets[socket.id];
        }
        if (activeWorkshops[socket.id]) {
            delete activeWorkshops[socket.id];
        }
        safetyMonitorSockets.delete(socket.id);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 PragyaLink Persistent Server running on port ${PORT}`));
