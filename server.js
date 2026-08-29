const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);

// CORS: configurable via ALLOWED_ORIGINS env var (comma-separated), since this app can be
// deployed at any domain. Defaults to wide-open for local/demo use, with a clear warning —
// set this before exposing the server publicly.
const allowedOrigins = process.env.ALLOWED_ORIGINS
    ? process.env.ALLOWED_ORIGINS.split(',').map(o => o.trim())
    : '*';
if (allowedOrigins === '*') {
    console.warn('⚠️  CORS is wide open (origin: "*"). Set the ALLOWED_ORIGINS env var (comma-separated) before deploying publicly.');
}
const io = new Server(server, { cors: { origin: allowedOrigins } });

app.use(express.static(path.join(__dirname, 'public')));
// The `verify` hook stashes the exact raw bytes of every request body onto req.rawBody.
// Needed specifically for the Paystack webhook below — its signature is computed over the
// raw body, and re-serializing the already-parsed JSON can produce different bytes than
// what Paystack actually signed, causing verification to fail even on a legitimate call.
app.use(express.json({ verify: (req, res, buf) => { req.rawBody = buf; } }));

// =========================================================
// 📦 PERSISTENT JSON FILE STORAGE
// =========================================================
const DB_FILE = path.join(__dirname, 'database.json');

function loadDatabase() {
    if (!fs.existsSync(DB_FILE)) {
        const initialData = {
            servicePassports: {}, rideHistory: [], registeredFleet: {},
            driverStats: {}, safetyAlerts: [], persistedPoolTrips: {},
            transactions: [], driverPayouts: {}, referralCodes: {}, referrals: [], walletCredits: {},
            payoutRecipientCodes: {}, ownerPayouts: {}, expenses: []
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
        parsed.transactions = parsed.transactions || [];
        parsed.driverPayouts = parsed.driverPayouts || {};
        parsed.referralCodes = parsed.referralCodes || {};
        parsed.referrals = parsed.referrals || [];
        parsed.walletCredits = parsed.walletCredits || {};
        parsed.payoutRecipientCodes = parsed.payoutRecipientCodes || {};
        parsed.ownerPayouts = parsed.ownerPayouts || {};
        parsed.expenses = parsed.expenses || [];
        return parsed;
    } catch (e) {
        console.error("Error reading database.json, re-initializing...", e);
        return {
            servicePassports: {}, rideHistory: [], registeredFleet: {}, driverStats: {}, safetyAlerts: [], persistedPoolTrips: {},
            transactions: [], driverPayouts: {}, referralCodes: {}, referrals: [], walletCredits: {}, payoutRecipientCodes: {},
            ownerPayouts: {}, expenses: []
        };
    }
}

// Debounced async save: every mutation site below just marks the DB dirty and this
// flushes at most once per DB_SAVE_DEBOUNCE_MS. Fixes a real bottleneck — every GPS tick
// that moved the odometer, every trip, every rating was previously a synchronous
// writeFileSync of the WHOLE (ever-growing) database.json, blocking Node's single thread
// on every single one. Still atomic (temp file + rename), just no longer on the hot path.
let dbDirty = false;
let dbSaveTimer = null;
const DB_SAVE_DEBOUNCE_MS = 1500;

function saveDatabase(data) {
    dbDirty = true;
    if (dbSaveTimer) return; // a flush is already scheduled, this write will be included in it
    dbSaveTimer = setTimeout(() => flushDatabaseAsync(data), DB_SAVE_DEBOUNCE_MS);
}

function flushDatabaseAsync(data) {
    dbSaveTimer = null;
    if (!dbDirty) return;
    dbDirty = false;
    const tmpFile = DB_FILE + '.tmp';
    const payload = JSON.stringify(data, null, 2);
    fs.writeFile(tmpFile, payload, (err) => {
        if (err) { console.error('❌ Failed to write temp db file:', err); dbDirty = true; return; }
        fs.rename(tmpFile, DB_FILE, (err2) => {
            if (err2) { console.error('❌ Failed to rename temp db file:', err2); dbDirty = true; }
        });
    });
}

// Best-effort synchronous flush so a graceful shutdown never drops the last ~1.5s of writes
function flushDatabaseSync() {
    if (!dbDirty) return;
    dbDirty = false;
    try {
        const tmpFile = DB_FILE + '.tmp';
        fs.writeFileSync(tmpFile, JSON.stringify(db, null, 2));
        fs.renameSync(tmpFile, DB_FILE);
    } catch (e) {
        console.error('❌ Failed final sync flush on shutdown:', e);
    }
}
process.on('SIGINT', () => { flushDatabaseSync(); process.exit(0); });
process.on('SIGTERM', () => { flushDatabaseSync(); process.exit(0); });

let db = loadDatabase();

// =========================================================
// 🛡️ CRASH SAFETY
// =========================================================
// Wraps every socket event handler so a malformed payload or unexpected null throws
// inside ONE handler for ONE client, instead of crashing the process for every
// connected driver, passenger, and dashboard. Use this instead of socket.on() directly.
// Also rate-limits per socket per event — a buggy or malicious client spamming, say,
// telemetry updates or SOS alerts gets silently throttled instead of hammering the server
// or flooding every connected dashboard with broadcasts.
const RATE_LIMITS = {
    'update-rider-location': { max: 4, windowMs: 1000 },
    'driver-go-online': { max: 5, windowMs: 5000 },
    'request-ride': { max: 5, windowMs: 10000 },
    'request-pool-ride': { max: 5, windowMs: 10000 },
    'submit-driver-rating': { max: 3, windowMs: 10000 },
    'passenger-sos-alert': { max: 3, windowMs: 30000 },
    'driver-sos-alert': { max: 3, windowMs: 30000 },
    'initiate-payment': { max: 5, windowMs: 30000 },
    'mark-paid-cash': { max: 5, windowMs: 30000 },
    'submit-referral-code': { max: 3, windowMs: 30000 },
    'log-expense': { max: 10, windowMs: 30000 },
    'register-owner-payout': { max: 5, windowMs: 30000 }
};
const DEFAULT_RATE_LIMIT = { max: 20, windowMs: 5000 };

function safeOn(socket, eventName, handler) {
    const limit = RATE_LIMITS[eventName] || DEFAULT_RATE_LIMIT;
    let windowStart = Date.now();
    let count = 0;

    socket.on(eventName, (...args) => {
        const now = Date.now();
        if (now - windowStart > limit.windowMs) { windowStart = now; count = 0; }
        count++;
        if (count > limit.max) {
            console.warn(`🚦 Rate limit hit: '${eventName}' from ${socket.id} — dropped`);
            return;
        }
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
            badge: 'New Driver',
            qualifiedReferrals: 0
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
    // Free recognition on top of the tier badge — costs nothing, reuses the existing
    // badge/leaderboard display everywhere a driver's identity already shows up.
    stats.isCommunityBuilder = (stats.qualifiedReferrals || 0) >= 3;
}

// =========================================================
// 💳 MOBILE MONEY (MoMo) — Paystack integration (Ghana)
// =========================================================
// Collect-then-disburse: the passenger pays into the platform's own merchant account,
// the platform takes its commission, then pays the driver out separately. This is the
// only model that actually supports Pool (each passenger pays their own share
// independently) and matches how every real ride-hail platform handles payment.
//
// Falls back to a SIMULATED mode automatically if PAYSTACK_SECRET_KEY isn't set, so the
// app still works end-to-end for local development without real credentials. Set that
// env var with your Paystack secret key (test or live) to go live — nothing else in the
// app needs to change.
const PAYSTACK_SECRET_KEY = process.env.PAYSTACK_SECRET_KEY || null;
const PAYSTACK_BASE_URL = 'https://api.paystack.co';
const PLATFORM_COMMISSION_RATE = 0.15;

if (!PAYSTACK_SECRET_KEY) {
    console.warn('⚠️  PAYSTACK_SECRET_KEY not set — MoMo payments are running in SIMULATED mode. Set that env var (your Paystack secret key) to charge/pay out for real.');
}

// Maps our network selector values (used across rider.html / passenger.html) to
// Paystack's own provider codes (for charges) and bank codes (for transfer recipients) —
// these are two different code sets for the same three telcos, confirmed against
// Paystack's current docs.
const NETWORK_MAP = {
    MTN: { chargeProvider: 'mtn', bankCode: 'MTN' },
    VODAFONE: { chargeProvider: 'vod', bankCode: 'VOD' },
    AIRTELTIGO: { chargeProvider: 'atl', bankCode: 'ATL' }
};

async function paystackRequest(method, endpoint, body) {
    const res = await fetch(`${PAYSTACK_BASE_URL}${endpoint}`, {
        method,
        headers: {
            'Authorization': `Bearer ${PAYSTACK_SECRET_KEY}`,
            'Content-Type': 'application/json'
        },
        body: body ? JSON.stringify(body) : undefined
    });
    const json = await res.json();
    if (!res.ok || json.status === false) {
        throw new Error(json.message || `Paystack API error (HTTP ${res.status})`);
    }
    return json.data;
}

// Requests a MoMo charge. IMPORTANT: Ghana MoMo charges complete OFFLINE — this call
// only confirms the approval PROMPT was sent to the payer's phone, not that they actually
// approved it. The real result (approved / declined / timed out) arrives later via the
// /api/payment/webhook route below, which is why onResult() here reports "request
// accepted" (awaitingWebhook: true) rather than a final outcome in real mode. Only the
// simulated fallback (no API key set) resolves instantly, matching how the earlier stub
// behaved for local testing.
function initiateMomoCharge(payerId, network, amountGhs, onResult) {
    if (!PAYSTACK_SECRET_KEY) {
        console.log(`[MoMo SIMULATED] Charging ${amountGhs.toFixed(2)} GHS from ${payerId} via ${network}...`);
        setTimeout(() => onResult({ success: true, reference: 'SIM-' + Date.now(), awaitingWebhook: false }), 2200);
        return;
    }

    const mapping = NETWORK_MAP[network] || NETWORK_MAP.MTN;
    const reference = 'PL-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8);

    paystackRequest('POST', '/charge', {
        email: `${payerId.replace(/[^0-9a-zA-Z]/g, '')}@pragyalink.local`, // Paystack requires an email even for MoMo charges — synthesized since passengers don't have one
        amount: Math.round(amountGhs * 100), // GHS -> pesewas (Paystack's currency subunit)
        currency: 'GHS',
        reference,
        mobile_money: { phone: payerId, provider: mapping.chargeProvider }
    }).then(data => {
        onResult({ success: true, reference: data.reference || reference, awaitingWebhook: true });
    }).catch(err => {
        console.error('❌ Paystack charge request failed:', err.message);
        onResult({ success: false, reference, awaitingWebhook: false });
    });
}

// Pays out to any MoMo number — used for both driver earnings and (new) fleet owner
// settlement shares, since the mechanics are identical either way. Creates (and caches) a
// transfer recipient per phone number the first time, then initiates the transfer. In
// Paystack test mode, transfers auto-succeed immediately (their documented behavior) —
// real production transfers go through actual processing.
async function disburseMomoPayout(momoNumber, network, amountGhs, label) {
    if (!momoNumber) {
        console.warn(`[MoMo] No payout number on file for ${label} — skipping payout of ${amountGhs.toFixed(2)} GHS`);
        return;
    }
    if (amountGhs <= 0) return;
    if (!PAYSTACK_SECRET_KEY) {
        console.log(`[MoMo SIMULATED] Paying out ${amountGhs.toFixed(2)} GHS to ${label} ${momoNumber} via ${network}`);
        return;
    }

    try {
        const mapping = NETWORK_MAP[network] || NETWORK_MAP.MTN;
        db.payoutRecipientCodes = db.payoutRecipientCodes || {};
        let recipientCode = db.payoutRecipientCodes[momoNumber];

        if (!recipientCode) {
            const recipient = await paystackRequest('POST', '/transferrecipient', {
                type: 'mobile_money',
                name: `PragyaLink ${label} ${momoNumber}`,
                account_number: momoNumber,
                bank_code: mapping.bankCode,
                currency: 'GHS'
            });
            recipientCode = recipient.recipient_code;
            db.payoutRecipientCodes[momoNumber] = recipientCode;
            saveDatabase(db);
        }

        await paystackRequest('POST', '/transfer', {
            source: 'balance',
            reason: `PragyaLink ${label} settlement`,
            amount: Math.round(amountGhs * 100),
            recipient: recipientCode,
            reference: 'PAYOUT-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8)
        });
        console.log(`✅ Paid out ${amountGhs.toFixed(2)} GHS to ${label} ${momoNumber}`);
    } catch (err) {
        console.error(`❌ Payout failed for ${label} ${momoNumber}:`, err.message);
    }
}

function recordTransaction(tx) {
    const record = { id: 'TXN-' + Date.now() + '-' + Math.random().toString(36).slice(2, 7), timestamp: new Date().toISOString(), ...tx };
    db.transactions.push(record);
    saveDatabase(db);
    return record;
}

// =========================================================
// 🤝 WORK & PAY — automated daily settlement between fleet owner and driver
// =========================================================
// Ghana's tricycle economy commonly runs on a daily rental agreement: the driver owes the
// owner a fixed cash target each day, everything past that is the driver's. That system
// breaks down because it's all manual — the owner has no way to verify what was actually
// collected. This automates it, with one hard constraint that has to be respected: the
// platform can only automatically split and disburse money it actually holds custody of.
// That's true for MoMo-collected fares. It is NOT true for cash — a passenger paying cash
// hands it directly to the driver, the platform never touches it, so there is nothing to
// electronically "send" the owner. Cash fares still update the tracked/visible progress
// (which is what actually solves "the owner can't verify what was collected"), they just
// don't trigger an automatic payout — that portion still has to be settled the old way,
// or via the driver sending the owner's share separately.
const DEFAULT_OWNER_SHARE_RATE = 0.6; // 60% owner / 40% driver until the daily target is met

function todayDateString() {
    // Ghana is UTC+0 year-round (no DST), so a UTC calendar day boundary genuinely lines
    // up with a Ghana calendar day — this isn't a shortcut that happens to work elsewhere,
    // it's correct specifically for this app's market.
    return new Date().toISOString().slice(0, 10);
}

function ensureWorkAndPayFields(passport) {
    if (passport.ownerShareRate == null) passport.ownerShareRate = DEFAULT_OWNER_SHARE_RATE;
    if (passport.collectedTodayGhs == null) passport.collectedTodayGhs = 0;
    if (passport.workAndPayResetDate == null) passport.workAndPayResetDate = todayDateString();
}

function resetWorkAndPayIfNewDay(passport) {
    const today = todayDateString();
    if (passport.workAndPayResetDate !== today) {
        passport.collectedTodayGhs = 0;
        passport.workAndPayResetDate = today;
    }
}

function findDriverSocketIdByPlate(plateNumber) {
    const entry = Object.values(activeRiders).find(r => r.plateNumber === plateNumber);
    return entry ? entry.socketId : null;
}

// Called from every payment-completion path (MoMo webhook success, wallet-credit instant
// success, cash) — the single place that decides the owner/driver split and, for
// MoMo-collected money only, actually disburses it. Returns the split for the caller to
// attach to the transaction record. Also the ONLY place shiftEarnings gets credited —
// trip-end handlers used to credit the full gross fare immediately, before any split was
// even computed, which meant a fleet driver's "Shift Earnings" showed gross throughput
// instead of their actual take-home. Fixed by moving the credit here, using the real
// settled share, for both fleet-linked AND independent vehicles.
function settleFareForVehicle(plateNumber, amountGhs, method) {
    const passport = ensurePassport(plateNumber);
    const ownerCode = passport.ownerCode;
    const driverSocketId = findDriverSocketIdByPlate(plateNumber);
    const liveRider = driverSocketId ? activeRiders[driverSocketId] : null;

    // Independent / owner-operator vehicles have no fleet owner to split with — the
    // driver keeps the full fare
    if (!ownerCode) {
        if (liveRider) liveRider.shiftEarnings = +(liveRider.shiftEarnings + amountGhs).toFixed(2);
        if (driverSocketId) {
            io.to(driverSocketId).emit('shift-earnings-update', { shiftEarnings: liveRider ? liveRider.shiftEarnings : amountGhs });
        }
        return { ownerShareGhs: 0, driverShareGhs: amountGhs, dailyTargetGhs: null, collectedTodayGhs: null, targetMet: null };
    }

    ensureWorkAndPayFields(passport);
    resetWorkAndPayIfNewDay(passport);

    const dailyTargetGhs = passport.dailyLeaseTarget || 0;
    const remainingToTarget = Math.max(0, dailyTargetGhs - passport.collectedTodayGhs);

    let ownerShareGhs = 0;
    if (remainingToTarget > 0 && dailyTargetGhs > 0) {
        const proposedOwnerShare = +(amountGhs * passport.ownerShareRate).toFixed(2);
        ownerShareGhs = Math.min(proposedOwnerShare, remainingToTarget); // never overshoot the daily target
    }
    const driverShareGhs = +(amountGhs - ownerShareGhs).toFixed(2);

    if (liveRider) liveRider.shiftEarnings = +(liveRider.shiftEarnings + driverShareGhs).toFixed(2);

    passport.collectedTodayGhs = +(passport.collectedTodayGhs + ownerShareGhs).toFixed(2);
    const targetMet = dailyTargetGhs > 0 && passport.collectedTodayGhs >= dailyTargetGhs;
    saveDatabase(db);

    // Only MoMo-collected fares can be electronically disbursed — see note above
    if (method !== 'CASH') {
        const netOwnerShare = +(ownerShareGhs * (1 - PLATFORM_COMMISSION_RATE)).toFixed(2);
        const netDriverShare = +(driverShareGhs * (1 - PLATFORM_COMMISSION_RATE)).toFixed(2);
        const ownerPayout = db.ownerPayouts[ownerCode];
        const driverPayout = db.driverPayouts[plateNumber];
        if (netOwnerShare > 0) disburseMomoPayout(ownerPayout?.momoNumber, ownerPayout?.network, netOwnerShare, 'owner');
        if (netDriverShare > 0) disburseMomoPayout(driverPayout?.momoNumber, driverPayout?.network, netDriverShare, 'driver');
    }

    // Live-update both sides — the owner's dashboard already gets this via the existing
    // targeted fleet broadcast (passport is embedded in each vehicle's payload), the
    // driver's console needs its own push since it isn't part of that mechanism.
    broadcastFleetUpdates(ownerCode);
    if (driverSocketId) {
        io.to(driverSocketId).emit('work-and-pay-update', {
            plateNumber, dailyTargetGhs, collectedTodayGhs: passport.collectedTodayGhs,
            progressPct: dailyTargetGhs > 0 ? Math.min(100, Math.round((passport.collectedTodayGhs / dailyTargetGhs) * 100)) : 0,
            targetMet
        });
        io.to(driverSocketId).emit('shift-earnings-update', { shiftEarnings: liveRider ? liveRider.shiftEarnings : driverShareGhs });
    }

    return { ownerShareGhs, driverShareGhs, dailyTargetGhs, collectedTodayGhs: passport.collectedTodayGhs, targetMet };
}

// Fires when a driver ends their shift (intentionally, or via disconnect) without having
// met their vehicle's daily Work & Pay target — the automated version of the owner having
// to notice and chase this down manually, which was the original pain point.
function checkAndEmitShortfallAlert(rider) {
    if (!rider || !rider.plateNumber) return;
    const passport = db.servicePassports[rider.plateNumber];
    if (!passport || !passport.ownerCode) return; // independent driver — no target to miss

    ensureWorkAndPayFields(passport);
    resetWorkAndPayIfNewDay(passport);

    const target = passport.dailyLeaseTarget || 0;
    if (target > 0 && passport.collectedTodayGhs < target) {
        const shortfallGhs = +(target - passport.collectedTodayGhs).toFixed(2);
        Object.keys(fleetOwnerSockets).forEach(socketId => {
            if (fleetOwnerSockets[socketId] === passport.ownerCode) {
                io.to(socketId).emit('quota-shortfall-alert', {
                    plateNumber: rider.plateNumber, driverName: rider.fullName,
                    collectedTodayGhs: passport.collectedTodayGhs, targetGhs: target, shortfallGhs,
                    timestamp: new Date().toISOString()
                });
            }
        });
    }
}

// =========================================================
// 🎁 REFERRAL PROGRAM
// =========================================================
// Layered on purpose: a small instant credit on signup (low fraud risk — the amount is
// small enough that gaming it isn't worth the effort) plus a bigger milestone bonus that
// only pays out once the referred person proves they're a real, repeat user. Paying the
// big reward for a signup alone is exactly the fraud pattern (bot signups, one person
// making a pile of "referred" accounts) this two-tier structure exists to avoid.
const REFERRAL_INSTANT_CREDIT_GHS = 2;
const REFERRAL_MILESTONE_BONUS_REFERRER_GHS = 10;
const REFERRAL_MILESTONE_BONUS_REFEREE_GHS = 5;
const REFERRAL_DRIVER_TRIP_THRESHOLD = 10;

function generateReferralCode(identifier) {
    if (!identifier) return null;
    if (db.referralCodes[identifier]) return db.referralCodes[identifier];
    const code = 'PL' + Math.random().toString(36).slice(2, 7).toUpperCase();
    db.referralCodes[identifier] = code;
    saveDatabase(db);
    return code;
}

function findReferrerByCode(code) {
    if (!code) return null;
    const normalized = code.trim().toUpperCase();
    return Object.keys(db.referralCodes).find(id => db.referralCodes[id] === normalized) || null;
}

function creditWallet(identifier, amountGhs) {
    if (!identifier) return;
    db.walletCredits[identifier] = (db.walletCredits[identifier] || 0) + amountGhs;
}

// Called once, when the referee first enters a code (registration / ownership setup)
function registerReferral(referrerCode, refereeId, type) {
    if (!referrerCode || !refereeId) return null;
    const referrerId = findReferrerByCode(referrerCode);
    if (!referrerId || referrerId === refereeId) return null; // invalid code or self-referral
    if (db.referrals.some(r => r.refereeId === refereeId)) return null; // already referred once

    const referral = { referrerId, refereeId, type, status: 'pending', createdAt: new Date().toISOString() };
    db.referrals.push(referral);
    creditWallet(refereeId, REFERRAL_INSTANT_CREDIT_GHS);
    saveDatabase(db);
    return referral;
}

// Called wherever a referee's activity might cross the qualification bar — a driver's
// Nth completed trip, or a passenger's first successfully paid trip.
function checkReferralQualification(refereeId) {
    const referral = db.referrals.find(r => r.refereeId === refereeId && r.status === 'pending');
    if (!referral) return;

    let qualifies = false;
    if (referral.type === 'driver') {
        const stats = db.driverStats[refereeId];
        qualifies = !!stats && stats.totalTrips >= REFERRAL_DRIVER_TRIP_THRESHOLD;
    } else if (referral.type === 'passenger') {
        qualifies = db.transactions.some(t => t.payerId === refereeId && t.status === 'SUCCESS');
    }
    if (!qualifies) return;

    creditWallet(referral.referrerId, REFERRAL_MILESTONE_BONUS_REFERRER_GHS);
    creditWallet(referral.refereeId, REFERRAL_MILESTONE_BONUS_REFEREE_GHS);
    referral.status = 'rewarded';
    referral.rewardedAt = new Date().toISOString();

    if (referral.type === 'driver') {
        const referrerStats = ensureDriverStats(referral.referrerId);
        referrerStats.qualifiedReferrals = (referrerStats.qualifiedReferrals || 0) + 1;
        recomputeBadge(referrerStats);
    }
    saveDatabase(db);
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

    // Recent expenses only (not the full history) — this payload gets rebuilt and resent
    // on every relevant driver telemetry tick, so an ever-growing full history here would
    // reintroduce the same "resending the whole growing dataset constantly" problem
    // already fixed elsewhere. Full history is available on-demand via the CSV export route.
    const recentExpenses = db.expenses.filter(e => e.ownerCode === fleetCode).slice(-50);

    io.to(ownerSocketId).emit('current-riders', {
        activeDrivers: liveFleet,
        registeredVehicles: savedFleetVehicles,
        expenses: recentExpenses,
        fleetCode
    });
}

function broadcastFleetUpdates(fleetCode) {
    // When the affected fleet is known, only recompute+resend for owners watching THAT
    // fleet — not every connected owner/fleet dashboard on every single driver's every
    // telemetry tick. Falls back to updating everyone only when the caller genuinely
    // doesn't know which fleet was affected.
    if (fleetCode) {
        Object.keys(fleetOwnerSockets).forEach(socketId => {
            if (fleetOwnerSockets[socketId] === fleetCode) emitFleetToOwner(socketId);
        });
    } else {
        Object.keys(fleetOwnerSockets).forEach(socketId => emitFleetToOwner(socketId));
    }
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
// Map tile style — defaults to a free, no-signup OSM style so the app works out of the
// box, but detail/label density improves noticeably with a free (no credit card
// required) MapTiler or Stadia Maps key. Confirmed both currently offer genuine no-card
// free tiers and are drop-in MapLibre style-URL swaps — no code changes needed beyond
// this, just set the env var. Injected into each map-bearing page at serve time via a
// placeholder substitution, keeping the client-side map init fully synchronous (no
// runtime fetch/await needed, so nothing about the existing page structure has to change).
const DEFAULT_MAP_STYLE_URL = 'https://tiles.openfreemap.org/styles/liberty';
const MAPTILER_KEY = process.env.MAPTILER_KEY || null;
const STADIA_API_KEY = process.env.STADIA_API_KEY || null;
const MAP_STYLE_PLACEHOLDER = '__PRAGYA_MAP_STYLE_URL__';

function resolveMapStyleUrl() {
    if (MAPTILER_KEY) return `https://api.maptiler.com/maps/streets-v2/style.json?key=${MAPTILER_KEY}`;
    if (STADIA_API_KEY) return `https://tiles.stadiamaps.com/styles/alidade_smooth.json?api_key=${STADIA_API_KEY}`;
    return DEFAULT_MAP_STYLE_URL;
}
if (!MAPTILER_KEY && !STADIA_API_KEY) {
    console.log('ℹ️  Using the free OpenFreeMap style (no key set). Set MAPTILER_KEY or STADIA_API_KEY for richer map detail — both offer genuine free tiers with no credit card.');
}

function sendHtmlWithMapStyle(res, filePath) {
    try {
        const html = fs.readFileSync(filePath, 'utf8').split(MAP_STYLE_PLACEHOLDER).join(resolveMapStyleUrl());
        res.send(html);
    } catch (e) {
        console.error(`❌ Failed to template ${filePath}, serving as-is:`, e.message);
        res.sendFile(filePath);
    }
}

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'views', 'index.html')));
app.get('/passenger', (req, res) => sendHtmlWithMapStyle(res, path.join(__dirname, 'views', 'passenger.html')));
app.get('/rider', (req, res) => sendHtmlWithMapStyle(res, path.join(__dirname, 'views', 'rider.html')));
app.get('/mechanic', (req, res) => sendHtmlWithMapStyle(res, path.join(__dirname, 'views', 'mechanic.html')));
app.get('/owner', (req, res) => sendHtmlWithMapStyle(res, path.join(__dirname, 'views', 'owner.html')));
app.get('/fleet', (req, res) => sendHtmlWithMapStyle(res, path.join(__dirname, 'views', 'fleet.html')));

app.get('/api/passenger/history/:phone', (req, res) => {
    const phone = req.params.phone;
    const history = db.rideHistory.filter(ride => ride.passengerPhone === phone);
    res.json(history);
});

// Automated yield report — per-vehicle gross revenue, owner net share, driver net share,
// logged expenses, and net profit to the owner, exported as CSV (opens cleanly in Excel/
// Sheets, and is what banks/loan providers actually want for a financial statement —
// more so than a PDF). ?range=daily (today) or weekly (last 7 days), defaults to weekly.
app.get('/api/owner/:ownerCode/report.csv', (req, res) => {
    const ownerCode = req.params.ownerCode.trim();
    const range = req.query.range === 'daily' ? 'daily' : 'weekly';

    const now = Date.now();
    const rangeStartMs = range === 'daily' ? now - 24 * 60 * 60 * 1000 : now - 7 * 24 * 60 * 60 * 1000;

    const fleetPlates = new Set((db.registeredFleet[ownerCode] || []).map(v => v.plateNumber));

    const rows = {}; // plateNumber -> aggregated row
    fleetPlates.forEach(plate => {
        rows[plate] = { plateNumber: plate, grossRevenueGhs: 0, ownerShareGhs: 0, driverShareGhs: 0, expensesGhs: 0, tripCount: 0 };
    });

    db.transactions.forEach(tx => {
        if (tx.status !== 'SUCCESS' || !fleetPlates.has(tx.plateNumber)) return;
        if (new Date(tx.timestamp).getTime() < rangeStartMs) return;
        const row = rows[tx.plateNumber];
        row.grossRevenueGhs += tx.amountGhs || 0;
        row.ownerShareGhs += tx.ownerShareGhs || 0;
        row.driverShareGhs += tx.driverShareGhs || 0;
        row.tripCount += 1;
    });

    db.expenses.forEach(exp => {
        if (exp.ownerCode !== ownerCode || !fleetPlates.has(exp.plateNumber)) return;
        if (new Date(exp.timestamp).getTime() < rangeStartMs) return;
        if (rows[exp.plateNumber]) rows[exp.plateNumber].expensesGhs += exp.amountGhs || 0;
    });

    const header = ['Plate Number', 'Trips', 'Gross Revenue (GHS)', 'Owner Share (GHS)', 'Driver Share (GHS)', 'Expenses (GHS)', 'Net Profit to Owner (GHS)'];
    const lines = [header.join(',')];
    let totals = { grossRevenueGhs: 0, ownerShareGhs: 0, driverShareGhs: 0, expensesGhs: 0, tripCount: 0 };

    // Guards against CSV formula injection (a cell starting with =, +, -, or @ can execute
    // as a formula when opened in Excel/Sheets) — cheap to add, and this file is explicitly
    // meant to be handed to banks/loan providers, so it's worth doing properly.
    function csvSafe(value) {
        const str = String(value);
        return /^[=+\-@]/.test(str) ? `'${str}` : str;
    }

    Object.values(rows).forEach(r => {
        const netProfit = r.ownerShareGhs - r.expensesGhs;
        lines.push([
            csvSafe(r.plateNumber), r.tripCount,
            r.grossRevenueGhs.toFixed(2), r.ownerShareGhs.toFixed(2), r.driverShareGhs.toFixed(2),
            r.expensesGhs.toFixed(2), netProfit.toFixed(2)
        ].join(','));
        totals.grossRevenueGhs += r.grossRevenueGhs;
        totals.ownerShareGhs += r.ownerShareGhs;
        totals.driverShareGhs += r.driverShareGhs;
        totals.expensesGhs += r.expensesGhs;
        totals.tripCount += r.tripCount;
    });

    lines.push([
        'TOTAL', totals.tripCount,
        totals.grossRevenueGhs.toFixed(2), totals.ownerShareGhs.toFixed(2), totals.driverShareGhs.toFixed(2),
        totals.expensesGhs.toFixed(2), (totals.ownerShareGhs - totals.expensesGhs).toFixed(2)
    ].join(','));

    const csv = lines.join('\n');
    const filename = `pragyalink-${ownerCode}-${range}-report-${todayDateString()}.csv`;
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(csv);
});

// Finalizes a payment exactly once, however it was triggered (real webhook or the stub's
// simulated callback) — idempotent by design, since payment gateways commonly retry or
// duplicate webhook delivery.
function finalizePayment(reference, status) {
    const tx = db.transactions.find(t => t.reference === reference);
    if (!tx || tx.status !== 'PENDING') return null; // unknown reference, or already settled

    tx.status = status === 'SUCCESS' ? 'SUCCESS' : 'FAILED';
    tx.settledAt = new Date().toISOString();

    if (tx.status === 'SUCCESS') {
        checkReferralQualification(tx.payerId);
        const settlement = settleFareForVehicle(tx.plateNumber, tx.amountGhs, tx.method);
        tx.ownerShareGhs = settlement.ownerShareGhs;
        tx.driverShareGhs = settlement.driverShareGhs;
    }
    saveDatabase(db);

    if (tx.payerSocketId) {
        io.to(tx.payerSocketId).emit('payment-status-update', {
            tripId: tx.tripId, status: tx.status, reference,
            amountGhs: tx.amountGhs, amountDue: tx.amountDue, creditApplied: tx.creditApplied
        });
    }
    return tx;
}

// Paystack calls this URL when a charge (or transfer) reaches a final state — this is
// the ONLY authoritative source of truth for whether a Ghana MoMo charge actually got
// approved, since those charges complete offline on the payer's phone. Signature
// verification is real: HMAC-SHA512 of the raw request body, keyed with your Paystack
// secret key, compared against the x-paystack-signature header — confirmed against
// Paystack's current docs. Falls back to accepting unverified calls ONLY when no secret
// key is configured (simulated/local-dev mode) — do not run this in that state with a
// public URL, since anyone who finds it could then fabricate a "payment succeeded" call.
app.post('/api/payment/webhook', (req, res) => {
    if (PAYSTACK_SECRET_KEY) {
        const signature = req.headers['x-paystack-signature'];
        const expectedHash = crypto.createHmac('sha512', PAYSTACK_SECRET_KEY).update(req.rawBody).digest('hex');
        if (!signature || signature !== expectedHash) {
            console.warn('⚠️  Rejected a webhook call with an invalid or missing Paystack signature');
            return res.status(401).json({ error: 'Invalid signature' });
        }
    } else {
        console.warn('⚠️  PAYSTACK_SECRET_KEY not set — accepted a webhook call WITHOUT signature verification (only acceptable in local/simulated dev mode)');
    }

    const { event, data } = req.body || {};
    if (event === 'charge.success' && data && data.reference) {
        finalizePayment(data.reference, 'SUCCESS');
    } else if (event === 'charge.failed' && data && data.reference) {
        finalizePayment(data.reference, 'FAILED');
    }
    // Always 200 — Paystack retries on non-2xx, and there's nothing to retry for events
    // we don't act on (e.g. transfer.success, which we already treat as fire-and-forget).
    res.sendStatus(200);
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
        if (record.ownerShareRate != null) {
            passport.ownerShareRate = Math.min(1, Math.max(0, parseFloat(record.ownerShareRate) || 0));
        }
        ensureWorkAndPayFields(passport);

        saveDatabase(db);
        emitFleetToOwner(socket.id);

        // If a driver for this plate is already online, refresh their registration flag live
        const liveEntry = Object.values(activeRiders).find(r => r.plateNumber === plate);
        if (liveEntry) {
            liveEntry.isRegisteredVehicle = true;
            broadcastFleetUpdates(ownerCode);
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

        // Track distance since the last known position, feed the service passport.
        // Accuracy-aware noise floor: GPS jitter while genuinely stationary routinely
        // exceeds 5-30m (parked under cover, dense buildings, ordinary consumer GPS
        // noise) — a flat 5m threshold was letting that jitter get silently counted as
        // real movement, which meant an oil-change reset could visibly climb back up
        // within seconds even with the vehicle sitting still. Scales the required
        // movement to the reading's own reported accuracy instead of a fixed number.
        const prev = activeRiders[socket.id];
        if (prev && prev.lat != null && prev.lng != null && data.lat != null && data.lng != null) {
            const deltaKm = calculateDistanceKm(prev.lat, prev.lng, data.lat, data.lng);
            const accuracyM = typeof data.gpsAccuracyM === 'number' ? data.gpsAccuracyM : 9999;
            const noiseFloorKm = Math.max(0.015, (accuracyM * 2) / 1000); // at least 15m, or 2x the reading's own accuracy
            if (deltaKm > noiseFloorKm && deltaKm < 2.0) {
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
        broadcastFleetUpdates(fleetOwnerCode);
    }

    // Driver explicitly goes offline without disconnecting the socket
    safeOn(socket, 'driver-go-offline', () => {
        if (activeRiders[socket.id]) {
            const affectedFleetCode = activeRiders[socket.id].fleetOwnerCode;
            checkAndEmitShortfallAlert(activeRiders[socket.id]);
            delete activeRiders[socket.id];
            delete activePoolTrips[socket.id];
            io.emit('rider-disconnected', socket.id);
            broadcastFleetUpdates(affectedFleetCode);
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
            broadcastFleetUpdates(rider.fleetOwnerCode);
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
            broadcastFleetUpdates(rider.fleetOwnerCode);
        }
        io.emit('trip-meter-started', data);
    });

    safeOn(socket, 'end-trip', (tripData) => {
        const rider = activeRiders[socket.id];
        if (rider) {
            const fareGhs = parseFloat(tripData.fareGhs || 0);
            // shiftEarnings is credited by settleFareForVehicle() once payment actually
            // settles, using the driver's real post-split share — not here with the gross
            // fare, which used to make a fleet driver's earnings display show gross
            // throughput instead of their actual take-home under Work & Pay.
            rider.status = 'IDLE';

            const stats = ensureDriverStats(rider.plateNumber);
            stats.totalTrips += 1;
            recomputeBadge(stats);
            checkReferralQualification(rider.plateNumber);

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
            broadcastFleetUpdates(rider.fleetOwnerCode);
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
        broadcastFleetUpdates(passport.ownerCode);
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

        // shiftEarnings is credited by settleFareForVehicle() once payment actually
        // settles (see the matching note in the end-trip handler above) — not here with
        // the gross per-stop share.
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
        broadcastFleetUpdates(rider.fleetOwnerCode);

        // If every stop is dropped, close out the pool trip
        if (trip.stops.every(s => s.status === 'DROPPED')) {
            const stats = ensureDriverStats(rider.plateNumber);
            stats.totalTrips += 1;
            recomputeBadge(stats);
            checkReferralQualification(rider.plateNumber);
            delete activePoolTrips[socket.id];
            persistPoolTrip(rider.plateNumber, null);
            rider.status = 'IDLE';
            io.emit('rider-location-updated', rider);
            broadcastFleetUpdates(rider.fleetOwnerCode);
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
            broadcastFleetUpdates(rider.fleetOwnerCode);
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

    // ---- Payments (Mobile Money) ----

    // Driver registers where their payout goes. Called from the ownership modal.
    safeOn(socket, 'register-payout-number', (data) => {
        if (!data || !data.plateNumber || !data.momoNumber || !data.network) return;
        const plate = data.plateNumber.toUpperCase();
        db.driverPayouts[plate] = { momoNumber: data.momoNumber, network: data.network };
        saveDatabase(db);
        socket.emit('payout-number-saved', db.driverPayouts[plate]);
    });

    // Fleet owner registers where their Work & Pay settlement share goes.
    safeOn(socket, 'register-owner-payout', (data) => {
        if (!data || !data.ownerCode || !data.momoNumber || !data.network) return;
        const ownerCode = data.ownerCode.trim();
        db.ownerPayouts[ownerCode] = { momoNumber: data.momoNumber, network: data.network };
        saveDatabase(db);
        socket.emit('owner-payout-saved', db.ownerPayouts[ownerCode]);
    });

    // Driver's own console asks for their vehicle's live daily settlement progress
    safeOn(socket, 'request-work-and-pay-status', (plateNumber) => {
        if (!plateNumber) return;
        const passport = db.servicePassports[plateNumber.toUpperCase()];
        if (!passport || !passport.ownerCode) {
            socket.emit('work-and-pay-update', null); // independent driver — no target to show
            return;
        }
        ensureWorkAndPayFields(passport);
        resetWorkAndPayIfNewDay(passport);
        saveDatabase(db);
        const dailyTargetGhs = passport.dailyLeaseTarget || 0;
        socket.emit('work-and-pay-update', {
            plateNumber: plateNumber.toUpperCase(), dailyTargetGhs, collectedTodayGhs: passport.collectedTodayGhs,
            progressPct: dailyTargetGhs > 0 ? Math.min(100, Math.round((passport.collectedTodayGhs / dailyTargetGhs) * 100)) : 0,
            targetMet: dailyTargetGhs > 0 && passport.collectedTodayGhs >= dailyTargetGhs
        });
    });

    // Owner or driver logs a fuel/maintenance/other expense against a vehicle
    safeOn(socket, 'log-expense', (data) => {
        const { plateNumber, ownerCode, category, amountGhs, note } = data || {};
        if (!plateNumber || !ownerCode || !amountGhs || amountGhs <= 0) return;
        db.expenses.push({
            id: 'EXP-' + Date.now() + '-' + Math.random().toString(36).slice(2, 7),
            plateNumber: plateNumber.toUpperCase(), ownerCode: ownerCode.trim(),
            category: category || 'other', amountGhs: parseFloat(amountGhs), note: note || '',
            timestamp: new Date().toISOString()
        });
        saveDatabase(db);
        broadcastFleetUpdates(ownerCode.trim());
    });

    // Passenger pays for a completed trip (private or one Pool stop). Wallet credit is
    // applied automatically before any MoMo charge is attempted.
    safeOn(socket, 'initiate-payment', (paymentData) => {
        const { tripId, payerId, payerPhone, network, amountGhs, plateNumber, poolStopId } = paymentData || {};
        if (!payerId || !amountGhs || amountGhs <= 0) return;

        const availableCredit = db.walletCredits[payerId] || 0;
        const creditApplied = +Math.min(availableCredit, amountGhs).toFixed(2);
        const amountDue = +(amountGhs - creditApplied).toFixed(2);
        if (creditApplied > 0) db.walletCredits[payerId] = +(availableCredit - creditApplied).toFixed(2);

        if (amountDue <= 0) {
            // Fully covered by wallet credit — no MoMo charge needed
            const tx = recordTransaction({
                tripId, payerId, payerPhone, network: network || null, method: 'WALLET_CREDIT',
                amountGhs, creditApplied, amountDue: 0, status: 'SUCCESS',
                payerSocketId: socket.id, plateNumber, poolStopId, reference: 'CREDIT-' + Date.now()
            });
            checkReferralQualification(payerId);
            const settlement = settleFareForVehicle(plateNumber, amountGhs, 'WALLET_CREDIT');
            tx.ownerShareGhs = settlement.ownerShareGhs;
            tx.driverShareGhs = settlement.driverShareGhs;
            saveDatabase(db);
            socket.emit('payment-status-update', { tripId, status: 'SUCCESS', reference: tx.reference, amountGhs, creditApplied, amountDue: 0 });
            return;
        }

        const tx = recordTransaction({
            tripId, payerId, payerPhone, network, method: 'MOMO',
            amountGhs, creditApplied, amountDue, status: 'PENDING',
            payerSocketId: socket.id, plateNumber, poolStopId, reference: null
        });
        socket.emit('payment-status-update', { tripId, status: 'PENDING', amountGhs, amountDue, creditApplied });

        initiateMomoCharge(payerId, network, amountDue, (result) => {
            tx.reference = result.reference;
            saveDatabase(db);
            if (!result.awaitingWebhook) {
                // Simulated mode (no Paystack key configured) — result IS the final outcome
                finalizePayment(result.reference, result.success ? 'SUCCESS' : 'FAILED');
            } else if (!result.success) {
                // Real mode, but the charge REQUEST itself failed (bad number, network
                // error) — no prompt was ever sent, so there's no webhook coming for this one
                finalizePayment(result.reference, 'FAILED');
            }
            // else: real mode, prompt sent successfully — stays PENDING until the
            // /api/payment/webhook route above hears back from Paystack
        });
    });

    // Cash fallback — no gateway involved, still logged for the ledger and still eligible
    // to trigger referral qualification.
    safeOn(socket, 'mark-paid-cash', (data) => {
        const { tripId, payerId, payerPhone, amountGhs, plateNumber, poolStopId } = data || {};
        if (!payerId || !amountGhs) return;

        // If the passenger backed out of a pending MoMo prompt to pay cash instead,
        // cancel that pending transaction. Otherwise, if they later approve the old
        // prompt on their phone out of habit, a late webhook would trigger a second
        // driver payout for the same trip — finalizePayment()'s idempotency check only
        // protects against a transaction being finalized twice, not against two separate
        // successful transactions existing for one trip.
        const pendingMomo = db.transactions.find(t => t.tripId === tripId && t.status === 'PENDING');
        if (pendingMomo) {
            pendingMomo.status = 'CANCELLED';
            pendingMomo.settledAt = new Date().toISOString();
        }

        const tx = recordTransaction({
            tripId, payerId, payerPhone, method: 'CASH', amountGhs,
            creditApplied: 0, amountDue: amountGhs, status: 'SUCCESS',
            payerSocketId: socket.id, plateNumber, poolStopId, reference: 'CASH-' + Date.now()
        });
        checkReferralQualification(payerId);
        // Tracked for the ledger/reports, not auto-disbursed — see settleFareForVehicle's comment for why
        const settlement = settleFareForVehicle(plateNumber, amountGhs, 'CASH');
        tx.ownerShareGhs = settlement.ownerShareGhs;
        tx.driverShareGhs = settlement.driverShareGhs;
        saveDatabase(db);
    });

    // ---- Referral program ----

    safeOn(socket, 'request-referral-info', (identifier) => {
        if (!identifier) return;
        const code = generateReferralCode(identifier);
        const myReferrals = db.referrals.filter(r => r.referrerId === identifier);
        socket.emit('referral-info-update', {
            code,
            walletCredit: db.walletCredits[identifier] || 0,
            referrals: myReferrals.map(r => ({ status: r.status, type: r.type, createdAt: r.createdAt }))
        });
    });

    safeOn(socket, 'submit-referral-code', (data) => {
        const { code, refereeId, type } = data || {};
        if (!code || !refereeId || !type) return;
        const referral = registerReferral(code, refereeId, type);
        socket.emit('referral-submit-result', {
            success: !!referral,
            walletCredit: db.walletCredits[refereeId] || 0
        });
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
            accuracyM: sosData.accuracyM || null,
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
            const affectedFleetCode = activeRiders[socket.id].fleetOwnerCode;
            delete activeRiders[socket.id];
            delete activePoolTrips[socket.id];
            io.emit('rider-disconnected', socket.id);
            broadcastFleetUpdates(affectedFleetCode);
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
