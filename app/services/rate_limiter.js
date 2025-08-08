import fs from 'fs';
import path from 'path';

const rateLimits = new Map();
const spammerSet = new Set(); // in memory mirror (loaded from file)

const LIMIT_PER_MINUTE = 10;
const SPAM_THRESHOLD = 50;
const WINDOW_MS = 60 * 1000;

const SPAM_LIST_FILE = path.resolve('./spammers.json');

// Load spammers on boot
if (fs.existsSync(SPAM_LIST_FILE)) {
    try {
        const content = fs.readFileSync(SPAM_LIST_FILE, 'utf-8');
        const parsed = JSON.parse(content);
        parsed.forEach((key) => spammerSet.add(key));
    } catch (e) {
        console.error('Failed to load spammers:', e);
    }
}

function saveSpammersToDisk() {
    fs.writeFileSync(SPAM_LIST_FILE, JSON.stringify(Array.from(spammerSet)));
}

export function isSpammer(key) {
    return spammerSet.has(key);
}

export function checkRateLimit({ userId }) {
    const key = userId;
    const now = Date.now();

    // Check if flagged spammer
    if (isSpammer(key)) return false;

    if (!rateLimits.has(key)) {
        rateLimits.set(key, []);
    }

    const timestamps = rateLimits.get(key).filter(ts => now - ts < WINDOW_MS);
    timestamps.push(now);

    rateLimits.set(key, timestamps);

    if (timestamps.length > SPAM_THRESHOLD) {
        console.warn(`User/IP flagged as spammer: ${key}`);
        spammerSet.add(key);
        saveSpammersToDisk();
        return false;
    }

    return timestamps.length <= LIMIT_PER_MINUTE;
}

export function clearFromSpammerList(key) {
    if (spammerSet.has(key)) {
        spammerSet.delete(key);
        saveSpammersToDisk();
    }
}
