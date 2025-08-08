import { getPendingMediaJobs, logSystemEvent } from './db.js';
import { scanMediaJobs } from './media_scanner.js';
import { generateSignedGetUrl } from '../services/s3.js';

const imageQueue = [];
const videoQueue = [];

let imageScanning = false;
let videoScanning = false;

let imageTimer = null;
let videoTimer = null;

let firstImageTime = null;
let firstVideoTime = null;

const IMAGE_MAX_WAIT = 10000;
const IMAGE_INTERVAL = 2000;
const IMAGE_BATCH_SIZE = 50;

const VIDEO_MAX_WAIT = 10000;
const VIDEO_INTERVAL = 3000;
const VIDEO_BATCH_SIZE = 10;

// Just triggers scan if not busy
function maybeFlushImages() {
    if (imageScanning || imageQueue.length === 0) return;

    const batch = imageQueue.splice(0, IMAGE_BATCH_SIZE);
    if (batch.length === 0) return;

    imageScanning = true;

    scanMediaJobs(batch, "images").finally(() => {
        imageScanning = false;
        if (imageQueue.length > 0) scheduleFlush(false); // trigger next if pending
    });
}

function maybeFlushVideos() {
    if (videoScanning || videoQueue.length === 0) return;

    const batch = videoQueue.splice(0, VIDEO_BATCH_SIZE);
    if (batch.length === 0) return;

    videoScanning = true;

    scanMediaJobs(batch, "videos").finally(() => {
        videoScanning = false;
        if (videoQueue.length > 0) scheduleFlush(true); // trigger next if pending
    });
}

function scheduleFlush(isVideo) {
    const now = Date.now();

    if (isVideo) {
        if (!firstVideoTime) firstVideoTime = now;
        clearTimeout(videoTimer);

        const elapsed = now - firstVideoTime;
        if (elapsed >= VIDEO_MAX_WAIT || videoQueue.length >= VIDEO_BATCH_SIZE) {
            firstVideoTime = null;
            maybeFlushVideos();
        } else {
            videoTimer = setTimeout(() => {
                firstVideoTime = null;
                maybeFlushVideos();
            }, Math.min(VIDEO_MAX_WAIT - elapsed, VIDEO_INTERVAL));
        }
    } else {
        if (!firstImageTime) firstImageTime = now;
        clearTimeout(imageTimer);

        const elapsed = now - firstImageTime;
        if (elapsed >= IMAGE_MAX_WAIT || imageQueue.length >= IMAGE_BATCH_SIZE) {
            firstImageTime = null;
            maybeFlushImages();
        } else {
            imageTimer = setTimeout(() => {
                firstImageTime = null;
                maybeFlushImages();
            }, Math.min(IMAGE_MAX_WAIT - elapsed, IMAGE_INTERVAL));
        }
    }
}

export async function queueMediaJob(job) {
    if (job.mime_type.startsWith('video')) {
        videoQueue.push(job);
        scheduleFlush(true);
    } else {
        imageQueue.push(job);
        scheduleFlush(false);
    }

    /* job = {
    id,
    user_id,
    file_name,
    file_size,
    sha256_hash,
    mime_type,
    linked_to_id,
    linked_to_type,
    url
    }
    */
}

// --- Fallback Recovery ---
(async () => {
    console.log('📦 Starting media job recovery...');
    const missedJobs = await getPendingMediaJobs();
    if (missedJobs.length === 0) {
        console.log('✅ No missed jobs found.');
        return;
    }

    for (const job of missedJobs) {
        console.log(`🔍 Processing job ID: ${job.id}`);
        console.log(`🔑 file_name: "${job.file_name}"`);

        if (!job.file_name || typeof job.file_name !== 'string') {
            console.error(`❌ Invalid file_name for job ${job.id}`);
            continue;
        }

        try {
            const signedUrl = await generateSignedGetUrl({ fileKey: job.file_name });

            if (!signedUrl) {
                console.error(`❌ Failed to generate signed URL for job ${job.id}`);
                continue;
            }

            console.log(`🔗 Signed URL generated for job ${job.id}: ${signedUrl}`);

            job.url = signedUrl;
            queueMediaJob(job);
        } catch (err) {
            console.error(`❗ Error while generating signed URL for job ${job.id}:`, err.message);
            const msg = `❗ [Media Recovery] Failed to generate signed URL for job ${job.id} (file_name: "${job.file_name}") – ${err.message}`;
            await logSystemEvent({ action: "error", error_name: "signed_url_generation_failed", message: msg, meta: { jobId: job.id, fileName: job.file_name } });
        }
    }

    if (imageQueue.length > 0) scheduleFlush(false);
    if (videoQueue.length > 0) scheduleFlush(true);
})();
