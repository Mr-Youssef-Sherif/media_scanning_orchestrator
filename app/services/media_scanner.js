import dotenv from "dotenv";
dotenv.config();
import { Function_ } from "modal";
import { markJobsAsComplete, createMediaItem, logSystemEvent, addBlockedHash, restrictUserMediaUploads } from "./db.js";
import { safeSearchFromUrls } from "./google_vision_images_scanner.js"
import { moveObjectWithinBuckets } from "./s3.js";

const scanMediaFn = await Function_.lookup("tomouh-scan-engine", "analyze_media");

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 3000;

const bucketMap = {
    post: 'posts-media',
    opportunity: 'opportunities-media',
    chat_media: 'chats-media',
    profile_picture: 'users-media',
    profile_cover: 'users-media',
    license_certification: 'talent-profiles-media',
    education: 'talent-profiles-media',
    project: 'talent-profiles-media',
    award_achievement: 'talent-profiles-media',
    work_experience: 'talent-profiles-media',
    volunteer_experience: 'talent-profiles-media',
    testimonial: 'talent-profiles-media',
    publication: 'talent-profiles-media',
};

async function prepareJobs(jobs, type) {
    return {
        type,
        jobs: jobs.map(job => ({
            job_id: job.id,
            url: job.url
        }))
    };
}

async function withRetry(fn, payload) {
    let attempt = 0;
    while (true) {
        try {
            const res = await fn.remote([], payload);
            console.dir(res, { depth: null });
            if (!res || !Array.isArray(res.results)) {
                console.error("Unexpected scanner response:", res);
                throw new Error("Bad response from scanner");
            }
            return res.results;
        } catch (err) {
            attempt++;
            console.error(`Attempt ${attempt} failed: ${err.message}`);
            if (attempt >= MAX_RETRIES) throw err;
            await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
        }
    }
}

export async function scanMediaJobs(jobs, type) {
    if (!["images", "videos"].includes(type)) {
        const msg = `Invalid scan type: ${type}`;
        await logSystemEvent({ action: "error", description: msg, error_name: "invalid_scan_type" });
        throw new Error(`Invalid scan type: ${type}`);
    }

    let results;

    if (type === "images" && jobs.length <= 16) {
        console.log(`[Scanner] Using Google Vision for ${jobs.length} image(s)`);
        results = await safeSearchFromUrls(jobs);
    } else {
        const payload = await prepareJobs(jobs, type);
        try {
            results = await withRetry(scanMediaFn, payload);
        } catch (err) {
            const msg = `Failed to scan media via modal scan engine: ${err.message}`;
            console.error(msg);
            await logSystemEvent({
                action: "error", description: msg, action: "failed_scan", error_name: "media_scan_engine_failed"
            });
            throw err;
        }
    }

    const resultsById = Object.fromEntries(results.map(r => [r.job_id, r]));

    const completedJobs = [];

    for (const job of jobs) {
        const result = resultsById[job.id];

        if (!result || typeof result.is_nsfw !== "boolean") {
            const msg = `[Scanner] Invalid or missing result for media ${job.id}`;
            console.warn(msg);
            await logSystemEvent({ action: "error", target_id: job.id, description: msg, error_name: "scan_result_missing" });
            continue;
        }
        try {
            // The toKey will be based on the linked to type(posts,etc..)

            // Fallback in case of unknown types
            const getTargetBucket = (linked_to_type) => bucketMap[linked_to_type] || 'default-bucket';

            const linked_to_type = job.linked_to_type;
            const toBucket = getTargetBucket(linked_to_type);

            const prefixTypes = ['post', 'opportunity'];
            const toKey = prefixTypes.includes(linked_to_type) || !linked_to_type
                ? job.file_name
                : `${linked_to_type}/${job.file_name}`; // For better sharding per category

            // Note: file name is like this - images/22/22b3f665-4e5c-4aef-901a-1e2f5e6c844e.jpeg

            if (result.is_nsfw) {
                const msg = `NSFW detected in ${type.slice(0, -1)} with ID ${job.id}`;

                // 1️. Add the item's hash to the block list
                try {
                    await addBlockedHash({
                        hash_value: job.sha256_hash,
                        hash_type: "sha256",
                        detected_type: "nsfw",
                        source_type: job.mime_type.startsWith('video') ? "video" : "image",
                        detected_by: "media_scanner",
                        file_key: toKey
                    });
                } catch (err) {
                    const errMsg = `Failed to add blocked hash for ${job.id}: ${err.message}`;
                    console.error(errMsg);
                    await logSystemEvent({
                        action: "error",
                        target_id: job.id,
                        description: errMsg,
                        error_name: "blocked_hash_add_failed"
                    });
                }

                // 2️. Restrict the user from uploading
                try {
                    await restrictUserMediaUploads({ userId: job.user_id });
                } catch (err) {
                    const errMsg = `Failed to restrict uploads for user ${job.user_id}: ${err.message}`;
                    console.error(errMsg);
                    await logSystemEvent({
                        action: "error",
                        target_id: job.id,
                        description: errMsg,
                        error_name: "user_upload_restriction_failed"
                    });
                }

                // 3️. Log the unsafe content detection
                try {
                    await logSystemEvent({
                        target_id: job.id,
                        description: msg,
                        action: "unsafe_content_detected",
                        metadata: {
                            file_name: job.file_name,
                            user_id: job.user_id,
                            linked_to_type: job.linked_to_type
                        }
                    });
                } catch (err) {
                    const errMsg = `Failed to log unsafe content detection for ${job.id}: ${err.message}`;
                    console.error(errMsg);
                    await logSystemEvent({
                        action: "error",
                        target_id: job.id,
                        description: errMsg,
                        error_name: "unsafe_content_log_failed"
                    });
                }

                // 4️. Move the file to quarantine
                try {
                    await moveObjectWithinBuckets({
                        fromKey: job.file_name,
                        toBucket: 'quarantine',
                        toKey: job.file_name,
                    });
                } catch (err) {
                    const errMsg = `Failed to move NSFW item ${job.id} to quarantine: ${err.message}`;
                    console.error(errMsg);
                    await logSystemEvent({
                        action: "error",
                        target_id: job.id,
                        description: errMsg,
                        error_name: "quarantine_move_failed"
                    });
                }
            } else {
                // 1️. Move file to final bucket
                try {
                    await moveObjectWithinBuckets({
                        fromKey: job.file_name,
                        toBucket: toBucket,
                        toKey: toKey
                    });
                } catch (err) {
                    const errMsg = `Failed to move media item for job ${job.id} to ${toBucket}: ${err.message}`;
                    console.error(errMsg);
                    await logSystemEvent({
                        action: "error",
                        target_id: job.id,
                        description: errMsg,
                        error_name: "media_move_failed"
                    });
                    continue; // Skip to next job since move failed
                }

                // 2️. Create the media item in DB
                try {
                    await createMediaItem({
                        job_id: job.id, // For auditing, analytics support, and to keep track of items across tables.
                        user_id: job.user_id,
                        file_name: toKey,
                        linked_to_id: job.linked_to_id,
                        linked_to_type: job.linked_to_type,
                        sha256_hash: job.sha256_hash,
                        width: result.width,
                        height: result.height,
                        duration: result.duration,
                        mime_type: job.mime_type,
                        file_size: job.file_size,
                        moderation_status: "approved",
                    });
                } catch (err) {
                    const errMsg = `Failed to create media item for job ${job.id}: ${err.message}`;
                    console.error(errMsg);
                    await logSystemEvent({
                        action: "error",
                        target_id: job.id,
                        description: errMsg,
                        error_name: "media_create_failed"
                    });
                    continue; // Skip to next job since creation failed
                }
            }

            completedJobs.push(job);
        } catch (err) {
            const msg = `Unexpected error while processing job ${job.id}: ${err.message}`;
            console.error(msg);
            await logSystemEvent({ action: "error", target_id: job.id, description: msg, error_name: "unexpected_job_processing_error" });

        }
    }

    try {
        await markJobsAsComplete(completedJobs);
        await logSystemEvent({ description: `Marked ${completedJobs.length} jobs as complete.`, action: "scan" });
    } catch (err) {
        console.error("Failed to mark jobs as complete:", err);
        await logSystemEvent({ action: "error", description: `Failed to mark jobs as complete: ${err.message}`, error_name: "mark_jobs_complete_failed" });
    }
}
