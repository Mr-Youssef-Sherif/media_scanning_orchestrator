// supabaseClient.js
import dotenv from 'dotenv';
dotenv.config();
import { createClient } from '@supabase/supabase-js';
import { randomUUID } from "crypto";
import { type } from 'os';

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;
export const supabase = createClient(supabaseUrl, supabaseKey);

// Insert a new media job
export async function insertMediaJob(job) {
    if (
        !job ||
        !job.id ||
        !job.user_id ||
        !job.linked_to_id ||
        !job.linked_to_type ||
        !job.file_name ||
        !job.file_size ||
        !job.sha256_hash ||
        !job.mime_type
    ) {
        throw new Error("Invalid job data");
    }

    const { data, error } = await supabase
        .from('media_jobs')
        .insert([{
            id: job.id,
            user_id: job.user_id,
            linked_to_id: job.linked_to_id,
            linked_to_type: job.linked_to_type,
            file_name: job.file_name,
            file_size: job.file_size,
            sha256_hash: job.sha256_hash,
            media_type: job.mime_type.startsWith('video') ? 'video' : 'image',
            mime_type: job.mime_type,
            status: 'awaiting_upload'
        }])
        .select('id') // Get the generated ID
        .single();

    if (error) throw error;

    return data;
}

// Move media item from jobs table to media table
export async function createMediaItem(mediaItem) {
    if (
        !mediaItem ||
        !mediaItem.job_id ||
        !mediaItem.user_id ||
        !mediaItem.linked_to_id ||
        !mediaItem.linked_to_type ||
        !mediaItem.file_name ||
        !mediaItem.file_size ||
        !mediaItem.sha256_hash ||
        !mediaItem.mime_type ||
        !mediaItem.width ||
        !mediaItem.height
    ) {
        throw new Error("Invalid media item data");
    }

    if (
        mediaItem.width == null ||
        mediaItem.height == null
    ) {
        throw new Error("Missing media dimensions");
    }

    const isVideo = mediaItem.mime_type.startsWith("video");

    // If video, ensure duration is provided
    if (isVideo && (mediaItem.duration === undefined || mediaItem.duration === null)) {
        throw new Error("Missing duration for video media item");
    }

    const generatedId = randomUUID();


    const { data, error } = await supabase
        .from('media')
        .insert([{
            id: generatedId,
            job_id: mediaItem.job_id,
            user_id: mediaItem.user_id,
            linked_to_id: mediaItem.linked_to_id,
            linked_to_type: mediaItem.linked_to_type,
            file_name: mediaItem.file_name,
            file_size: mediaItem.file_size,
            file_hash: mediaItem.sha256_hash,
            hash_algorithm: "sha256",
            mime_type: mediaItem.mime_type,
            moderation_status: 'approved',
            media_type: mediaItem.mime_type.startsWith("image") ? "image" : "video",
            duration: isVideo ? mediaItem.duration : null, // in seconds
            width: mediaItem.width,
            height: mediaItem.height,
        }])
        .select('id') // Get the generated ID
        .single();

    if (error) {
        console.error("Failed to insert media", mediaItem.id, error);
        throw error;
    }


    return data;
}


// Mark media job as uploaded and get the job
export async function markMediaJobAsUploaded(jobId) {
    if (!jobId) throw new Error("Invalid job ID");

    const { data, error } = await supabase
        .from('media_jobs')
        .update({ status: 'pending' })
        .eq('id', jobId)
        .select()
        .single();        // Ensures it returns one row only

    if (error) throw error;

    return data;         // 🔁 Now you get the updated job back
}


// Get all media jobs with status 'pending'
export async function getPendingMediaJobs() {
    const { data, error } = await supabase
        .from('media_jobs')
        .select('*')
        .eq('status', 'pending');

    if (error) throw error;
    return data;
}

// Bulk update media jobs to 'complete' status
export async function markJobsAsComplete(jobs) {
    if (jobs.length === 0) return;

    const ids = jobs.map(job => job.id);

    const { error } = await supabase
        .from('media_jobs')
        .update({ status: 'complete' })
        .in('id', ids);

    if (error) throw error;
}

export async function logSystemEvent({
    description,
    action = "error",
    error_name = null,
    metadata = {},
    target_id = null,
}) {
    const { error } = await supabase
        .from('logs')
        .insert([{
            source: 'media_scanner_orchestrator',
            action: action, // blocked_by_ban, failed_scan, unsafe_content_detected, 
            status: "pending",
            description,
            metadata,
            target_id,
            target_type: "scan",
            error_name: error_name
        }]);

    if (error) {
        console.error("[logSystemEvent] Failed to insert log:", error);
    }
}

/* Log errors
media_scan_engine_failed, invalid_scan_type, media_upload_request_failed, scan_result_missing,
quarantine_move_failed, unsafe_content_log_failed, user_upload_restriction_failed, blocked_hash_add_failed,
media_move_failed, media_create_failed, mark_jobs_complete_failed, unexpected_job_processing_error,
signed_url_generation_failed
*/

// Check if a hash is blocked
export async function isHashBlocked({ hashValue, hashType }) {
    if (!hashValue || !hashType) {
        throw new Error("Missing hash value or type");
    }

    const { data, error } = await supabase
        .from('blocked_hashes')
        .select('id')
        .eq('hash_value', hashValue)
        .eq('hash_type', hashType)
        .maybeSingle();

    if (error) {
        console.error("Error checking blocked hash:", error);
        throw error;
    }

    return !!data; // returns true if blocked
}

// Add a new blocked hash
export async function addBlockedHash({
    hash_value,
    hash_type,
    detected_type,
    source_type = null,
    detected_by = null,
    file_key = null,
    is_verified = false,
    notes = null,
}) {
    if (!hash_value || !hash_type || !detected_type) {
        throw new Error("Missing required fields for blocked hash");
    }

    const { data, error } = await supabase
        .from('blocked_hashes')
        .insert([{
            hash_value,
            hash_type,
            detected_type,
            source_type,
            detected_by,
            file_key,
            is_verified,
            notes,
        }])
        .select()
        .single();

    if (error) {
        console.error("Error adding blocked hash:", error);
        throw error;
    }

    return data;
}

export async function getUserBanStatus(userId) {
    if (!userId) {
        throw new Error("Missing user ID");
    }

    const now = new Date().toISOString();

    const { data, error } = await supabase
        .from('users_bans')
        .select('type, reason, expires_at')
        .eq('user_id', userId)
        .or(`expires_at.is.null,expires_at.gt.${now}`); // still valid bans

    if (error) {
        console.error("Error checking user ban:", error);
        throw error;
    }

    const activeBans = data || [];
    return {
        isBanned: activeBans.length > 0,
        bans: activeBans
    };
}

export async function restrictUserMediaUploads({ userId }) {
    const { error } = await supabase
        .from('users_bans')
        .insert([{
            user_id: userId,
            type: "media_upload",
            reason: "Unsafe content detected on upload.",
            source: "system",
        }]);

    if (error) {
        console.error("[logSystemEvent] Failed to insert log:", error);
    }
};

// Clean up and analytics

// Get jobs older than N hours
export async function getOldJobs(hours = 2) {
    const cutoff = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
    const { data, error } = await supabase
        .from('media_jobs')
        .select('id, status, media_type, file_size, created_at')
        .lt('created_at', cutoff);

    if (error) throw error;
    return data;
}

// Get flagged jobs from logs
export async function getFlaggedJobs(jobIds) {
    if (!jobIds.length) return [];
    const { data, error } = await supabase
        .from('logs')
        .select('target_id, metadata')
        .in('target_id', jobIds)
        .eq('action', 'unsafe_content_detected');

    if (error) throw error;

    return data.map(log => ({
        id: log.target_id,
        media_type: log.metadata?.media_type || 'unknown',
        file_size: log.metadata?.file_size || 0
    }));
}

// Get errored jobs from logs
export async function getErroredJobs(jobIds) {
    if (!jobIds.length) return [];
    const { data, error } = await supabase
        .from('logs')
        .select('target_id, error_name, metadata')
        .in('target_id', jobIds)
        .not('error_name', 'is', null);

    if (error) throw error;

    return data.map(log => ({
        id: log.target_id,
        error_name: log.error_name,
        media_type: log.metadata?.media_type || 'unknown',
        file_size: log.metadata?.file_size || 0
    }));
}

// Delete old jobs
export async function deleteOldJobs({ completeDays = 7, incompleteMinutes = 60 }) {
    const completeCutoff = new Date(Date.now() - completeDays * 24 * 60 * 60 * 1000).toISOString();
    const incompleteCutoff = new Date(Date.now() - incompleteMinutes * 60 * 1000).toISOString();

    // Delete completed
    await supabase
        .from('media_jobs')
        .delete()
        .eq('status', 'complete')
        .lt('created_at', completeCutoff);

    // Delete abandoned uploads
    await supabase
        .from('media_jobs')
        .delete()
        .eq('status', 'awaiting_upload')
        .lt('created_at', incompleteCutoff);
}
