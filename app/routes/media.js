import express from 'express';
import { queueMediaJob } from '../services/queue.js';
import { authenticate } from '../middleware/auth.js';
import { checkRateLimit } from '../services/rate_limiter.js';
import { generateSignedGetUrl, generateSignedUploadUrl } from '../services/s3.js';
import { insertMediaJob, markMediaJobAsUploaded, logSystemEvent, isHashBlocked, getUserBanStatus } from '../services/db.js';
import crypto from 'crypto';
import validator from 'validator';

export const router = express.Router();

const allowedMimeTypes = ['image/jpeg', 'image/png', 'video/mp4'];
const allowedLinkedTypes = ['post', 'opportunity', 'license_certification', 'education', 'project', 'award_achievement', 'work_experience', 'volunteer_experience', 'testimonial', 'message', 'publication', 'course', 'note', 'exam', 'event', 'chat_media', 'profile_picture', 'profile_cover'];
const maxVideoFileSize = 70 * 1024 * 1024; // 70MB
const maxImageFileSize = 10 * 1024 * 1024; // 10MB

function extractUuid(fileKey) {
    const parts = fileKey.split('/');
    return parts.length === 3 ? parts[2].split('.')[0] : null;
}

// request-upload
router.post('/request-upload', authenticate, async (req, res) => {
    try {
        const userId = req.user.id;

        if (!validator.isUUID(userId)) {
            return res.status(400).json({ error: 'Invalid or missing user_id' });
        }

        if (!checkRateLimit({ userId })) {
            return res.status(429).json({ error: "Too many requests. Please try again later." });
        }

        const {
            file_name,
            file_size,
            mime_type,
            sha256_hash,
            linked_to_id,
            linked_to_type,
            md5Hash,
        } = req.body;

        if (!file_name || !file_size || !mime_type || !sha256_hash || !linked_to_id || !linked_to_type || !md5Hash) {
            return res.status(400).json({ error: 'Missing required fields' });
        }

        if (!allowedLinkedTypes.includes(linked_to_type)) {
            return res.status(400).json({ error: 'Invalid linked_to_type' });
        }

        if (!allowedMimeTypes.includes(mime_type)) {
            return res.status(400).json({ error: 'Invalid MIME type' });
        }

        const isVideo = mime_type.startsWith('video');
        const isImage = mime_type.startsWith('image');

        if ((isVideo && (file_size > maxVideoFileSize || file_size <= 0)) ||
            (isImage && (file_size > maxImageFileSize || file_size <= 0))) {
            return res.status(400).json({ error: 'Invalid file size' });
        }

        if (!/^[a-fA-F0-9]{64}$/.test(sha256_hash)) {
            return res.status(400).json({ error: "Invalid SHA-256 hash format" });
        }

        if (!/^[a-fA-F0-9]{32}$/.test(md5Hash)) {
            return res.status(400).json({ error: "Invalid MD5 hash format" });
        }

        // Check if the user is banned to upload media
        const { isBanned, bans } = await getUserBanStatus(userId);
        if (isBanned) {
            const hasAccountBan = bans.some(ban => ban.type === 'account_access' || ban.type === 'all');
            const hasUploadBan = bans.some(ban => ban.type === 'media_upload' || ban.type === 'all');

            if (hasAccountBan) {
                console.log("🚫 User is locked out of account");
                await logSystemEvent({
                    action: "blocked_by_ban",
                    description: `User ${userId} attempted to upload a file while being restricted.`,
                    metadata: {
                        user_id: userId,
                        sha256_hash,
                        linked_to_type,
                        linked_to_id,
                        ban_type: "account_access"
                    }
                });
                return res.status(403).json({ error: "You are account is restricted." });
            } else if (hasUploadBan) {
                console.log("🚫 User is banned from uploading media");
                await logSystemEvent({
                    action: "blocked_by_ban",
                    description: `User ${userId} attempted to upload a file while being restricted.`,
                    metadata: {
                        user_id: userId,
                        sha256_hash,
                        linked_to_type,
                        linked_to_id,
                        ban_type: "media_upload"
                    }
                });
                return res.status(403).json({ error: "You are currently restricted from uploading media." });
            }
        }

        // Check if hash is allowed or blocked
        const is_hash_blocked = await isHashBlocked({ hashValue: sha256_hash, hashType: "sha256" });

        if (is_hash_blocked) {
            await logSystemEvent({
                action: "unsafe_content_reupload",
                description: `User ${userId} attempted to upload a known-blocked file`,
                metadata: {
                    user_id: userId,
                    sha256_hash,
                    linked_to_type,
                    linked_to_id
                }
            });
            return res.status(400).json({ error: "This file can't be uploaded due to a policy violation. If you believe this is an error, contact support." });
        }

        const jobId = crypto.randomUUID();

        const folder = isVideo ? 'videos' : 'images';
        const shard = jobId.slice(0, 2);
        const extension = mime_type.split('/')[1];
        const fileKey = `${folder}/${shard}/${jobId}.${extension}`;

        // Insert metadata in DB
        const result = await insertMediaJob({
            id: jobId,
            user_id: userId,
            linked_to_id: linked_to_id,
            linked_to_type: linked_to_type,
            file_name: fileKey,
            file_size: file_size,
            sha256_hash: sha256_hash,
            mime_type: mime_type
        });

        if (!result || !result.id) {
            return res.status(500).json({ error: 'Failed to create media job' });
        }

        const signedUrl = await generateSignedUploadUrl({
            fileName: `${shard}/${jobId}.${extension}`,
            contentType: mime_type,
            contentLength: file_size,
            sha256Hash: sha256_hash,
            md5Hash: md5Hash,
            folder: folder,
        });

        return res.json({ signedUrl, fileKey, jobId });
    } catch (err) {
        console.error('Error requesting upload:', err);
        const msg = `Failed to handle /request-upload for user ${req?.user?.id || 'unknown'}: ${err.message}`;
        await logSystemEvent({ action: "error", description: msg, metadata: { user_id: `${req?.user?.id || 'unknown'}` }, error_name: "media_upload_request_failed" });
        return res.status(500).json({ error: 'Server error' });
    }
});

router.post('/upload-complete', authenticate, async (req, res) => {
    try {
        const { file_key } = req.body;
        if (!file_key || (!file_key.startsWith('videos/') && !file_key.startsWith('images/'))) {
            return res.status(400).json({ error: 'Missing required field or invalid file key' });
        }

        const uuid = extractUuid(file_key);
        if (!uuid) {
            return res.status(400).json({ error: 'Invalid file key format' });
        }

        // Generate signed URL for the uploaded file
        let signedUrl;
        try {
            signedUrl = await generateSignedGetUrl({ fileKey: file_key });
        } catch (err) {
            console.error("Signed URL generation failed:", err);
            return res.status(500).json({ error: 'Failed to generate signed URL' });
        }

        // Get the metadata from DB by filename and mark it as 'pending'
        const metadata = await markMediaJobAsUploaded(uuid);

        if (!metadata) {
            return res.status(404).json({ error: 'Media job not found or already processed' });
        }

        // Queue the job for scanning
        await queueMediaJob({
            id: metadata.id,
            user_id: metadata.user_id,
            file_name: metadata.file_name,
            file_size: metadata.file_size,
            sha256_hash: metadata.sha256_hash,
            mime_type: metadata.mime_type,
            linked_to_id: metadata.linked_to_id,
            linked_to_type: metadata.linked_to_type,
            url: signedUrl
        });

        return res.json({ status: 'queued' });
    } catch (err) {
        console.error('Error queueing media job:', err);
        return res.status(500).json({ error: 'Server error' });
    }
});