import { S3Client, PutObjectCommand, GetObjectCommand, CopyObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import dotenv from 'dotenv';
dotenv.config();

const s3 = new S3Client({
    region: 'auto',
    endpoint: process.env.R2_ENDPOINT,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY_ID,
        secretAccessKey: process.env.R2_ACCESS_KEY
    }
});

// Generate a signed URL for uploading a file to S3
export async function generateSignedUploadUrl({
    fileName,
    contentType,
    contentLength,
    sha256Hash,
    md5Hash,
    folder,
}) {
    const bucket = process.env.R2_BUCKET;
    const key = `${folder}/${fileName}`;
    const command = new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        ContentType: contentType,
        ContentLength: contentLength,
        ChecksumSHA256: Buffer.from(sha256Hash, 'hex').toString('base64'), // ✅ BASE64
        ContentMD5: Buffer.from(md5Hash, 'hex').toString('base64'),
    });

    const signedUrl = await getSignedUrl(s3, command, { expiresIn: 600 });

    return signedUrl;
}

// Generate a signed URL for accessing a file in S3
export async function generateSignedGetUrl({ fileKey }) {
    const command = new GetObjectCommand({
        Bucket: process.env.R2_BUCKET,
        Key: fileKey
    });

    return await getSignedUrl(s3, command, { expiresIn: 600 });
}

// Move object between buckets (same credential, one client)
export async function moveObjectWithinBuckets({
    fromKey,
    toKey,
    fromBucket,
    toBucket,
}) {
    const s3 = new S3Client({
        region: 'auto',
        endpoint: process.env.R2_ENDPOINT,
        credentials: {
            accessKeyId: process.env.R2_ACCESS_KEY_ID_FOR_ALL,
            secretAccessKey: process.env.R2_ACCESS_KEY_FOR_ALL
        }
    });

    await s3.send(new CopyObjectCommand({
        Bucket: toBucket,
        CopySource: `${fromBucket}/${fromKey}`,
        Key: toKey,
    }));

    await s3.send(new DeleteObjectCommand({
        Bucket: fromBucket,
        Key: fromKey,
    }));
}
