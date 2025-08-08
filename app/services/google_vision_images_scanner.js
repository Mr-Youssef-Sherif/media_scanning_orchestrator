import axios from 'axios';
import dotenv from "dotenv";
dotenv.config();

const google_vision_api_key = process.env.GOOGLE_VISION_API_KEY;

export async function safeSearchFromUrls(jobs) {
    const requests = jobs.map(job => ({
        image: {
            source: { imageUri: job.url }
        },
        features: [{ type: 'SAFE_SEARCH_DETECTION' }]
    }));

    const { data } = await axios.post(
        `https://vision.googleapis.com/v1/images:annotate?key=${google_vision_api_key}`,
        { requests }
    );

    return data.responses.map((res, i) => {
        const job = jobs[i];
        if (res.error) {
            console.warn(`[Vision API] Error in image ${job.id}:`, res.error.message);
            return { job_id: job.id, is_nsfw: false, error: res.error };
        }

        const result = res.safeSearchAnnotation;
        const is_nsfw = ["POSSIBLE", "LIKELY", "VERY_LIKELY"].some(likelihood =>
            [result.adult, result.violence, result.racy].includes(likelihood)
        );

        return {
            job_id: job.id,
            is_nsfw,
            raw: result
        };
    });
}
