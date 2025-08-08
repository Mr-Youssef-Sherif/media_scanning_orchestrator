import {
    getOldJobs,
    getFlaggedJobs,
    getErroredJobs,
    deleteOldJobs,
    upsertAnalyticsRow,
    logSystemEvent
} from './db.js';

function summarizeMedia(items) {
    const stats = { count: 0, totalSize: 0, typeCounts: {} };
    for (const m of items) {
        stats.count++;
        stats.totalSize += m.file_size || 0;
        stats.typeCounts[m.media_type] = (stats.typeCounts[m.media_type] || 0) + 1;
    }
    return stats;
}

export async function cleanupAndGenerateAnalytics() {
    console.log(`[Analytics] Starting cleanup + analytics...`);
    await logSystemEvent({
        description: "Starting cleanup + analytics run",
        action: "analytics_run_start"
    });

    try {
        // Cleanup step
        await deleteOldJobs({ completeDays: 7, incompleteMinutes: 60 });
        await logSystemEvent({
            description: "Deleted old jobs",
            action: "analytics_cleanup_done"
        });

        // Get jobs older than 2 hours
        let jobs;
        try {
            jobs = await getOldJobs(2);
        } catch (err) {
            console.error("[Analytics] Failed to fetch old jobs:", err);
            await logSystemEvent({
                description: "Failed to fetch old jobs",
                action: "analytics_error",
                error_name: "getOldJobs_failed",
                metadata: { message: err.message, stack: err.stack }
            });
            return; // stop run
        }

        if (!jobs.length) {
            console.log(`[Analytics] No old jobs found`);
            await logSystemEvent({
                description: "No jobs found older than 2 hours",
                action: "analytics_no_jobs"
            });
            return;
        }

        // Group jobs by day
        const jobsByDay = {};
        for (const job of jobs) {
            const day = job.created_at.split('T')[0];
            (jobsByDay[day] ||= []).push(job);
        }

        // Process each day separately
        for (const [day, dayJobs] of Object.entries(jobsByDay)) {
            try {
                const jobIds = dayJobs.map(j => j.id);

                // Core counts
                const completedCount = dayJobs.filter(j => j.status === 'complete').length;
                const abandonedCount = dayJobs.filter(j => j.status === 'awaiting_upload').length;

                // Media summaries from jobs directly
                const acceptedStats = summarizeMedia(dayJobs.filter(j => j.status === 'complete'));

                // From logs
                const flaggedStats = summarizeMedia(await getFlaggedJobs(jobIds));
                const erroredStats = summarizeMedia(await getErroredJobs(jobIds));

                // Build stats object
                const stats = {
                    completed_jobs_count: completedCount,
                    abandoned_jobs_count: abandonedCount,
                    accepted_media_count: acceptedStats.count,
                    flagged_media_count: flaggedStats.count,
                    errored_jobs_count: erroredStats.count,
                    total_accepted_file_size: acceptedStats.totalSize,
                    total_flagged_file_size: flaggedStats.totalSize,
                    total_errored_file_size: erroredStats.totalSize,
                    accepted_media_type_counts: acceptedStats.typeCounts,
                    flagged_media_type_counts: flaggedStats.typeCounts,
                    errored_media_type_counts: erroredStats.typeCounts,
                    notes: `Aggregated from ${dayJobs.length} jobs`
                };

                await upsertAnalyticsRow(day, stats);
                await logSystemEvent({
                    description: `Analytics updated for day ${day}`,
                    action: "analytics_day_processed",
                    metadata: stats,
                    target_id: day
                });

                console.log(`[Analytics] Updated stats for ${day}`);
            } catch (err) {
                console.error(`[Analytics] Failed to process day ${day}:`, err);
                await logSystemEvent({
                    description: `Failed to process analytics for day ${day}`,
                    action: "analytics_error",
                    error_name: "day_processing_failed",
                    metadata: { day, message: err.message, stack: err.stack },
                    target_id: day
                });
            }
        }

        console.log(`[Analytics] Done`);
        await logSystemEvent({
            description: "Cleanup + analytics run completed successfully",
            action: "analytics_run_complete"
        });

    } catch (err) {
        console.error("[Analytics] Fatal error during cleanup + analytics:", err);
        await logSystemEvent({
            description: "Fatal error during cleanup + analytics",
            action: "analytics_error",
            error_name: "fatal_run_error",
            metadata: { message: err.message, stack: err.stack }
        });
    }
}
