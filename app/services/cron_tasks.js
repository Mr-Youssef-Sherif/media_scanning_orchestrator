// cron_tasks.js
import cron from 'node-cron';
import { cleanupAndGenerateAnalytics } from './cleanup_and_analytics.js';

export function startCronJobs() {
    // Run immediately on startup
    cleanupAndGenerateAnalytics();

    // Schedule to run every 2 hours
    cron.schedule('0 */2 * * *', () => {
        console.log('[Cron] Running daily media job cleanup...');
        cleanupAndGenerateAnalytics();
    });
}
