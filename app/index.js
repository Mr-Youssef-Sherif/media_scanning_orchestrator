import express from 'express';
import dotenv from 'dotenv';
import { router as mediaRouter } from './routes/media.js';
import { startCronJobs } from './services/cron_tasks.js';

dotenv.config();

const app = express();
app.use(express.json({ limit: '10mb' }));

app.use('/media', mediaRouter);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Media service running on port ${PORT}`);
});

startCronJobs();
