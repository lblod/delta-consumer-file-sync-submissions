import { CronJob } from 'cron';
import { app, errorHandler } from 'mu';
import {
  CRON_PATTERN_FILE_SYNC, SERVICE_NAME, DISABLE_AUTOMATIC_SYNC
} from './config';
import { ProcessingQueue } from './lib/processing-queue';
import { startSync } from './pipelines/files-sync';
import bodyParser from 'body-parser';

const fileSyncQueue = new ProcessingQueue('file-sync-queue');

app.use( bodyParser.json( { type: function(req) { return /^application\/json/.test( req.get('content-type') ); } } ) );

app.get('/', function(req, res) {
  res.send(`Hello, you have reached ${SERVICE_NAME}! I'm doing just fine :)`);
});

app.post('/delta', async function( req, res ) {
  try {
    const body = req.body;
  }
}

new CronJob(CRON_PATTERN_FILE_SYNC, async function() {
  console.log(`Status of DISABLE_AUTOMATIC_SYNC: ${DISABLE_AUTOMATIC_SYNC}`);
  if(DISABLE_AUTOMATIC_SYNC) {
    console.log('Sync disabled, doing nothing');
  }
  else {
    const now = new Date().toISOString();
    console.info(`Delta sync triggered by cron job at ${now}`);
    fileSyncQueue.addJob(startSync);
  }
}, null, true);

/*
 * ENDPOINTS CURRENTLY MEANT FOR DEBUGGING
 */

app.post('/file-sync-jobs', async function( _, res ){
  startSync();
  res.send({ msg: 'Started file sync job' });
});

app.use(errorHandler);
