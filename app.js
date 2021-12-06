import bodyParser from 'body-parser';
import { CronJob } from 'cron';
import { app, errorHandler } from 'mu';
import {
  CRON_PATTERN_HEALING_SYNC, DISABLE_AUTOMATIC_SYNC, SERVICE_NAME, DISABLE_INITIAL_SYNC
} from './config';
import { getUnsyncedUris } from './lib/file-sync-job-utils';
import { ProcessingQueue } from './lib/processing-queue';
import { syncFilesAddition } from './pipelines/files-sync';
import { waitForDatabase } from './lib/database';

const fileSyncQueue = new ProcessingQueue('file-sync-queue');

console.log(`Status DISABLE_INITIAL_SYNC: ${DISABLE_INITIAL_SYNC}`);
if(!DISABLE_INITIAL_SYNC){
  waitForDatabase(() => {
    fileSyncQueue.addJob(async () => {
      const unsyncedFileUris = await getUnsyncedUris();
      await syncFilesAddition(unsyncedFileUris.map(t => t.pFile));
      console.log(`Initial sync was success`);
    });
  });
}

app.use( bodyParser.json( { type: function(req) { return /^application\/json/.test( req.get('content-type') ); } } ) );

app.get('/', function(req, res) {
  res.send(`Hello, you have reached ${SERVICE_NAME}! I'm doing just fine :)`);
});

app.post('/delta', async function(req, res){
  if(DISABLE_AUTOMATIC_SYNC){
    console.log(`Status of DISABLE_AUTOMATIC_SYNC: ${DISABLE_AUTOMATIC_SYNC}`);
  }
  else {
    const delta = req.body;
    const inserts = delta.map(changeSet => changeSet.inserts).flat();
    const subjects = [ ...new Set(inserts.map(t => t.subject.value)) ];

    if(subjects.length){
      fileSyncQueue.addJob(async () => {
        await syncFilesAddition(subjects);
      });
    }
  }
  res.status(202).send();
});

new CronJob(CRON_PATTERN_HEALING_SYNC, async function() {
  if(DISABLE_AUTOMATIC_SYNC) {
    console.log('Sync disabled, doing nothing');
  }
  else {
    const now = new Date().toISOString();
    console.info(`Delta healing sync triggered by cron job at ${now}`);
    fileSyncQueue.addJob(async () => {
      const unsyncedFileUris = await getUnsyncedUris();
      await syncFilesAddition(unsyncedFileUris.map(t => t.pFile));
    });
  }
}, null, true);

/*
 * ENDPOINTS CURRENTLY MEANT FOR DEBUGGING
 */

app.post('/file-sync-jobs', async function( _, res ){
  fileSyncQueue.addJob(async () => {
    const unsyncedFileUris = await getUnsyncedUris();
    await syncFilesAddition(unsyncedFileUris.map(t => t.pFile));
  });
  res.send({ msg: 'Started file sync job' });
});

app.use(errorHandler);
