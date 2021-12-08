import bodyParser from 'body-parser';
import { CronJob } from 'cron';
import { app, errorHandler } from 'mu';
import {
    CRON_PATTERN_HEALING_SYNC, DISABLE_AUTOMATIC_SYNC, DISABLE_INITIAL_SYNC, FILE_SYNC_JOB_OPERATION, SERVICE_NAME
} from './config';
import {
    STATUS_BUSY
} from './lib/constants';
import { waitForDatabase } from './lib/database';
import { getFilesReadyForRemoval, getFilesReadyForSync } from './lib/file-sync-job-utils';
import { failJob, getJobs } from './lib/job';
import { ProcessingQueue } from './lib/processing-queue';
import { runFilesCreation, runFilesRemoval } from './pipelines/files-sync';

const fileSyncQueue = new ProcessingQueue('file-sync-queue');

/**
 * BACKGROUND JOBS
 **/

console.log(`Status DISABLE_INITIAL_SYNC: ${DISABLE_INITIAL_SYNC}`);
if(!DISABLE_INITIAL_SYNC){
  waitForDatabase(scheduleFullSync);
}

new CronJob(CRON_PATTERN_HEALING_SYNC, async function() {
  if(DISABLE_AUTOMATIC_SYNC) {
    console.log('Sync disabled, doing nothing');
  }
  else {
    const now = new Date().toISOString();
    console.info(`Delta healing sync triggered by cron job at ${now}`);
    await scheduleFullSync();
  }
}, null, true);

/**
 * API
 **/

app.use( bodyParser.json( { type: function(req) { return /^application\/json/.test( req.get('content-type') ); } } ) );

app.get('/', function(req, res) {
  res.send(`Hello, you have reached ${SERVICE_NAME}! I'm doing just fine :)`);
});

app.post('/delta', async function(req, res){
  if(DISABLE_AUTOMATIC_SYNC){
    console.log(`Status of DISABLE_AUTOMATIC_SYNC: ${DISABLE_AUTOMATIC_SYNC}`);
  }
  else {
    const changeSets = req.body;
    for(const changeSet of changeSets){
      const deletes = [ ...new Set(changeSet.deletes.map(t => t.subject.value)) ];
      const inserts = [ ...new Set(changeSet.inserts.map(t => t.subject.value)) ];
      fileSyncQueue.addJob(async () => {

        const filesToRemove = deletes.length ? await getFilesReadyForRemoval(deletes) : [] ;
        const filesToAdd = inserts.length ? await getFilesReadyForSync(inserts) : [];

        if(filesToAdd.length || filesToRemove.length){
          await ensureNoPendingJobs();
        }
        if(filesToRemove.length){
          await runFilesRemoval(filesToRemove);
        }
        if(filesToAdd.length){
          await runFilesCreation(filesToAdd);
        }
      });
    }
  }
  res.status(202).send();
});

/**
 * DEBUG-API
 **/

app.post('/file-sync-jobs', async function( _, res ){
  await scheduleFullSync();
  res.send({ msg: 'Scheduled file sync job' });
});

app.use(errorHandler);

/**
 * HELPERS
 **/
async function ensureNoPendingJobs(){
  console.log(`Verify whether there are hanging jobs`);
  const jobs = await getJobs(FILE_SYNC_JOB_OPERATION, [ STATUS_BUSY ]);
  console.log(`Found ${jobs.length} hanging jobs, failing them first`);

  for(const job of jobs){
    await failJob(job.job);
  }
}

async function scheduleFullSync(){
  fileSyncQueue.addJob(async () => {
    await ensureNoPendingJobs();
    const filesToRemove = await getFilesReadyForRemoval();
    if(filesToRemove.length){
      await runFilesRemoval(filesToRemove);
    }
    const filesToAdd = await getFilesReadyForSync();
    if(filesToAdd.length){
      await runFilesCreation(filesToAdd);
    }
    console.log(`Full sync finished`);
  });
}
