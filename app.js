import bodyParser from 'body-parser';
import { CronJob } from 'cron';
import { app, errorHandler } from 'mu';
import {
    CRON_PATTERN_HEALING_SYNC, DISABLE_AUTOMATIC_SYNC, DISABLE_INITIAL_SYNC, SERVICE_NAME
} from './config';
import { waitForDatabase } from './lib/database';
import { getUrisToDelete, getUrisToSync } from './lib/file-sync-job-utils';
import { ProcessingQueue } from './lib/processing-queue';
import { syncFile } from './pipelines/files-sync';

const fileSyncQueue = new ProcessingQueue('file-sync-queue');

/**
 * BACKGROUND JOBS
 **/

console.log(`Status DISABLE_INITIAL_SYNC: ${DISABLE_INITIAL_SYNC}`);
if(!DISABLE_INITIAL_SYNC){
  waitForDatabase(() => {
    fileSyncQueue.addJob(async () => {
      await syncFile(await getUrisToDelete(), await getUrisToSync());
      console.log(`Initial sync was success`);
    });
  });
}

new CronJob(CRON_PATTERN_HEALING_SYNC, async function() {
  if(DISABLE_AUTOMATIC_SYNC) {
    console.log('Sync disabled, doing nothing');
  }
  else {
    const now = new Date().toISOString();
    console.info(`Delta healing sync triggered by cron job at ${now}`);
    fileSyncQueue.addJob(async () => {
      await syncFile(await getUrisToDelete(), await getUrisToSync());
      console.log(`Healing sync was success`);
    });
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
        //The information is based on the oldUri, the pipeline expects the mapped Uri
        const urisToDelete = await getUrisToDelete(deletes);
        await syncFile(urisToDelete, inserts);
      });
    }
  }
  res.status(202).send();
});

/**
 * DEBUG-API
 **/

app.post('/file-sync-jobs', async function( _, res ){
  fileSyncQueue.addJob(async () => {
    await syncFile(await getUrisToDelete(), await getUrisToSync());
  });
  res.send({ msg: 'Started file sync job' });
});

app.use(errorHandler);
