import { FILE_SYNC_JOB_OPERATION, JOBS_GRAPH, JOB_CREATOR_URI, SERVICE_NAME } from '../config';
import { STATUS_BUSY, STATUS_FAILED, STATUS_SUCCESS } from '../lib/constants';
import { createError, createJobError } from '../lib/error';
import { downloadFile, getNewFileToSync, publishPhysicalFile, calculateNewFileData } from '../lib/file-sync-job-utils';
import { createFileSyncTask } from '../lib/file-sync-task';
import { createJob, failJob, getJobs } from '../lib/job';
import { updateStatus } from '../lib/utils';

export async function startSync(delta) {
  let job;

  try {
    await ensureNoPendingJobs();

    const filesData = await getNewFilesToSync(delta);
    if(!filesData.length) {
      console.log('No fileData found to sync. Doing nothing');
    }
    else {

      job = await createJob(JOBS_GRAPH, FILE_SYNC_JOB_OPERATION, JOB_CREATOR_URI, STATUS_BUSY);

      let parentTask;
      for(const [ index, fileData ] of filesData.entries()) {
        console.log(`Ingesting file created on ${fileData.pFile}`);

        const task = await createFileSyncTask(JOBS_GRAPH, job, `${index}`, STATUS_BUSY, fileData, parentTask);
        try {
          const newFileData = calculateNewFileData(fileData);
          await downloadFile(fileData, newFileData);
          //TODO: refine this, as to have real new URI's
          await publishPhysicalFile(fileData, newFileData);
        }
        catch(error){
          //TODO: log error
          console.error(`Task ${task} failed`);
          console.error(error);
          await updateStatus(task, STATUS_FAILED);
        }

        await updateStatus(task, STATUS_SUCCESS);
        parentTask = task;
        console.log(`Sucessfully ingested file ${fileData.pFile}`);

      }

      await updateStatus(job, STATUS_SUCCESS);
    }
  }
  catch(error){
     console.error(`General error: ${error}`);
    if(job){
      await createJobError(JOBS_GRAPH, job, error);
      await failJob(job);
    }
    else {
      await createError(JOBS_GRAPH, SERVICE_NAME, `Unexpected error while ingesting: ${error}`);
    }
  }
}

async function ensureNoPendingJobs(){
  //Note: it is ok to fail these, because we assume it is running in a queue. So there is no way
  // a job in status busy was effectively doing something
  console.log(`Verify whether there are hanging jobs`);
  const jobs = await getJobs(FILE_SYNC_JOB_OPERATION, [ STATUS_BUSY ]);
  console.log(`Found ${jobs.length} hanging jobs, failing them first`);

  for(const job of jobs){
    await failJob(job.job);
  }
}

async function getNewFilesToSync(delta){
  const inserts = delta.map(changeSet => changeSet.inserts).flat();
  const subjects = [ ...new Set(inserts.map(t => t.suject.value)) ];
  const files = [];
  for(const subject of subjects){
    const file = await getNewFileToSync(file);
    if(file){
      files.push(file);
    }
  }
  return files;
}
