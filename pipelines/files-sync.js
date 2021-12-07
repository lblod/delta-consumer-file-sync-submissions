import { FILE_SYNC_JOB_OPERATION, JOBS_GRAPH, JOB_CREATOR_URI, SERVICE_NAME } from '../config';
import { FILE_DELETE_TASK_OPERATION, STATUS_BUSY, STATUS_FAILED, STATUS_SUCCESS } from '../lib/constants';
import { createError, createJobError } from '../lib/error';
import { calculateNewFileData, deleteFile, deleteFileMeta, downloadFile, getFileDataToDelete, getFileDataTosync, publishPhysicalFile } from '../lib/file-sync-job-utils';
import { createFileSyncTask } from '../lib/file-sync-task';
import { createJob, failJob, getJobs } from '../lib/job';
import { updateStatus } from '../lib/utils';


export async function syncFile(fileUrisToDelete, fileUrisToSync) {
  let job;

  try {
    //Note: it is ok to fail these, because we assume it is running in a queue. So there is no way
    // a job in status busy was effectively doing something
    await ensureNoPendingJobs();

    const fileDataToSync = await getFileDataToProcessHelper(fileUrisToSync, getFileDataTosync);
    const fileDataToDelete = await getFileDataToProcessHelper(fileUrisToDelete, getFileDataToDelete);

    if(!(fileDataToSync.length || fileDataToDelete.length)) {
      console.log('No fileUris found which were ready for sync, or to delete Doing nothing');
    }
    else {

      job = await createJob(JOBS_GRAPH, FILE_SYNC_JOB_OPERATION, JOB_CREATOR_URI, STATUS_BUSY);

      let parentTask;

      //Delete files which have been entirely deleted on the source
      for(const [index, fileData] of fileDataToDelete.entries()){
        console.log(`Deleting file ${fileData.pFile}`);

        const task = await createFileSyncTask(JOBS_GRAPH, job, `${index}`, STATUS_BUSY, fileData, parentTask, FILE_DELETE_TASK_OPERATION);
        try {
          await deleteFileMeta(fileData);
          await deleteFile(fileData);
        }
        catch(error){
          //TODO: log error
          console.error(`Task ${task} failed`);
          console.error(error);
          await updateStatus(task, STATUS_FAILED);
        }

        await updateStatus(task, STATUS_SUCCESS);
        parentTask = task;
        console.log(`Sucessfully deleted file file ${fileData.pFile}`);
      }

      //Sync files which have been published entirely on the source

      //Note: the index depends on the previous tasks, so using the length of the previous as an offset
      const indexOffset = fileDataToDelete.length;
      for(const [ index, fileData ] of fileDataToSync.entries()) {
        console.log(`Ingesting file created on ${fileData.pFile}`);
        const task = await createFileSyncTask(JOBS_GRAPH, job, `${indexOffset + index}`, STATUS_BUSY, fileData, parentTask);
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
  console.log(`Verify whether there are hanging jobs`);
  const jobs = await getJobs(FILE_SYNC_JOB_OPERATION, [ STATUS_BUSY ]);
  console.log(`Found ${jobs.length} hanging jobs, failing them first`);

  for(const job of jobs){
    await failJob(job.job);
  }
}

async function getFileDataToProcessHelper(fileUris, queryFunction){
  const results = [];
  for(const fileUri of fileUris){
    const fileData = await queryFunction(fileUri);
    if(fileData){
      results.push(fileData);
    }
  }
  return results;
}
