import { FILE_SYNC_JOB_OPERATION, JOBS_GRAPH, JOB_CREATOR_URI, SERVICE_NAME } from '../config';
import { FILE_DELETE_TASK_OPERATION, STATUS_BUSY, STATUS_FAILED, STATUS_SUCCESS } from '../lib/constants';
import { createError, createJobError } from '../lib/error';
import { calculateNewFileData, deleteFile, deleteFileMeta, downloadFile, publishPhysicalFile } from '../lib/file-sync-job-utils';
import { createFileSyncTask } from '../lib/file-sync-task';
import { createJob, failJob } from '../lib/job';
import { updateStatus } from '../lib/utils';

export async function runFilesRemoval(filesData){
  let job;

  try {
    job = await createJob(JOBS_GRAPH, FILE_SYNC_JOB_OPERATION, JOB_CREATOR_URI, STATUS_BUSY);

    let parentTask;

    //Delete files which have been entirely deleted on the source
    for(const [index, fileData] of filesData.entries()){
      console.log(`Deleting file ${fileData.pFile}`);

      const task = await createFileSyncTask(JOBS_GRAPH, job, `${index}`, STATUS_BUSY, fileData, parentTask, FILE_DELETE_TASK_OPERATION);
      try {
        await deleteFileMeta(fileData);
        await deleteFile(fileData);
        await updateStatus(task, STATUS_SUCCESS);
        console.log(`Sucessfully deleted file file ${fileData.pFile}`);
      }
      catch(error){
        console.error(`Task ${task} failed`);
        console.error(error);
        await updateStatus(task, STATUS_FAILED);
      }
      parentTask = task;
    }
    await updateStatus(job, STATUS_SUCCESS);
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

export async function runFilesCreation(filesData){
  let job;

  try {
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
        await updateStatus(task, STATUS_SUCCESS);
        console.log(`Sucessfully ingested file ${fileData.pFile}`);
      }
      catch(error){
        //TODO: log error
        console.error(`Task ${task} failed`);
        console.error(error);
        await updateStatus(task, STATUS_FAILED);
      }
      parentTask = task;
    }
    await updateStatus(job, STATUS_SUCCESS);
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
