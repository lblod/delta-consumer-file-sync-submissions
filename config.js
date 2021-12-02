// CONFIGURATION
export const DOWNLOAD_FILE_PATH = process.env.DCR_DOWNLOAD_FILE_PATH || '/delta-files-share/download?uri=:uri';
export const CRON_PATTERN_FILE_SYNC = process.env.CRON_PATTERN_FILE_SYNC || '0 * * * * *'; // every minute
export const SERVICE_NAME = process.env.SERVICE_NAME || 'delta-consumer-file-sync-submissions';
export const FILE_SYNC_JOB_OPERATION = process.env.FILE_SYNC_JOB_OPERATION
  || 'http://redpencil.data.gift/id/jobs/concept/JobOperation/deltas/consumer/physicalFileSync';
export const DISABLE_AUTOMATIC_SYNC = process.env.DISABLE_AUTOMATIC_SYNC == 'true' ? true : false;

// GRAPHS
export const JOBS_GRAPH = process.env.JOBS_GRAPH || 'http://mu.semte.ch/graphs/system/jobs';
export const JOB_CREATOR_URI = process.env.JOB_CREATOR_URI || 'http://data.lblod.info/services/id/delta-consumer-file-sync-submissions';

// MANDATORY SIMPLE
if(!process.env.FILES_ENDPOINT_BASE_URL)
  throw `Expected 'FILES_ENDPOINT_BASE_URL' to be provided.`;
export const FILES_ENDPOINT_BASE_URL = process.env.FILES_ENDPOINT_BASE_URL;

if(!process.env.SOURCE_FILES_DATA_GRAPH)
  throw `Expected 'SOURCE_FILES_DATA_GRAPH' to be provided.`;
export const SOURCE_FILES_DATA_GRAPH = process.env.SOURCE_FILES_DATA_GRAPH;

// COMPOSED VARIABLES
export const FILE_DOWNLOAD_URL = `${FILES_ENDPOINT_BASE_URL}${DOWNLOAD_FILE_PATH}`;
