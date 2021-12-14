import { updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import { FILE_SYNC_TASK_OPERATION, PREFIXES } from './constants';
import { createTask } from './task';

export async function createFileSyncTask(graph, job, index, status, fileData, parentTask, operation = FILE_SYNC_TASK_OPERATION) {
  const task = await createTask( graph,
                                 job,
                                 index,
                                 operation,
                                 status,
                                 parentTask ? [ parentTask ] : []
                               );


  const id = uuid();
  const containerUri = `http://data.lblod.info/id/dataContainers/${id}`;

  const addContainerQuery = `
   ${PREFIXES}

   INSERT DATA {
     GRAPH ${sparqlEscapeUri(graph)} {
      ${sparqlEscapeUri(containerUri)} a nfo:DataContainer.
      ${sparqlEscapeUri(containerUri)} dct:subject <http://redpencil.data.gift/id/concept/DeltaSync/FileSyncData>.
      ${sparqlEscapeUri(containerUri)} mu:uuid ${sparqlEscapeString(id)}.
      ${sparqlEscapeUri(containerUri)} task:hasPhysicalFile ${sparqlEscapeUri(fileData.pFile)}.
      ${sparqlEscapeUri(task)} task:resultsContainer ${sparqlEscapeUri(containerUri)}.
      ${sparqlEscapeUri(task)} task:inputContainer ${sparqlEscapeUri(containerUri)}.
     }
    }
  `;

  await update(addContainerQuery);

  return task;
}
