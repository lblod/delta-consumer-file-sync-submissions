import { updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeDateTime, sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import { DELTA_SYNC_TASK_OPERATION, PREFIXES, FILE_SYNC_TASK_OPERATION } from './constants';
import { createTask } from './task';

export async function createFileSyncTask(graph, job, index, status, fileData, parentTask) {
  const task = await createTask( graph,
                                 job,
                                 index,
                                 FILE_SYNC_TASK_OPERATION,
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
      ${sparqlEscapeUri(containerUri)} task:hasFile ${sparqlEscapeUri(fileData.vFile)}.
      ${sparqlEscapeUri(task)} task:resultsContainer ${sparqlEscapeUri(containerUri)}.
      ${sparqlEscapeUri(task)} task:inputContainer ${sparqlEscapeUri(containerUri)}.
     }
    }
  `;

  await update(addContainerQuery);

  return task;
}
