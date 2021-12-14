import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import fs from 'fs';
import { sparqlEscapeDateTime, sparqlEscapeInt, sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import fetch from 'node-fetch';
import path from 'path';
import { FILE_DOWNLOAD_URL, SOURCE_FILES_DATA_GRAPH } from '../config';
import { PREFIXES, FILE_SYNCED } from './constants';
import { parseResult } from './utils';

export async function deleteFileMeta(fileData){
  const pFile = fileData.pFile;
  //TODO: this might be a bit too aggressive
  const queryStr = `
    ${PREFIXES}
    DELETE {
      GRAPH ?g {
        ?pFile ?p ?o.
      }
    }
    WHERE {
      BIND(${sparqlEscapeUri(pFile)} as ?pFile)
      GRAPH ?g {
        ?pFile ?p ?o.
      }
    }
  `;
  await update(queryStr);
}

export async function deleteFile(fileData){
  const filePath = fileData.pFile.replace("share://", "/share/");
  fs.unlinkSync(filePath);
}

/**
 * Gets the files ready for removal.
 * A file is considered ready for removal, if essential triples (depending the model) are absent in source graph.
 * See queries in the implentation for more details.
 * @param  {[type]} sourceUris If provided, it will filter for the specific soure <share://> uris.
 * @return {[Object]}     Array of objects containing information to execute the deletion
 */
export async function getFilesReadyForRemoval( sourceUris = [] ) {
  const queryStr = subjectToBind => {
    const subjectBind = subjectToBind ? `BIND(${sparqlEscapeUri(subjectToBind)} as ?pFile)` : '';
    return `
    ${PREFIXES}

    SELECT DISTINCT ?pFile WHERE {
      ${subjectBind}
      GRAPH ?g {
       ?pFile adms:status ${sparqlEscapeUri(FILE_SYNCED)}.
      }
      FILTER NOT EXISTS {
        GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
         ?pFile a ?type.
        }
      }
    }
   `;
  };
  return await helpGetFileData(sourceUris, queryStr, 'RegularFile');
}

/**
 * Gets the files ready for syncing.
 * A file is considered ready, if the expected model triples are present in the source graph
 * See queries in the implentation for more details.
 * @param  {[type]} sourceUris If provided, it will filter for the specific soure <share://> uris.
 * @return {[Object]}     Array of objects containing information to execute the sync
 */
export async function getFilesReadyForSync( sourceUris = [] ) {
  let results = await getRegularFilesReadyForSync(sourceUris);
  results = [...results, ...await getCachedFilesReadyForSync(sourceUris) ];
  results = [ ... results, ...await getFormDataReadyForSync(sourceUris) ];
  return results;
}

export async function getRegularFilesReadyForSync( sourceUris = [] ){
  const queryStr = subjectToBind => {
    const subjectBind = subjectToBind ? `BIND(${sparqlEscapeUri(subjectToBind)} as ?pFile)` : '';
    return `
    ${PREFIXES}

    SELECT DISTINCT ?pFile ?vFile ?fileUuid ?fileName ?format ?fileSize ?extension ?created ?modified WHERE {
      ${subjectBind}
      GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
       ?pFile a nfo:FileDataObject;
           nie:dataSource ?vFile;
           mu:uuid ?fileUuid ;
           nfo:fileName ?fileName ;
           dct:format ?format ;
           nfo:fileSize ?fileSize ;
           dbpedia:fileExtension ?extension ;
           dct:created ?created ;
           dct:modified ?modified .
      }

      ?vFile a nfo:FileDataObject.

      FILTER NOT EXISTS {
        ?pFile adms:status ${sparqlEscapeUri(FILE_SYNCED)}.
      }
    }
   `;
  };

  return await helpGetFileData(sourceUris, queryStr, 'RegularFile');
}

export async function getCachedFilesReadyForSync( sourceUris = [] ){
  const queryStr = subjectToBind => {
    const subjectBind = subjectToBind ? `BIND(${sparqlEscapeUri(subjectToBind)} as ?pFile)` : '';
    return `
    ${PREFIXES}

    SELECT DISTINCT ?pFile ?vFile ?fileUuid ?fileName ?format ?fileSize ?extension WHERE {
      ${subjectBind}
      GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
       ?pFile a nfo:LocalFileDataObject;
           nie:dataSource ?vFile;
           mu:uuid ?fileUuid ;
           nfo:fileName ?fileName ;
           dct:format ?format ;
           nfo:fileSize ?fileSize ;
           dbpedia:fileExtension ?extension.
      }

      ?vFile a nfo:RemoteDataObject.

      FILTER NOT EXISTS {
        ?pFile adms:status ${sparqlEscapeUri(FILE_SYNCED)}.
      }
    }
   `;
  };

  return await helpGetFileData(sourceUris, queryStr, 'CachedFile');
}

export async function getFormDataReadyForSync( sourceUris = [] ){
  const queryStr = subjectToBind => {
    const subjectBind = subjectToBind ? `BIND(${sparqlEscapeUri(subjectToBind)} as ?pFile)` : '';
    return `
    ${PREFIXES}

    SELECT DISTINCT ?pFile ?vFile ?dctType ?fileUuid ?fileName ?format ?fileSize ?extension ?created ?modified WHERE {

      GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
      ${subjectBind}
       ?pFile a nfo:FileDataObject;
           dct:type ?dctType;
           mu:uuid ?fileUuid;
           nfo:fileName ?fileName;
           dct:format ?format;
           nfo:fileSize ?fileSize;
           dct:created ?created;
           dct:modified ?modified .

       OPTIONAL { ?pFile dbpedia:fileExtension ?extension.}
      }

      ?vFile a ext:SubmissionDocument;
        dct:source ?pFile.

      FILTER (?dctType NOT IN (<http://data.lblod.gift/concepts/form-file-type>) )
      FILTER NOT EXISTS {
        ?pFile adms:status ${sparqlEscapeUri(FILE_SYNCED)}.
      }
    }
   `;
  };

  return await helpGetFileData(sourceUris, queryStr, 'FormDataFile');
}

export async function helpGetFileData( sourceUris = [], queryTemplate, fileType ){
  let results = [];
  if(!sourceUris.length){
    results = parseResult(await query(queryTemplate('')));
  }
  else {
    //We query one by one, since else it might be heavy on the database.
    for(const uri of sourceUris){
      results = [ ...parseResult(await query(queryTemplate(uri))), ...results];
    }
  }

  results.forEach(r => r.fileType = fileType);

  return results;
}

export async function downloadFile(fileData) {
  const targetPath = fileData.pFile.replace("share://", "/share/");
  const targetDir = path.dirname(targetPath);
  fs.mkdirSync(targetDir, { recursive: true });
  const downloadUrl = FILE_DOWNLOAD_URL.replace(':uri', fileData.pFile);
  await download(downloadUrl, targetPath);
}

export async function publishPhysicalFile(fileData) {
  const { fileType } = fileData;
  if(fileType == 'RegularFile'){
    await publishRegularFile(fileData);
  }
  else if(fileType == 'CachedFile'){
    await publishCachedFile(fileData);
  }
  else if(fileType == 'FormDataFile'){
    await publishFormDataFile(fileData);
  }
  else {
    throw "Unrecognized fileType, probably there is a typo";
  }
}

async function publishRegularFile(fileData){
  //Why using the fetched information, and not a simple insert
  //INSERT { <share://file> ?p ?o } WHERE { GRAPH <sourceFilesGraph> { <share://file> ?p ?o }
  //When doing so, the data might be already deleted in <sourceFilesGraph> potentially yielding inconsistent results.
  const { pFile, vFile, fileName, format, fileUuid, fileSize, extension, created, modified } = fileData;

  const updateQuery = `
    ${PREFIXES}
    INSERT {
      GRAPH ?vFileGraph {
       ?pFile a nfo:FileDataObject;
          adms:status ${sparqlEscapeUri(FILE_SYNCED)};
          nie:dataSource ${sparqlEscapeUri(vFile)};
          mu:uuid ${sparqlEscapeString(fileUuid)} ;
          nfo:fileName ${sparqlEscapeString(fileName)} ;
          dct:format ${sparqlEscapeString(format)} ;
          nfo:fileSize ${sparqlEscapeInt(fileSize)} ;
          dbpedia:fileExtension ${sparqlEscapeString(extension)} ;
          dct:created ${sparqlEscapeDateTime(created)} ;
          dct:modified ${sparqlEscapeDateTime(modified)} .
     }
    }
    WHERE {
      BIND(${sparqlEscapeUri(pFile)} as ?pFile)
      GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
        ?pFile nie:dataSource ?vFile.
      }
      GRAPH ?vFileGraph {
        ?vFile a nfo:FileDataObject.
      }
    }
  `;
  await update(updateQuery);
}

async function publishCachedFile(fileData){
  const { pFile, vFile, fileName, fileUuid, format, fileSize, extension } = fileData;

  const updateQuery = `
    ${PREFIXES}
    INSERT {
      GRAPH ?vFileGraph {
        ?pFile a nfo:FileDataObject;
          a nfo:LocalFileDataObject;
          adms:status ${sparqlEscapeUri(FILE_SYNCED)};
          nie:dataSource ${sparqlEscapeUri(vFile)};
          mu:uuid ${sparqlEscapeString(fileUuid)} ;
          nfo:fileName ${sparqlEscapeString(fileName)} ;
          dct:format ${sparqlEscapeString(format)} ;
          nfo:fileSize ${sparqlEscapeInt(fileSize)} ;
          dbpedia:fileExtension ${sparqlEscapeString(extension)}.
     }
    }
    WHERE {
      BIND(${sparqlEscapeUri(pFile)} as ?pFile)
      GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
        ?pFile nie:dataSource ?vFile.
      }
      GRAPH ?vFileGraph {
        ?vFile a nfo:RemoteDataObject.
      }
    }
  `;
  await update(updateQuery);
}

async function publishFormDataFile(fileData){
  const { pFile, vFile, fileType, fileUuid, fileName, format, fileSize, extension, created, modified, dctType } = fileData;

  let extensionTriple = '';
  if(extension){
    extensionTriple = `${sparqlEscapeUri(pFile)} dbpedia:fileExtension ${sparqlEscapeString(extension)}.`;
  }

  const updateQuery = `
    ${PREFIXES}
    INSERT {
      GRAPH ?vFileGraph {
        ?pFile a nfo:FileDataObject;
          adms:status ${sparqlEscapeUri(FILE_SYNCED)};
          mu:uuid ${sparqlEscapeString(fileUuid)} ;
          dct:type ${sparqlEscapeUri(dctType)};
          nfo:fileName ${sparqlEscapeString(fileName)} ;
          dct:format ${sparqlEscapeString(format)} ;
          nfo:fileSize ${sparqlEscapeInt(fileSize)} ;
          dct:created ${sparqlEscapeDateTime(created)} ;
          dct:modified ${sparqlEscapeDateTime(modified)} .

        ${extensionTriple}
     }
    }
    WHERE {
      BIND(${sparqlEscapeUri(pFile)} as ?pFile)
      GRAPH ?vFileGraph {
        ?vFile a ext:SubmissionDocument;
          dct:source ?pFile.
      }
    }
  `;
  await update(updateQuery);
}

async function download(downloadUrl, targetPath) {
  try {
    console.log(`Getting: ${downloadUrl}`);
    const res = await fetch(downloadUrl);

    if (!res.ok){
      throw Error(`Error ${res.status} for ${downloadUrl}`);
    }

    return new Promise((resolve, reject) => {
      const writeStream = fs.createWriteStream(targetPath);
      res.body.pipe(writeStream);
      writeStream.on('close', resolve);
      writeStream.on('error', reject);
    });
  }
  catch(e) {
    console.log(`Something went wrong while downloading file from ${downloadUrl}`);
    console.log(e);
    throw e;
  }
}
