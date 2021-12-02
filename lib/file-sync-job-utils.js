import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import fs from 'fs';
import { sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDateTime, uuid } from 'mu';
import fetch from 'node-fetch';
import path from 'path';
import { FILE_DOWNLOAD_URL, SOURCE_FILES_DATA_GRAPH } from '../config';
import { PREFIXES } from './constants';
import { parseResult } from './utils';

export async function getNewFilesToSync() {
  const regularFilesQuery = `
    ${PREFIXES}

    SELECT DISTINCT ?pFile ?vFile ?fileUuid ?fileName ?format ?fileSize ?extension ?created ?modified WHERE {
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
        ?newPFile a nfo:FileDataObject;
          <http://purl.org/dc/terms/replaces> ?pFile.
      }
    }
   `;

  const regularFiles = parseResult(await query(regularFilesQuery));
  regularFiles.forEach(e => e.fileType = 'RegularFile');

  const cachedFilesQuery = `
    ${PREFIXES}

    SELECT DISTINCT ?pFile ?vFile ?fileUuid ?fileName ?format ?fileSize ?extension WHERE {
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
        ?newPFile a nfo:LocalFileDataObject;
          <http://purl.org/dc/terms/replaces> ?pFile.
      }
    }
   `;

  const cachedFiles = parseResult(await query(cachedFilesQuery));
  cachedFiles.forEach(e => e.fileType = 'CachedFile');


  const formDataQuery = `
    ${PREFIXES}

    SELECT DISTINCT ?pFile ?vFile ?dctType ?fileUuid ?fileName ?format ?fileSize ?extension ?created ?modified WHERE {
      GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
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
        ?newPFile a nfo:FileDataObject;
          <http://purl.org/dc/terms/replaces> ?pFile.
      }
    }
  `;

  const formDataFiles = parseResult(await query(formDataQuery));
  formDataFiles.forEach(e => e.fileType = 'FormDataFile');

  return [ ...formDataFiles ];
}
export function calculateNewFileData(fileData){
  //TODO: probably per type of file this should differ. For now we just take it over. (no Time sorry)
  const newData = {};
  newData.newPuri = fileData.pFile;
  newData.newPUuid = fileData.fileUuid;
  newData.targetPath = fileData.pFile.replace("share://", "/share/");
  return newData;
}
export async function downloadFile(origData, targetData) {
  const targetPath = targetData.targetPath;
  const targetDir = path.dirname(targetData.targetPath);
  fs.mkdirSync(targetDir, { recursive: true });
  const downloadUrl = FILE_DOWNLOAD_URL.replace(':uri', origData.pFile);
  await download(downloadUrl, targetPath);
}

export async function publishPhysicalFile(oldFileData, newFileData) {
  const { fileType } = oldFileData;
  if(fileType == 'RegularFile'){
    await publishRegularFile(oldFileData, newFileData);
  }
  else if(fileType == 'CachedFile'){
    await publishCachedFile(oldFileData, newFileData);
  }
  else if(fileType == 'FormDataFile'){
    await publishFormDataFile(oldFileData, newFileData);
  }
  else {
    throw "Unrecognized fileType, probably there is a tyo";
  }
}

async function publishRegularFile(oldFileData, newFileData){
  const { pFile, vFile, fileName, format, fileSize, extension, created, modified } = oldFileData;
  const { newPuri, newPUuid } = newFileData;

  const updateQuery = `
    ${PREFIXES}
    INSERT {
      GRAPH ?vFileGraph {
        ${sparqlEscapeUri(newPuri)} a nfo:FileDataObject;
          <http://purl.org/dc/terms/replaces> ${sparqlEscapeUri(pFile)};
          nie:dataSource ${sparqlEscapeUri(vFile)};
          mu:uuid ${sparqlEscapeString(newPUuid)} ;
          nfo:fileName ${sparqlEscapeString(fileName)} ;
          dct:format ${sparqlEscapeString(format)} ;
          nfo:fileSize ${sparqlEscapeInt(fileSize)} ;
          dbpedia:fileExtension ${sparqlEscapeString(extension)} ;
          dct:created ${sparqlEscapeDateTime(created)} ;
          dct:modified ${sparqlEscapeDateTime(modified)} .
     }
    }
    WHERE {
      BIND(${sparqlEscapeUri(pFile)} as ?oldPFile)
      GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
        ?oldPFile nie:dataSource ?vFile.
      }
      GRAPH ?vFileGraph {
        ?vFile a nfo:FileDataObject.
      }
    }
  `;
  await update(updateQuery);
}

async function publishCachedFile(oldFileData, newFileData){
  const { pFile, vFile, fileName, format, fileSize, extension } = oldFileData;
  const { newPuri, newPUuid } = newFileData;

  const updateQuery = `
    ${PREFIXES}
    INSERT {
      GRAPH ?vFileGraph {
        ${sparqlEscapeUri(newPuri)} a nfo:FileDataObject;
          a nfo:LocalFileDataObject;
          <http://purl.org/dc/terms/replaces> ${sparqlEscapeUri(pFile)};
          nie:dataSource ${sparqlEscapeUri(vFile)};
          mu:uuid ${sparqlEscapeString(newPUuid)} ;
          nfo:fileName ${sparqlEscapeString(fileName)} ;
          dct:format ${sparqlEscapeString(format)} ;
          nfo:fileSize ${sparqlEscapeInt(fileSize)} ;
          dbpedia:fileExtension ${sparqlEscapeString(extension)}.
     }
    }
    WHERE {
      BIND(${sparqlEscapeUri(pFile)} as ?oldPFile)
      GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
        ?oldPFile nie:dataSource ?vFile.
      }
      GRAPH ?vFileGraph {
        ?vFile a nfo:RemoteDataObject.
      }
    }
  `;
  await update(updateQuery);
}

async function publishFormDataFile(oldFileData, newFileData){
  const { pFile, vFile, fileType, fileName, format, fileSize, extension, created, modified, dctType } = oldFileData;
  const { newPuri, newPUuid } = newFileData;

  let extensionTriple = '';
  if(extension){
    extensionTriple = `${sparqlEscapeUri(newPuri)} dbpedia:fileExtension ${sparqlEscapeString(extension)}.`;
  }

  const updateQuery = `
    ${PREFIXES}
    DELETE {
      GRAPH ?vFileGraph {
        ?vFile dct:source ?oldPFile.
      }
    }
    INSERT {

      GRAPH ?vFileGraph {
        ${sparqlEscapeUri(newPuri)} a nfo:FileDataObject;
          <http://purl.org/dc/terms/replaces> ?oldPFile ;
          mu:uuid ${sparqlEscapeString(newPUuid)} ;
          dct:type ${sparqlEscapeUri(dctType)};
          nfo:fileName ${sparqlEscapeString(fileName)} ;
          dct:format ${sparqlEscapeString(format)} ;
          nfo:fileSize ${sparqlEscapeInt(fileSize)} ;
          dct:created ${sparqlEscapeDateTime(created)} ;
          dct:modified ${sparqlEscapeDateTime(modified)} .

        ${extensionTriple}

        ?vFile dct:source ${sparqlEscapeUri(newPuri)}.
     }
    }
    WHERE {
      BIND(${sparqlEscapeUri(pFile)} as ?oldPFile)
      GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
        ?oldPFile a nfo:FileDataObject.
      }
      GRAPH ?vFileGraph {
        ?vFile a ext:SubmissionDocument;
          dct:source ?oldPFile.
      }
    }
  `;
  await update(updateQuery);
}

async function download(downloadUrl, targetPath) {
  try {
    const res = await fetch(downloadUrl);
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
