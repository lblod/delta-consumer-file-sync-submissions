import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import fs from 'fs';
import { sparqlEscapeDateTime, sparqlEscapeInt, sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import fetch from 'node-fetch';
import path from 'path';
import { FILE_DOWNLOAD_URL, SOURCE_FILES_DATA_GRAPH } from '../config';
import { PREFIXES } from './constants';
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
      BIND(${sparqlEscapeUri(pFile)}} as ?pFile})
      GRAPH ?g {
        ?pFile ?p ?o.
      }
    }
  `;
  await update(queryStr);

  if(fileData.type == 'FormDataFile'){
    const removeSourceQ = `
      ${PREFIXES}
      DELETE {
       GRAPH ?g {
         ?vFile dct:source ?pFile.
       }
      }
      WHERE {
        BIND(${sparqlEscapeString(pFile)} as ?pFile)
        GRAPH ?g {
          ?vFile a ext:SubmissionDocument;
            dct:source ?pFile.
        }
      }
    `;
    await update(removeSourceQ);
  }
}

export async function deleteFile(fileData){
  const filePath = fileData.pFile.replace("share://", "/share/");
  fs.unlinkSync(filePath);
}

export async function getFileDataToDelete(fileUri) {
  //TODO: we perhaps need more file meta data
  const subjectBind = `BIND(${sparqlEscapeUri(fileUri)} as ?pFile)`;

  const regularFilesQuery = `
    ${PREFIXES}

    SELECT DISTINCT ?pFile ?vFile WHERE {
      ${subjectBind}
      GRAPH ?g {
       ?pFile a nfo:FileDataObject;
           nie:dataSource ?vFile.
      }
      ?vFile a nfo:FileDataObject.
    }
   `;

  const regularFile = parseResult(await query(regularFilesQuery))[0];

  if(regularFile){
    regularFile.fileType = 'RegularFile';
    return regularFile;
  }

  const cachedFilesQuery = `
    ${PREFIXES}

    SELECT DISTINCT ?pFile ?vFile WHERE {
      ${subjectBind}
      GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
       ?pFile a nfo:LocalFileDataObject;
           nie:dataSource ?vFile.
      }
      ?vFile a nfo:RemoteDataObject.
    }
   `;

  const cachedFile = parseResult(await query(cachedFilesQuery))[0];
  if(cachedFile){
    cachedFile.fileType = 'CachedFile';
    return cachedFile;
  }

  const formDataQuery = `
    ${PREFIXES}

    SELECT DISTINCT ?pFile ?vFile WHERE {

      GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
      ${subjectBind}
       ?pFile a nfo:FileDataObject.
      }
      ?vFile a ext:SubmissionDocument;
        dct:source ?pFile.
    }
  `;

  const formDataFile = parseResult(await query(formDataQuery))[0];
  if(formDataFile){
    formDataFile.fileType = 'FormDataFile';
    return formDataFile;
  }

  return null;
}

export async function getUrisToDelete(){
  const queryStr = `
    ${PREFIXES}

    SELECT DISTINCT ?newFile WHERE {
       GRAPH ?g {
        ?newFile a ?type;
          <http://purl.org/dc/terms/replaces> ?pFile.
      }
      FILTER (?type IN (
                <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#FileDataObject>,
                <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#LocalFileDataObject>
                )
              )
      FILTER NOT EXISTS {
        GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
         ?pFile a ?type.
        }
      }
    }
  `;
  return (parseResult(await query(queryStr))).map(t => t.pFile);
}

export async function getUrisToSync(){
  const queryStr = `
    ${PREFIXES}

    SELECT DISTINCT ?pFile WHERE {
      GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
        ?pFile a ?type.
      }
      FILTER (?type IN (
                <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#FileDataObject>,
                <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#LocalFileDataObject>
                )
              )

      FILTER NOT EXISTS {
        ?pFile dct:type <http://data.lblod.gift/concepts/form-file-type>.
      }
      FILTER NOT EXISTS {
          ?newPFile a ?type;
            <http://purl.org/dc/terms/replaces> ?pFile.
      }
    }
  `;
  return (parseResult(await query(queryStr))).map(t => t.pFile);
}

export async function getFileDataTosync(fileUri) {
  const subjectBind = `BIND(${sparqlEscapeUri(fileUri)} as ?pFile)`;
  const regularFilesQuery = `
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
        ?newPFile a nfo:FileDataObject;
          <http://purl.org/dc/terms/replaces> ?pFile.
      }
    }
   `;

  const regularFile = parseResult(await query(regularFilesQuery))[0];

  if(regularFile){
    regularFile.fileType = 'RegularFile';
    return regularFile;
  }

  const cachedFilesQuery = `
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
        ?newPFile a nfo:LocalFileDataObject;
          <http://purl.org/dc/terms/replaces> ?pFile.
      }
    }
   `;

  const cachedFile = parseResult(await query(cachedFilesQuery))[0];
  if(cachedFile){
    cachedFile.fileType = 'CachedFile';
    return cachedFile;
  }

  const formDataQuery = `
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
        ?newPFile a nfo:FileDataObject;
          <http://purl.org/dc/terms/replaces> ?pFile.
      }
    }
  `;

  const formDataFile = parseResult(await query(formDataQuery))[0];
  if(formDataFile){
    formDataFile.fileType = 'FormDataFile';
    return formDataFile;
  }

  return null;
}

export function calculateNewFileData(fileData){
  //TODO: make paths configurable
  const { fileType } = fileData;
  const newData = {};
  newData.newPUuid = uuid();
  if(fileType == 'RegularFile' || fileType == 'CachedFile' ){
    newData.newPuri = `share://${newData.newPUuid}.${fileData.extension}`;

  }
  else if(fileType == 'FormDataFile'){
   newData.newPuri = `share://submission/${newData.newPUuid}.${fileData.extension || 'ttl'}`; //TODO: fix the source data.
  }
  else {
    throw "Unrecognized fileType, probably there is a typo";
  }
  newData.targetPath = newData.newPuri.replace("share://", "/share/");
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
    throw "Unrecognized fileType, probably there is a typo";
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
