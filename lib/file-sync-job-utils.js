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
      BIND(${sparqlEscapeUri(pFile)} as ?pFile)
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
          ?vFile dct:source ?pFile.
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

export async function getFilesReadyForRemoval( sourceUris = [] ) {
  let results = await getRegularFilesReadyForRemoval(sourceUris);
  results = [...results, ...await getCachedFilesReadyForRemoval(sourceUris) ];
  results = [ ... results, ...await getFormDataReadyForRemoval(sourceUris) ];
  return results;
}

export async function getRegularFilesReadyForRemoval( sourceUris = [] ){
  const queryStr = subjectToBind => {
    const subjectBind = subjectToBind ? `BIND(${sparqlEscapeUri(subjectToBind)} as ?oldPFile)` : '';
    return `
    ${PREFIXES}

    SELECT DISTINCT ?pFile ?vFile WHERE {
      ${subjectBind}
      GRAPH ?g {
       ?pFile a nfo:FileDataObject;
         nie:dataSource ?vFile;
         <http://purl.org/dc/terms/replaces> ?oldPFile.
      }
      FILTER NOT EXISTS {
        GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
         ?oldPFile a ?type.
        }
      }
    }
   `;
  };
  return await helpGetFileData(sourceUris, queryStr, 'RegularFile');
}

export async function getCachedFilesReadyForRemoval( sourceUris = [] ){
  const queryStr = subjectToBind => {
    const subjectBind = subjectToBind ? `BIND(${sparqlEscapeUri(subjectToBind)} as ?oldPFile)` : '';
    return `
    ${PREFIXES}

    SELECT DISTINCT ?pFile ?vFile WHERE {
      ${subjectBind}
      GRAPH ?g {
       ?pFile a nfo:LocalFileDataObject;
           nie:dataSource ?vFile;
         <http://purl.org/dc/terms/replaces> ?oldPFile.
      }
      FILTER NOT EXISTS {
        GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
         ?oldPFile a ?type.
        }
      }
    }
   `;
  };
  return await helpGetFileData(sourceUris, queryStr, 'CachedFile');
}

export async function getFormDataReadyForRemoval( sourceUris = [] ){
  const queryStr = subjectToBind => {
    const subjectBind = subjectToBind ? `BIND(${sparqlEscapeUri(subjectToBind)} as ?oldPFile)` : '';
    //Note this is a very implicit fetch, the model forces us to do so.
    //`?vFile dct:source ?pFile.` will still exist, because has been mapped in the previous sync.
    // AND assumed it has a new name
    return `
    ${PREFIXES}

    SELECT DISTINCT ?pFile ?vFile WHERE {
      ${subjectBind}
      GRAPH ?g {
       ?pFile a nfo:FileDataObject;
         <http://purl.org/dc/terms/replaces> ?oldPFile.
      }

      ?vFile dct:source ?pFile.

      FILTER NOT EXISTS {
        GRAPH ${sparqlEscapeUri(SOURCE_FILES_DATA_GRAPH)}{
         ?oldPFile a ?type.
        }
      }
    }
   `;
  };
  return await helpGetFileData(sourceUris, queryStr, 'FormDataFile');
}

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
        ?newPFile a nfo:FileDataObject;
          <http://purl.org/dc/terms/replaces> ?pFile.
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
        ?newPFile a nfo:LocalFileDataObject;
          <http://purl.org/dc/terms/replaces> ?pFile.
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
        ?newPFile a nfo:FileDataObject;
          <http://purl.org/dc/terms/replaces> ?pFile.
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

export function calculateNewFileData(fileData){
  //TODO: make paths configurable
  const { fileType } = fileData;
  const newData = {};
  newData.newPUuid = uuid();
  if(fileType == 'RegularFile' || fileType == 'CachedFile' ){
    newData.newPuri = `share://${newData.newPUuid}.${fileData.extension.replace('.', '')}`;

  }
  else if(fileType == 'FormDataFile'){
   newData.newPuri = `share://submissions/${newData.newPUuid}.${fileData.extension.replace('.', '') || 'ttl'}`; //TODO: fix the source data.
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
