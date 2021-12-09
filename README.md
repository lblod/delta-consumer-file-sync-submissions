# delta-consumer-file-sync-submissions

POC style service responsible for syncing files related to submissions
It is very likely this is subject for improvement, this in terms of robustness, genericity, speed and authorization.
But we have to start somewhere.
In our defense, the model is rather complicated (and not respecting the file model defined in [mu-file-service](https://github.com/mu-semtech/file-service)).

## Flow
 - Periodic job checks wether there are physical files to be synced
 - If found, download, map to logical file and 'publishes' the new information

## Assumptions
- Files are publicly available (note: they are cosnsidered public)
- Files follow a subset of the submissions-model see [here](https://github.com/lblod/import-submission-service)
- Files are availble on the API defined by [file-service-share](https://github.com/redpencilio/file-service-share).
- Only addition and deletion full files is supported.

## Configuration
### Add the service to a stack.

Add the following to your `docker-compose.yml`:

```yaml
consumer:
  image: lblod/delta-consumer-file-sync-submissions
  environment:
    FILES_ENDPOINT_BASE_URL: "http://files.example.com"
    SOURCE_FILES_DATA_GRAPH: "http://mu.semte.ch/graphs/delta-submissions-original-physical-files"
  volumes:
    - ./data/files:/share
```
### Environment variables
- `DISABLE_INITIAL_SYNC (default: false)`: disables the initial sync
- `DOWNLOAD_FILE_PATH (default: '/delta-files-share/download?uri=:id')`:  path to download [file-service-share](https://github.com/redpencilio/file-service-share)
- `CRON_PATTERN_HEALING_SYNC (default '0 5 4 * * *')`:  cron pattern to trigger healing job
- `SERVICE_NAME (default 'delta-consumer-file-sync-submissions')`: name of the service
- `FILE_SYNC_JOB_OPERATION (default: http://redpencil.data.gift/id/jobs/concept/JobOperation/deltas/consumer/physicalFileSync)`:  name of the job to sync files
- `DISABLE_AUTOMATIC_SYNC (default: false)`: mainly for debugging purposes
- `JOBS_GRAPH (default: 'http://mu.semte.ch/graphs/system/jobs')`: name of the graph where jobs are stored
- `JOB_CREATOR_URI (default: 'http://data.lblod.info/services/id/delta-consumer-file-sync-submissions')`: the name of the job creator
- `FILES_ENDPOINT_BASE_URL [REQUIRED]`: endpoint where files can be fetched
- `SOURCE_FILES_DATA_GRAPH [REQUIRED]`: Graph where original phyiscal file information is stored.

### API
There is a little debugger API available. Please check `app.js` to see how it works.

### Model
#### prefixes
```
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX oslc: <http://open-services.net/ns/core#>
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX adms: <http://www.w3.org/ns/adms#>
```

#### Job
The instance of a process or group of processes (workflow).

##### class
`cogs:Job`

##### properties

Name | Predicate | Range | Definition
--- | --- | --- | ---
uuid |mu:uuid | xsd:string
creator | dct:creator | rdfs:Resource
status | adms:status | adms:Status
created | dct:created | xsd:dateTime
modified | dct:modified | xsd:dateTime
jobType | task:operation | skos:Concept
error | task:error | oslc:Error

#### Task
Subclass of `cogs:Job`

##### class
`task:Task`

##### properties

Name | Predicate | Range | Definition
--- | --- | --- | ---
uuid |mu:uuid | xsd:string
status | adms:status | adms:Status
created | dct:created | xsd:dateTime
modified | dct:modified | xsd:dateTime
operation | task:operation | skos:Concept
index | task:index | xsd:string | May be used for orderering. E.g. : '1', '2.1', '2.2', '3'
error | task:error | oslc:Error
parentTask| cogs:dependsOn | task:Task
job | dct:isPartOf | rdfs:Resource | Refer to the parent job
resultsContainer | task:resultsContainer | nfo:DataContainer | An generic type, optional
inputContainer | task:inputContainer | nfo:DataContainer | An generic type, optional


#### DataContainer
A generic container gathering information about what has been processed. The consumer needs to determine how to handle it.
See also: [job-controller-service](https://github.com/lblod/job-controller-service) for a more standardized use.

##### class
`nfo:DataContainer`

##### properties

Name | Predicate | Range | Definition
--- | --- | --- | ---
uuid |mu:uuid | xsd:string
subject | dct:subject | skos:Concept | Provides some information about the content
hasFile | task:hasFile | the virtual file we are syncing

#### Error

##### class
`oslc:Error`

##### properties
Name | Predicate | Range | Definition
--- | --- | --- | ---
uuid |mu:uuid | xsd:string
message | oslc:message | xsd:string

## TODOs
- update files
- `<share://extractedTriples> nie:dataSource <share://harvested.html>` is not synced properly. (i.e. triples from harvested files)
- Some easy configurable security
