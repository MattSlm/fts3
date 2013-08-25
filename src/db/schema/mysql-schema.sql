SET storage_engine=INNODB;

--
-- Only one host at a time must run sanity checks
--
CREATE TABLE t_server_sanity (
  revertToSubmitted TINYINT(1) DEFAULT 0,
  cancelWaitingFiles TINYINT(1) DEFAULT 0,
  revertNotUsedFiles TINYINT(1) DEFAULT 0,
  forceFailTransfers TINYINT(1) DEFAULT 0,
  setToFailOldQueuedJobs TINYINT(1) DEFAULT 0,
  checkSanityState TINYINT(1) DEFAULT 0,
  t_revertToSubmitted          TIMESTAMP NULL DEFAULT NULL,
  t_cancelWaitingFiles          TIMESTAMP NULL DEFAULT NULL,
  t_revertNotUsedFiles          TIMESTAMP NULL DEFAULT NULL,
  t_forceFailTransfers          TIMESTAMP NULL DEFAULT NULL,
  t_setToFailOldQueuedJobs          TIMESTAMP NULL DEFAULT NULL,
  t_checkSanityState          TIMESTAMP NULL DEFAULT NULL   
); 
INSERT INTO t_server_sanity (revertToSubmitted,cancelWaitingFiles,revertNotUsedFiles,forceFailTransfers,setToFailOldQueuedJobs,checkSanityState,t_revertToSubmitted,t_cancelWaitingFiles,t_revertNotUsedFiles,t_forceFailTransfers,t_setToFailOldQueuedJobs,t_checkSanityState) values(0,0,0,0,0,0,UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP());

--
-- Holds various server configuration options
--
CREATE TABLE t_server_config (
  retry          INTEGER DEFAULT 0,
  max_time_queue INTEGER DEFAULT 0
);
INSERT INTO t_server_config (retry,max_time_queue) values(0,0);

--
-- Holds the optimizer mode
--
CREATE TABLE t_optimize_mode (
  mode_opt       INTEGER NOT NULL DEFAULT 1
);

--
-- Holds optimization parameters
--
CREATE TABLE t_optimize (
  auto_number	INTEGER AUTO_INCREMENT,
--
-- file id
  file_id      INTEGER NOT NULL,
--
-- source se
  source_se    VARCHAR(255),
--
-- dest se  
  dest_se      VARCHAR(255),
--
-- number of streams
  nostreams    INTEGER DEFAULT NULL,
--
-- timeout
  timeout      INTEGER DEFAULT NULL,
--
-- active transfers
  active       INTEGER DEFAULT NULL,
--
-- throughput
  throughput   FLOAT DEFAULT NULL,
--
-- tcp buffer size
  buffer       INTEGER DEFAULT NULL,   
--
-- the nominal size of the file (bytes)
  filesize     DOUBLE DEFAULT NULL,
--
-- timestamp
  datetime     TIMESTAMP,
  
  CONSTRAINT t_optimize_pk PRIMARY KEY (auto_number)
);

--
-- Historial optimizer evolution
--
CREATE TABLE t_optimizer_evolution (
    datetime     TIMESTAMP,
    source_se    VARCHAR(255),
    dest_se      VARCHAR(255),
    nostreams    INTEGER DEFAULT NULL,
    timeout      INTEGER DEFAULT NULL,
    active       INTEGER DEFAULT NULL,
    throughput   FLOAT DEFAULT NULL,
    buffer       INTEGER DEFAULT NULL,
    filesize     DOUBLE DEFAULT NULL
);
CREATE INDEX t_optimizer_source_and_dest ON t_optimizer_evolution(source_se, dest_se);

--
-- Holds certificate request information
--
CREATE TABLE t_config_audit (
--
-- timestamp
  datetime     TIMESTAMP,
--
-- dn
  dn           VARCHAR(1024),
--
-- what has changed
  config       VARCHAR(4000), 
--
-- action (insert/update/delete)
  action       VARCHAR(100)    
);


--
-- Configures debug mode for a given pair
--
CREATE TABLE t_debug (
--
-- source hostname
  source_se    VARCHAR(255),
--
-- dest hostanme
  dest_se      VARCHAR(255),
--
-- debug on/off
  debug        VARCHAR(3) DEFAULT 'off'
);


--
-- Holds certificate request information
--
CREATE TABLE t_credential_cache (
--
-- delegation identifier
  dlg_id       VARCHAR(100),
--
-- DN of delegated proxy owner
  dn           VARCHAR(255),
--
-- certificate request
  cert_request LONGTEXT,
--
-- private key of request
  priv_key     LONGTEXT,
--
-- list of voms attributes contained in delegated proxy
  voms_attrs   LONGTEXT,
--
-- set primary key
  CONSTRAINT cred_cache_pk PRIMARY KEY (dlg_id, dn)
);

--
-- Holds delegated proxies
--
CREATE TABLE t_credential (
--
-- delegation identifier
  dlg_id     VARCHAR(100),
--
-- DN of delegated proxy owner
  dn         VARCHAR(255),
--
-- delegated proxy certificate chain
  proxy      LONGTEXT,
--
-- list of voms attributes contained in delegated proxy
  voms_attrs LONGTEXT,
--
-- termination time of the credential
  termination_time TIMESTAMP NOT NULL,
--
-- set primary key
  CONSTRAINT cred_pk PRIMARY KEY (dlg_id, dn),
  INDEX (termination_time)
);

--
-- Schema version
--
CREATE TABLE t_credential_vers (
  major INTEGER NOT NULL,
  minor INTEGER NOT NULL,
  patch INTEGER NOT NULL
);
INSERT INTO t_credential_vers (major,minor,patch) VALUES (1,2,0);

--
-- SE from the information service, currently BDII
--

CREATE TABLE t_se (
-- The internal id
  se_id_info INTEGER AUTO_INCREMENT,
  name       VARCHAR(255) NOT NULL,
  endpoint   VARCHAR(1024),
  se_type    VARCHAR(30),
  site       VARCHAR(100),
  state      VARCHAR(30),
  version    VARCHAR(30),
-- This field will contain the host parse for FTS and extracted from name 
  host       VARCHAR(100),
  se_transfer_type     VARCHAR(30),
  se_transfer_protocol VARCHAR(30),
  se_control_protocol  VARCHAR(30),
  gocdb_id             VARCHAR(100),
  KEY (se_id_info),
  CONSTRAINT se_info_pk PRIMARY KEY (name)
);

-- 
-- relation of SE and VOs
--
CREATE TABLE t_se_acl (
  name VARCHAR(255),
  vo   VARCHAR(32),
  CONSTRAINT se_acl_pk PRIMARY KEY (name, vo)
);

-- GROUP NAME and its members
CREATE TABLE t_group_members (
  groupName VARCHAR(255) NOT NULL,
  member    VARCHAR(255) NOT NULL UNIQUE,
  CONSTRAINT t_group_members_pk PRIMARY KEY (groupName, member),
  CONSTRAINT t_group_members_fk FOREIGN KEY (member) REFERENCES t_se (name)  
); 

-- SE HOSTNAME / GROUP NAME / *

CREATE TABLE t_link_config ( 
  source               VARCHAR(255) NOT NULL,
  destination          VARCHAR(255) NOT NULL,
  state                VARCHAR(30)  NOT NULL,
  symbolicName         VARCHAR(255) NOT NULL UNIQUE,
  nostreams            INTEGER NOT NULL,
  tcp_buffer_size      INTEGER DEFAULT 0,
  urlcopy_tx_to        INTEGER NOT NULL,
  no_tx_activity_to    INTEGER DEFAULT 360,
  auto_tuning		   VARCHAR(3) check (auto_tuning in ('on', 'off', 'all')),
  placeholder1         INTEGER,
  placeholder2         INTEGER,  
  placeholder3         VARCHAR(255),
  CONSTRAINT t_link_config_pk PRIMARY KEY (source, destination)    
);

CREATE TABLE t_share_config ( 
  source       VARCHAR(255) NOT NULL,
  destination  VARCHAR(255) NOT NULL,
  vo           VARCHAR(100) NOT NULL,
  active       INTEGER NOT NULL,
  CONSTRAINT t_share_config_pk PRIMARY KEY (source, destination, vo),
  CONSTRAINT t_share_config_fk FOREIGN KEY (source, destination) REFERENCES t_link_config (source, destination) ON DELETE CASCADE
);

--
-- blacklist of bad SEs that should not be transferred to
--
CREATE TABLE t_bad_ses (
--
-- The hostname of the bad SE   
  se             VARCHAR(256),
--
-- The reason this host was added 
  message        VARCHAR(2048) DEFAULT NULL,
--
-- The time the host was added
  addition_time  TIMESTAMP NULL DEFAULT NULL,
--
-- The DN of the administrator who added it
  admin_dn       VARCHAR(1024),
  --
-- VO that is banned for the SE
   vo			 VARCHAR(50) DEFAULT NULL,
--
-- status: either CANCEL or WAIT or WAIT_AS
   status 		 VARCHAR(10) DEFAULT NULL,
--
-- the timeout that is used when WAIT status was specified
   wait_timeout  INTEGER default 0,
  CONSTRAINT bad_se_pk PRIMARY KEY (se)
);

--
-- blacklist of bad DNs that should not be transferred to
--
CREATE TABLE t_bad_dns (
--
-- The hostname of the bad SE   
  dn              VARCHAR(256),
--
-- The reason this host was added 
  message        VARCHAR(2048) DEFAULT NULL,
--
-- The time the host was added
  addition_time  TIMESTAMP NULL DEFAULT NULL,
--
-- The DN of the administrator who added it
  admin_dn       VARCHAR(1024),
--
-- status: either CANCEL or WAIT
  status 		 VARCHAR(10) DEFAULT NULL,
--
-- the timeout that is used when WAIT status was specified
  wait_timeout  INTEGER default 0,
  CONSTRAINT bad_dn_pk PRIMARY KEY (dn)
);


--
-- Store se_pair ACL
--
CREATE TABLE t_se_pair_acl (
--
-- the name of the se_pair
  se_pair_name  VARCHAR(32),
--
-- The principal name
  principal     VARCHAR(255) NOT NULL,
--
-- Set Primary Key
  CONSTRAINT se_pair_acl_pk PRIMARY KEY (se_pair_name, principal)
);

--
-- Store VO ACL
--
CREATE TABLE t_vo_acl (
--
-- the name of the VO
  vo_name     VARCHAR(50) NOT NULL,
--
-- The principal name
  principal   VARCHAR(255) NOT NULL,
--
-- Set Primary Key
  CONSTRAINT vo_acl_pk PRIMARY KEY (vo_name, principal)
);

--
-- t_job contains the list of jobs currently in the transfer database.
--
CREATE TABLE t_job (
--
-- the job_id, a IETF UUID in string form.
  job_id               CHAR(36) NOT NULL PRIMARY KEY,
--
-- The state the job is currently in
  job_state            VARCHAR(32) NOT NULL,
--
-- Session reuse for this job. Allowed values are Y, (N), NULL
  reuse_job            VARCHAR(3), 
--
-- Canceling flag. Allowed values are Y, (N), NULL
  cancel_job           CHAR(1),
--
-- Transport specific parameters
  job_params           VARCHAR(255),
--
-- Source SE host name
  source_se            VARCHAR(255),
--
-- Dest SE host name
  dest_se              VARCHAR(255),  
--
-- the DN of the user starting the job - they are the only one
-- who can sumbit/cancel
  user_dn              VARCHAR(1024) NOT NULL,
--
-- the DN of the agent currently serving the job
  agent_dn             VARCHAR(1024),
--
-- the user credentials passphrase. This is passed to the movement service in
-- order to retrieve the appropriate user proxy to do the transfers
  user_cred            VARCHAR(255),
--
-- The user's credential delegation id
  cred_id              VARCHAR(100),
--
-- Blob to store user capabilites and groups
  voms_cred            LONGTEXT,
--
-- The VO that owns this job
  vo_name              VARCHAR(50),
--
-- The reason the job is in the current state
  reason               VARCHAR(2048),
--
-- The time that the job was submitted
  submit_time          TIMESTAMP NULL DEFAULT NULL,
--
-- The time that the job was in a terminal state
  finish_time          TIMESTAMP NULL DEFAULT NULL,
--
-- Priority for Intra-VO Scheduling
  priority             INTEGER DEFAULT 3,
--
-- Submitting FTS hostname
  submit_host          VARCHAR(255),
--
-- Maximum time in queue before start of transfer (in seconds)
  max_time_in_queue    INTEGER,
--
-- The Space token to be used for the destination files
  space_token          VARCHAR(255),
--
-- The Storage Service Class to be used for the destination files
  storage_class        VARCHAR(255),
--
-- The endpoint of the MyProxy server that should be used if the
-- legacy cert retrieval is used
  myproxy_server       VARCHAR(255),
--
-- Internal job parameters,used to pass job specific data from the
-- WS to the agent
  internal_job_params  VARCHAR(255),
--
-- Overwrite flag for job
  overwrite_flag       CHAR(1) DEFAULT NULL,
--
-- this timestamp will be set when the job enter in one of the terminal 
-- states (Finished, FinishedDirty, Failed, Canceled). Use for table
-- partitioning
  job_finished         TIMESTAMP NULL DEFAULT NULL,
--
--  Space token of the source files
--
  source_space_token   VARCHAR(255),
--
-- description used by the agents to eventually get the source token. 
--
  source_token_description VARCHAR(255), 
-- *** New in 3.3.0 ***
--
-- pin lifetime of the copy of the file created after a successful srmPutDone
-- or srmCopy operations, in seconds
  copy_pin_lifetime        INTEGER DEFAULT NULL,
--
-- fail the transfer immediately if the file location is NEARLINE (do not even
-- start the transfer). The default is false.
  fail_nearline            CHAR(1) DEFAULT NULL,
--
-- Specified is the checksum is required on the source and destination, destination or none
  checksum_method          CHAR(1) DEFAULT NULL,
 --
 -- Specifies how many configurations were assigned to the transfer-job
  configuration_count      INTEGER default NULL,
--
-- Bringonline timeout
  bring_online INTEGER default NULL,
--
-- retry
  retry INTEGER default 0,
--
-- retry delay
  retry_delay INTEGER default 0,
--
-- Job metadata
  job_metadata VARCHAR(255)     
);
  
  
--
-- t_file stores the actual file transfers - one row per source/dest pair
--
CREATE TABLE t_file (
-- file_id is a unique identifier for a (source, destination) pair with a
-- job.  It is created automatically.
--
  file_id          INTEGER PRIMARY KEY AUTO_INCREMENT,
-- the file index is used in case multiple sources/destinations were provided for one file
-- entries with the same file_index and same file_id are pointing to the same file 
-- (but use different protocol)
  file_index       INTEGER,
--
-- job_id (used in joins with file table)
  job_id           CHAR(36) NOT NULL,
--
-- The state of this file
  file_state       VARCHAR(32) NOT NULL,
--
-- The Source Logical Name
  logical_name     VARCHAR(1100),
--
-- The Source Logical Name
  symbolicName     VARCHAR(255),  
--
-- Hostname which this file was transfered
  transferHost     VARCHAR(255),
--
-- The Source
  source_surl      VARCHAR(1100),
--
-- The Destination
  dest_surl        VARCHAR(1100),
--
-- Source SE host name
  source_se            VARCHAR(255),
--
-- Dest SE host name
  dest_se              VARCHAR(255),  
--
-- The agent who is transferring the file. This is only valid when the file
-- is in 'Active' state
  agent_dn         VARCHAR(1024),
--
-- The error scope
  error_scope      VARCHAR(32),
--
-- The FTS phase when the error happened
  error_phase      VARCHAR(32),
--
-- The class for the reason field
  reason_class     VARCHAR(32),
--
-- The reason the file is in this state
  reason           VARCHAR(2048),
--
-- Total number of failures (including transfer,catalog and prestaging errors)
  num_failures     INTEGER,
--
-- Number of transfer failures in last attemp cycle (reset at the Hold->Pending transition)
  current_failures INTEGER,
--
-- Number of catalog failures (not reset at the Hold->Pending transition)
  catalog_failures INTEGER,
--
-- Number of prestaging failures (reset at the Hold->Pending transition)
  prestage_failures  INTEGER,
--
-- the nominal size of the file (bytes)
  filesize           DOUBLE,
--
-- the user-defined checksum of the file "checksum_type:checksum"
  checksum           VARCHAR(100),
--
-- the timestamp when the file is in a terminal state
  finish_time       TIMESTAMP NULL DEFAULT NULL,
--
-- the timestamp when the file is in a terminal state
  start_time        TIMESTAMP NULL DEFAULT NULL,  
--
-- internal file parameters for storing information between retry attempts
  internal_file_params  VARCHAR(255),
--
-- this timestamp will be set when the job enter in one of the terminal 
-- states (Finished, FinishedDirty, Failed, Canceled). Use for table
-- partitioning
  job_finished          TIMESTAMP NULL DEFAULT NULL,
--
-- the pid of the process which is executing the file transfer
  pid                   INTEGER,
--
-- transfer duration
  tx_duration           DOUBLE,
--
-- Average throughput
  throughput            FLOAT,
--
-- How many times should the transfer be retried 
  retry                 INTEGER DEFAULT 0,
  
--
-- user provided size of the file (bytes)
-- we use DOUBLE because SOCI truncates BIGINT to int32
  user_filesize  DOUBLE,  
  
--
-- File metadata
  file_metadata   VARCHAR(255),
  
--
-- selection strategy used in case when multiple protocols were provided
  selection_strategy VARCHAR(255),
--
-- Staging start timestamp
  staging_start   TIMESTAMP NULL DEFAULT NULL,  
--
-- Staging finish timestamp
  staging_finished   TIMESTAMP NULL DEFAULT NULL,
--
-- bringonline token
  bringonline_token VARCHAR(255),
--
-- the timestamp that the file will be retried
  retry_timestamp          TIMESTAMP NULL DEFAULT NULL,
--
--
  wait_timestamp		TIMESTAMP NULL DEFAULT NULL,
--
--
  wait_timeout			INTEGER,

  t_log_file        VARCHAR(2048),
  t_log_file_debug  INTEGER,
    
  FOREIGN KEY (job_id) REFERENCES t_job(job_id)
);

--
-- Keep error reason that drove to retries
--
CREATE TABLE t_file_retry_errors (
    file_id   INTEGER NOT NULL,
    attempt   INTEGER NOT NULL,
    datetime  TIMESTAMP,
    reason    VARCHAR(2048),
    CONSTRAINT t_file_retry_errors_pk PRIMARY KEY(file_id, attempt),
    CONSTRAINT t_file_retry_fk FOREIGN KEY (file_id) REFERENCES t_file(file_id) ON DELETE CASCADE
);
CREATE INDEX t_file_retry_fid ON t_file_retry_errors (file_id);

-- 
-- t_file_share_config the se configuration to be used by the job
--
CREATE TABLE t_file_share_config (
  file_id         INTEGER       NOT NULL,
  source          VARCHAR(255)   NOT NULL,
  destination     VARCHAR(255)   NOT NULL,
  vo              VARCHAR(100)   NOT NULL,
  CONSTRAINT t_file_share_config_pk PRIMARY KEY (file_id, source, destination, vo),
  CONSTRAINT t_share_config_fk1 FOREIGN KEY (source, destination, vo) REFERENCES t_share_config (source, destination, vo) ON DELETE CASCADE,
  CONSTRAINT t_share_config_fk2 FOREIGN KEY (file_id) REFERENCES t_file (file_id) ON DELETE CASCADE
);


--
-- t_stage_req table stores the data related to a file orestaging request
--
CREATE TABLE t_stage_req (
--
-- vo name
   vo_name           VARCHAR(255) NOT NULL
-- hostname
   ,host           VARCHAR(255) NOT NULL			
--
-- parallel bringonline ops
  ,concurrent_ops INTEGER DEFAULT 0
  
-- Set primary key
  ,CONSTRAINT stagereq_pk PRIMARY KEY (vo_name, host)
);



--
--
-- Index Section 
--
--
-- t_job indexes:
-- t_job(job_id) is primary key
CREATE INDEX job_job_state    ON t_job(job_state);
CREATE INDEX job_vo_name      ON t_job(vo_name);
CREATE INDEX job_cred_id      ON t_job(user_dn,cred_id);
CREATE INDEX job_jobfinished_id     ON t_job(job_finished);

-- t_file indexes:
-- t_file(file_id) is primary key
CREATE INDEX file_job_id     ON t_file(job_id);
CREATE INDEX file_jobfinished_id ON t_file(job_finished);
CREATE INDEX file_source_state ON t_file(source_se, file_state);
CREATE INDEX file_dest_state ON t_file(dest_se, file_state);
CREATE INDEX file_state_dest_surl ON t_file(file_state, dest_surl);
CREATE INDEX job_reuse  ON t_job(reuse_job);



CREATE INDEX optimize_source_a         ON t_optimize(source_se,dest_se);


CREATE INDEX t_server_config_max_time         ON t_server_config(max_time_queue);
CREATE INDEX t_server_config_retry         ON t_server_config(retry);



-- Config index

CREATE INDEX idx_debug      ON t_debug (debug);
CREATE INDEX idx_source      ON t_debug (source_se);
CREATE INDEX idx_dest      ON t_debug (dest_se);
-- 
--
-- Schema version
--
CREATE TABLE t_schema_vers (
  major INTEGER NOT NULL,
  minor INTEGER NOT NULL,
  patch INTEGER NOT NULL,
  --
  -- save a state when upgrading the schema
  state VARCHAR(24)
);
INSERT INTO t_schema_vers (major,minor,patch) VALUES (1,0,0);


-- Saves the bother of writing down again the same schema
CREATE TABLE t_file_backup AS (SELECT * FROM t_file);
CREATE TABLE t_job_backup  AS (SELECT * FROM t_job);

CREATE INDEX t_job_backup_1            ON t_job_backup(job_id);
CREATE INDEX t_file_backup_1            ON t_file_backup(file_id, job_id);

-- Profiling information
CREATE TABLE t_profiling_info (
    period  INT NOT NULL,
    updated TIMESTAMP NOT NULL
);

CREATE TABLE t_profiling_snapshot (
    scope      VARCHAR(255) NOT NULL PRIMARY KEY,
    cnt        LONG NOT NULL,
    exceptions LONG NOT NULL,
    total      DOUBLE NOT NULL,
    average    DOUBLE NOT NULL
);

CREATE INDEX t_prof_snapshot_total ON t_profiling_snapshot(total);

