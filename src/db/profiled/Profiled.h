/********************************************//**
 * Copyright @ Members of the EMI Collaboration, 2010.
 * See www.eu-emi.eu for details on the copyright holders.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***********************************************/

#pragma once

#include <common_dev.h>
#include <iomanip>
#include <map>
#include "GenericDbIfce.h"

/**
 * This class is intended to wrap implementations of the DB,
 * profiling their times.
 */
class ProfiledDB : public GenericDbIfce
{
private:
    GenericDbIfce *db;
    void (*destroy_db)(void *);

public:
    ProfiledDB(GenericDbIfce* db, void (*destroy_db)(void *));
    ~ProfiledDB();

    void init(std::string username, std::string password, std::string connectString, int pooledConn);

    void submitPhysical(const std::string & jobId, std::vector<job_element_tupple> src_dest_pair, const std::string & paramFTP,
                        const std::string & DN, const std::string & cred, const std::string & voName, const std::string & myProxyServer,
                        const std::string & delegationID, const std::string & spaceToken, const std::string & overwrite,
                        const std::string & sourceSpaceToken, const std::string & sourceSpaceTokenDescription, int copyPinLifeTime,
                        const std::string & failNearLine, const std::string & checksumMethod, const std::string & reuse,
                        int bringonline, std::string metadata,
                        int retry, int retryDelay, std::string sourceSe, std::string destinationSe);


    void getTransferJobStatus(std::string requestID, bool archive, std::vector<JobStatus*>& jobs);

    void getTransferFileStatus(std::string requestID, bool archive,
                               unsigned offset, unsigned limit, std::vector<FileTransferStatus*>& files);

    void listRequests(std::vector<JobStatus*>& jobs, std::vector<std::string>& inGivenStates,
                      std::string restrictToClientDN, std::string forDN, std::string VOname);

    TransferJobs* getTransferJob(std::string jobId, bool archive);

    void getByJobIdReuse(std::vector<TransferJobs*>& jobs, std::map< std::string, std::list<TransferFiles*> >& files, bool reuse);

    void getByJobId(std::map< std::string, std::list<TransferFiles*> >& files);

    void getSe(Se* &se, std::string seName);

    unsigned int updateFileStatus(TransferFiles* file, const std::string status);

    void addSe(std::string endpoint, std::string se_type, std::string site, std::string name, std::string state, std::string version, std::string host,
               std::string se_transfer_type, std::string se_transfer_protocol, std::string se_control_protocol, std::string gocdb_id);

    void updateSe(std::string endpoint, std::string se_type, std::string site, std::string name, std::string state, std::string version, std::string host,
                  std::string se_transfer_type, std::string se_transfer_protocol, std::string se_control_protocol, std::string gocdb_id);

    void deleteSe(std::string NAME);

    bool updateFileTransferStatus(double throughput, std::string job_id, int file_id, std::string transfer_status, std::string transfer_message, int process_id, double filesize, double duration);

    bool updateJobTransferStatus(int file_id, std::string job_id, const std::string status);

    void updateFileTransferProgress(std::string job_id, int file_id, double throughput, double transferred);

    void cancelJob(std::vector<std::string>& requestIDs);

    void getCancelJob(std::vector<int>& requestIDs);

    void insertGrDPStorageCacheElement(std::string dlg_id, std::string dn, std::string cert_request, std::string priv_key, std::string voms_attrs);

    void updateGrDPStorageCacheElement(std::string dlg_id, std::string dn, std::string cert_request, std::string priv_key, std::string voms_attrs);

    CredCache* findGrDPStorageCacheElement(std::string delegationID, std::string dn);

    void deleteGrDPStorageCacheElement(std::string delegationID, std::string dn);

    void insertGrDPStorageElement(std::string dlg_id, std::string dn, std::string proxy, std::string voms_attrs, time_t termination_time);

    void updateGrDPStorageElement(std::string dlg_id, std::string dn, std::string proxy, std::string voms_attrs, time_t termination_time);

    Cred* findGrDPStorageElement(std::string delegationID, std::string dn);

    void deleteGrDPStorageElement(std::string delegationID, std::string dn);

    bool getDebugMode(std::string source_hostname, std::string destin_hostname);

    void setDebugMode(std::string source_hostname, std::string destin_hostname, std::string mode);

    void getSubmittedJobsReuse(std::vector<TransferJobs*>& jobs, const std::string & vos);

    void auditConfiguration(const std::string & dn, const std::string & config, const std::string & action);

    void fetchOptimizationConfig2(OptimizerSample* ops, const std::string & source_hostname, const std::string & destin_hostname);

    bool updateOptimizer(double throughput, int file_id , double filesize, double timeInSecs, int nostreams, int timeout, int buffersize,std::string source_hostname, std::string destin_hostname);

    void initOptimizer(const std::string & source_hostname, const std::string & destin_hostname, int file_id);

    bool isCredentialExpired(const std::string & dlg_id, const std::string & dn);

    bool isTrAllowed(const std::string & source_se, const std::string & dest);

    int getSeOut(const std::string & source, const std::set<std::string> & destination);

    int getSeIn(const std::set<std::string> & source, const std::string & destination);

    void setAllowed(const std::string & job_id, int file_id, const std::string & source_se, const std::string & dest, int nostreams, int timeout, int buffersize);

    void setAllowedNoOptimize(const std::string & job_id, int file_id, const std::string & params);

    bool terminateReuseProcess(const std::string & jobId);

    void forceFailTransfers(std::map<int, std::string>& collectJobs);

    void setPid(const std::string & jobId, int fileId, int pid);

    void setPidV(int pid, std::map<int,std::string>& pids);

    void revertToSubmitted();

    void backup();

    void forkFailedRevertState(const std::string & jobId, int fileId);

    void forkFailedRevertStateV(std::map<int,std::string>& pids);

    bool retryFromDead(std::vector<struct message_updater>& messages);

    void blacklistSe(std::string se, std::string vo, std::string status, int timeout, std::string msg, std::string adm_dn);

    void blacklistDn(std::string dn, std::string msg, std::string adm_dn);

    void unblacklistSe(std::string se);

    void unblacklistDn(std::string dn);

    bool isSeBlacklisted(std::string se, std::string vo);

    bool allowSubmitForBlacklistedSe(std::string se);

    boost::optional<int> getTimeoutForSe(std::string se);

    bool isDnBlacklisted(std::string dn);

    bool isFileReadyState(int fileID);

    bool isFileReadyStateV(std::map<int,std::string>& fileIds);

    bool checkGroupExists(const std::string & groupName);

    void getGroupMembers(const std::string & groupName, std::vector<std::string>& groupMembers);

    void addMemberToGroup(const std::string & groupName, std::vector<std::string>& groupMembers);

    void deleteMembersFromGroup(const std::string & groupName, std::vector<std::string>& groupMembers);

    std::string getGroupForSe(const std::string se);


    void submitHost(const std::string & jobId);

    std::string transferHost(int fileId);

    std::string transferHostV(std::map<int,std::string>& fileIds);

    void addLinkConfig(LinkConfig* cfg);
    void updateLinkConfig(LinkConfig* cfg);
    void deleteLinkConfig(std::string source, std::string destination);
    LinkConfig* getLinkConfig(std::string source, std::string destination);
    bool isThereLinkConfig(std::string source, std::string destination);
    std::pair<std::string, std::string>* getSourceAndDestination(std::string symbolic_name);
    bool isGrInPair(std::string group);
    bool isShareOnly(std::string se);

    void addShareConfig(ShareConfig* cfg);
    void updateShareConfig(ShareConfig* cfg);
    void deleteShareConfig(std::string source, std::string destination, std::string vo);
    void deleteShareConfig(std::string source, std::string destination);
    ShareConfig* getShareConfig(std::string source, std::string destination, std::string vo);
    std::vector<ShareConfig*> getShareConfig(std::string source, std::string destination);

    bool checkIfSeIsMemberOfAnotherGroup( const std::string & member);

    void addFileShareConfig(int file_id, std::string source, std::string destination, std::string vo);

    void getFilesForNewSeCfg(std::string source, std::string destination, std::string vo, std::vector<int>& out);

    void getFilesForNewGrCfg(std::string source, std::string destination, std::string vo, std::vector<int>& out);

    void delFileShareConfig(int file_id, std::string source, std::string destination, std::string vo);

    void delFileShareConfig(std::string groupd, std::string se);

    bool hasStandAloneSeCfgAssigned(int file_id, std::string vo);

    bool hasPairSeCfgAssigned(int file_id, std::string vo);

    bool hasStandAloneGrCfgAssigned(int file_id, std::string vo);

    bool hasPairGrCfgAssigned(int file_id, std::string vo);

    int countActiveTransfers(std::string source, std::string destination, std::string vo);

    int countActiveOutboundTransfersUsingDefaultCfg(std::string se, std::string vo);

    int countActiveInboundTransfersUsingDefaultCfg(std::string se, std::string vo);

    int sumUpVoShares (std::string source, std::string destination, std::set<std::string> vos);

    bool checkConnectionStatus();

    void setPriority(std::string jobId, int priority);

    void setRetry(int retry);

    int getRetry(const std::string & jobId);

    int getRetryTimes(const std::string & jobId, int fileId);

    void setRetryTransfer(const std::string & jobId, int fileId);

    int getMaxTimeInQueue();

    void setMaxTimeInQueue(int afterXHours);

    void setToFailOldQueuedJobs(std::vector<std::string>& jobs);

    std::vector< std::pair<std::string, std::string> > getPairsForSe(std::string se);

    std::vector<std::string> getAllStandAlloneCfgs();

    std::vector<std::string> getAllShareOnlyCfgs();

    int activeProcessesForThisHost();

    std::vector< std::pair<std::string, std::string> > getAllPairCfgs();

    void setFilesToNotUsed(std::string jobId, int fileIndex, std::vector<int>& files);

    std::vector< boost::tuple<std::string, std::string, int> >  getVOBringonlimeMax();

    std::vector<struct message_bringonline> getBringOnlineFiles(std::string voName, std::string hostName, int maxValue);

    void bringOnlineReportStatus(const std::string & state, const std::string & message, struct message_bringonline msg);

    void addToken(const std::string & job_id, int file_id, const std::string & token);

    void getCredentials(std::string & vo_name, const std::string & job_id, int file_id, std::string & dn, std::string & dlg_id);

    void setMaxStageOp(const std::string& se, const std::string& vo, int val);

    void useFileReplica(std::string jobId, int fileId);

    double getSuccessRate(std::string source, std::string destination);

    double getAvgThroughput(std::string source, std::string destination);

    void updateProtocol(const std::string& jobId, int fileId, int nostreams, int timeout, int buffersize, double filesize);

    void cancelFilesInTheQueue(const std::string& se, const std::string& vo, std::set<std::string>& jobs);

    void cancelJobsInTheQueue(const std::string& dn, std::vector<std::string>& jobs);

    void transferLogFile(const std::string& filePath, const std::string& jobId, int fileId, bool debug);

    struct message_state getStateOfTransfer(const std::string& jobId, int fileId);

    void getFilesForJob(const std::string& jobId, std::vector<int>& files);

    void getFilesForJobInCancelState(const std::string& jobId, std::vector<int>& files);

    void setFilesToWaiting(const std::string& se, const std::string& vo, int timeout);

    void setFilesToWaiting(const std::string& dn, int timeout);

    void cancelWaitingFiles(std::set<std::string>& jobs);

    void revertNotUsedFiles();

    void checkSanityState();

    void checkSchemaLoaded();

    void storeProfiling(const fts3::ProfilingSubsystem* prof);

    void setOptimizerMode(int mode);

    void setRetryTransfer(const std::string & jobId, int fileId, int retry, const std::string& reason);

    void getTransferRetries(int fileId, std::vector<FileRetry*>& retries);

    void updateHeartBeat(unsigned* index, unsigned* count, unsigned* start, unsigned* end);
};


void destroy_profiled_db(void *db);
