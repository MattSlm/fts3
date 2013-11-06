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
#include <soci.h>
#include "GenericDbIfce.h"

using namespace FTS3_COMMON_NAMESPACE;

class MySqlAPI : public GenericDbIfce
{
public:
    MySqlAPI();
    virtual ~MySqlAPI();

    class CleanUpSanityChecks
    {
    public:
        CleanUpSanityChecks(MySqlAPI* instanceLocal, soci::session& sql, struct message_sanity &msg):instanceLocal(instanceLocal), sql(sql), msg(msg), returnValue(false)
        {
            returnValue = instanceLocal->assignSanityRuns(sql, msg);
        }

        ~CleanUpSanityChecks()
        {
            instanceLocal->resetSanityRuns(sql, msg);
        }

        bool getCleanUpSanityCheck()
        {
            return returnValue;
        }

        MySqlAPI* instanceLocal;
        soci::session& sql;
        struct message_sanity &msg;
        bool returnValue;
    };


    /**
     * Intialize database connection  by providing information from fts3config file
     **/

    virtual void init(std::string username, std::string password, std::string connectString, int pooledConn);

    /**
     * Submit a transfer request to be stored in the database
     **/
    virtual void submitPhysical(const std::string & jobId, std::vector<job_element_tupple> src_dest_pair,
                                const std::string & DN, const std::string & cred,
                                const std::string & voName, const std::string & myProxyServer, const std::string & delegationID,
                                const std::string & sourceSe, const std::string & destinationSe,
                                const JobParameterHandler & params);

    virtual void getTransferJobStatus(std::string requestID, bool archive, std::vector<JobStatus*>& jobs);

    virtual void getTransferFileStatus(std::string requestID, bool archive, unsigned offset, unsigned limit, std::vector<FileTransferStatus*>& files);

    virtual void listRequests(std::vector<JobStatus*>& jobs, std::vector<std::string>& inGivenStates, std::string restrictToClientDN, std::string forDN, std::string VOname);

    virtual TransferJobs* getTransferJob(std::string jobId, bool archive);

    virtual void getSubmittedJobsReuse(std::vector<TransferJobs*>& jobs, const std::string & vos);

    virtual void getByJobIdReuse(std::vector<TransferJobs*>& jobs, std::map< std::string, std::list<TransferFiles*> >& files);

    virtual void getByJobId(std::map< std::string, std::list<TransferFiles*> >& files);

    virtual void getSe(Se* &se, std::string seName);

    virtual unsigned int updateFileStatus(TransferFiles* file, const std::string status);

    virtual void addSe(std::string ENDPOINT, std::string SE_TYPE, std::string SITE, std::string NAME, std::string STATE, std::string VERSION, std::string HOST,
                       std::string SE_TRANSFER_TYPE, std::string SE_TRANSFER_PROTOCOL, std::string SE_CONTROL_PROTOCOL, std::string GOCDB_ID);

    virtual void updateSe(std::string ENDPOINT, std::string SE_TYPE, std::string SITE, std::string NAME, std::string STATE, std::string VERSION, std::string HOST,
                          std::string SE_TRANSFER_TYPE, std::string SE_TRANSFER_PROTOCOL, std::string SE_CONTROL_PROTOCOL, std::string GOCDB_ID);

    virtual void deleteSe(std::string NAME);

    virtual bool updateFileTransferStatus(double throughput, std::string job_id, int file_id, std::string transfer_status, std::string transfer_message, int process_id, double filesize, double duration);

    virtual bool updateJobTransferStatus(int file_id, std::string job_id, const std::string status);

    virtual void updateFileTransferProgress(std::string job_id, int file_id, double throughput, double transferred);

    virtual void cancelJob(std::vector<std::string>& requestIDs);

    virtual void getCancelJob(std::vector<int>& requestIDs);


    /*t_credential API*/
    virtual void insertGrDPStorageCacheElement(std::string dlg_id, std::string dn, std::string cert_request, std::string priv_key, std::string voms_attrs);

    virtual void updateGrDPStorageCacheElement(std::string dlg_id, std::string dn, std::string cert_request, std::string priv_key, std::string voms_attrs);

    virtual CredCache* findGrDPStorageCacheElement(std::string delegationID, std::string dn);

    virtual void deleteGrDPStorageCacheElement(std::string delegationID, std::string dn);

    virtual void insertGrDPStorageElement(std::string dlg_id, std::string dn, std::string proxy, std::string voms_attrs, time_t termination_time);

    virtual void updateGrDPStorageElement(std::string dlg_id, std::string dn, std::string proxy, std::string voms_attrs, time_t termination_time);

    virtual  Cred* findGrDPStorageElement(std::string delegationID, std::string dn);

    virtual void deleteGrDPStorageElement(std::string delegationID, std::string dn);

    virtual bool getDebugMode(std::string source_hostname, std::string destin_hostname);

    virtual void setDebugMode(std::string source_hostname, std::string destin_hostname, std::string mode);

    virtual void auditConfiguration(const std::string & dn, const std::string & config, const std::string & action);

    virtual void fetchOptimizationConfig2(OptimizerSample* ops, const std::string & source_hostname, const std::string & destin_hostname);

    virtual bool updateOptimizer(double throughput, int file_id , double filesize, double timeInSecs, int nostreams, int timeout, int buffersize,std::string source_hostname, std::string destin_hostname);

    virtual void initOptimizer(const std::string & source_hostname, const std::string & destin_hostname, int file_id);

    virtual bool isCredentialExpired(const std::string & dlg_id, const std::string & dn);

    virtual bool isTrAllowed(const std::string & source_se, const std::string & dest);

    virtual bool isTrAllowed2(const std::string & source_se, const std::string & dest);

    virtual int getSeOut(const std::string & source, const std::set<std::string> & destination);

    virtual int getSeIn(const std::set<std::string> & source, const std::string & destination);

    virtual int getCredits(const std::string & source_hostname, const std::string & destination_hostname);

    virtual void setAllowed(const std::string & job_id, int file_id, const std::string & source_se, const std::string & dest, int nostreams, int timeout, int buffersize);

    virtual void setAllowedNoOptimize(const std::string & job_id, int file_id, const std::string & params);

    virtual bool terminateReuseProcess(const std::string & jobId);

    virtual void forceFailTransfers(std::map<int, std::string>& collectJobs);

    virtual void setPid(const std::string & jobId, int fileId, int pid);

    virtual void setPidV(int pid, std::map<int,std::string>& pids);

    virtual void revertToSubmitted();

    virtual void backup(long* nJobs, long* nFiles);

    virtual void forkFailedRevertState(const std::string & jobId, int fileId);

    virtual void forkFailedRevertStateV(std::map<int,std::string>& pids);

    virtual bool retryFromDead(std::vector<struct message_updater>& messages);

    virtual void blacklistSe(std::string se, std::string vo, std::string status, int timeout, std::string msg, std::string adm_dn);

    virtual void blacklistDn(std::string dn, std::string msg, std::string adm_dn);

    virtual void unblacklistSe(std::string se);

    virtual void unblacklistDn(std::string dn);

    virtual bool isSeBlacklisted(std::string se, std::string vo);

    virtual bool allowSubmitForBlacklistedSe(std::string se);

    virtual boost::optional<int> getTimeoutForSe(std::string se);

    virtual bool isDnBlacklisted(std::string dn);

    virtual bool isFileReadyState(int fileID);

    virtual bool isFileReadyStateV(std::map<int,std::string>& fileIds);

    //t_group_members
    virtual  bool checkGroupExists(const std::string & groupName);

    virtual void getGroupMembers(const std::string & groupName, std::vector<std::string>& groupMembers);

    virtual void addMemberToGroup(const std::string & groupName, std::vector<std::string>& groupMembers);

    virtual void deleteMembersFromGroup(const std::string & groupName, std::vector<std::string>& groupMembers);

    virtual std::string getGroupForSe(const std::string se);

    //t_config_symbolic
    virtual void addLinkConfig(LinkConfig* cfg);
    virtual void updateLinkConfig(LinkConfig* cfg);
    virtual void deleteLinkConfig(std::string source, std::string destination);
    virtual LinkConfig* getLinkConfig(std::string source, std::string destination);
    virtual bool isThereLinkConfig(std::string source, std::string destination);
    virtual std::pair<std::string, std::string>* getSourceAndDestination(std::string symbolic_name);
    virtual bool isGrInPair(std::string group);
    virtual bool isShareOnly(std::string se);

    virtual void addShareConfig(ShareConfig* cfg);
    virtual void updateShareConfig(ShareConfig* cfg);
    virtual void deleteShareConfig(std::string source, std::string destination, std::string vo);
    virtual void deleteShareConfig(std::string source, std::string destination);
    virtual ShareConfig* getShareConfig(std::string source, std::string destination, std::string vo);
    virtual std::vector<ShareConfig*> getShareConfig(std::string source, std::string destination);

    virtual void addActivityConfig(std::string vo, std::string shares, bool active);
    virtual void updateActivityConfig(std::string vo, std::string shares, bool active);
    virtual void deleteActivityConfig(std::string vo);
    virtual bool isActivityConfigActive(std::string vo);
    virtual std::map< std::string, double > getActivityConfig(std::string vo);

    virtual bool checkIfSeIsMemberOfAnotherGroup( const std::string & member);

    virtual void addFileShareConfig(int file_id, std::string source, std::string destination, std::string vo);

    virtual void getFilesForNewSeCfg(std::string source, std::string destination, std::string vo, std::vector<int>& out);

    virtual void getFilesForNewGrCfg(std::string source, std::string destination, std::string vo, std::vector<int>& out);

    virtual void delFileShareConfig(int file_id, std::string source, std::string destination, std::string vo);

    virtual void delFileShareConfig(std::string groupd, std::string se);

    virtual bool hasStandAloneSeCfgAssigned(int file_id, std::string vo);

    virtual bool hasPairSeCfgAssigned(int file_id, std::string vo);

    virtual bool hasStandAloneGrCfgAssigned(int file_id, std::string vo);

    virtual bool hasPairGrCfgAssigned(int file_id, std::string vo);

    virtual int countActiveTransfers(std::string source, std::string destination, std::string vo);

    virtual int countActiveOutboundTransfersUsingDefaultCfg(std::string se, std::string vo);

    virtual int countActiveInboundTransfersUsingDefaultCfg(std::string se, std::string vo);

    virtual int sumUpVoShares (std::string source, std::string destination, std::set<std::string> vos);

    virtual void setPriority(std::string jobId, int priority);

    virtual bool checkConnectionStatus();

    virtual void setRetry(int retry);

    virtual int getRetry(const std::string & jobId);

    virtual int getRetryTimes(const std::string & jobId, int fileId);

    virtual int getMaxTimeInQueue();

    virtual void setMaxTimeInQueue(int afterXHours);

    virtual void setToFailOldQueuedJobs(std::vector<std::string>& jobs);

    virtual std::vector< std::pair<std::string, std::string> > getPairsForSe(std::string se);

    virtual std::vector<std::string> getAllStandAlloneCfgs();

    virtual std::vector<std::string> getAllShareOnlyCfgs();

    virtual std::vector< std::pair<std::string, std::string> > getAllPairCfgs();

    virtual int activeProcessesForThisHost();

    virtual void setFilesToNotUsed(std::string jobId, int fileIndex, std::vector<int>& files);

    virtual std::vector< boost::tuple<std::string, std::string, int> >  getVOBringonlimeMax();

    virtual std::vector<struct message_bringonline> getBringOnlineFiles(std::string voName, std::string hostName, int maxValue);

    virtual void bringOnlineReportStatus(const std::string & state, const std::string & message, struct message_bringonline msg);

    virtual void addToken(const std::string & job_id, int file_id, const std::string & token);

    virtual void getCredentials(std::string & vo_name, const std::string & job_id, int file_id, std::string & dn, std::string & dlg_id);

    virtual void setMaxStageOp(const std::string& se, const std::string& vo, int val);

    virtual void useFileReplica(std::string jobId, int fileId);

    virtual double getSuccessRate(std::string source, std::string destination);

    virtual double getAvgThroughput(std::string source, std::string destination);

    virtual void updateProtocol(const std::string& jobId, int fileId, int nostreams, int timeout, int buffersize, double filesize);

    virtual void cancelFilesInTheQueue(const std::string& se, const std::string& vo, std::set<std::string>& jobs);

    virtual void cancelJobsInTheQueue(const std::string& dn, std::vector<std::string>& jobs);

    virtual void transferLogFile( const std::string& filePath, const std::string& jobId, int fileId, bool debug);

    virtual struct message_state getStateOfTransfer(const std::string& jobId, int fileId);

    virtual void getFilesForJob(const std::string& jobId, std::vector<int>& files);

    virtual void getFilesForJobInCancelState(const std::string& jobId, std::vector<int>& files);

    virtual void setFilesToWaiting(const std::string& se, const std::string& vo, int timeout);

    virtual void setFilesToWaiting(const std::string& dn, int timeout);

    virtual void cancelWaitingFiles(std::set<std::string>& jobs);

    virtual void revertNotUsedFiles();

    virtual void checkSanityState();

    virtual void checkSchemaLoaded();

    virtual void storeProfiling(const fts3::ProfilingSubsystem* prof);

    virtual void setOptimizerMode(int mode);

    virtual void setRetryTransfer(const std::string & jobId, int fileId, int retry, const std::string& reason);

    virtual void getTransferRetries(int fileId, std::vector<FileRetry*>& retries);

    bool assignSanityRuns(soci::session& sql, struct message_sanity &msg);

    void resetSanityRuns(soci::session& sql, struct message_sanity &msg);

    void updateHeartBeat(unsigned* index, unsigned* count, unsigned* start, unsigned* end);

    std::map<std::string, double> getActivityShareConf(soci::session& sql, std::string vo);

    virtual std::vector<std::string> getAllActivityShareConf();

    std::map<std::string, long long> getActivitiesInQueue(soci::session& sql, std::string src, std::string dst, std::string vo);

    std::map<std::string, int> getFilesNumPerActivity(soci::session& sql, std::string src, std::string dst, std::string vo, int filesNum);

private:
    size_t                poolSize;
    soci::connection_pool* connectionPool;
    std::string           hostname;

    bool getInOutOfSe(const std::string& sourceSe, const std::string& destSe);

    void recordOptimizerUpdate(soci::session& sql, int active, double filesize, double throughput,
                               int nostreams, int timeout, int buffersize,std::string source_hostname, std::string destin_hostname);

    int getOptimizerMode(soci::session& sql);

    void countFileInTerminalStates(soci::session& sql, std::string jobId,
                                   unsigned int& finished, unsigned int& cancelled, unsigned int& failed);


    bool getChangedFile (std::string source, std::string dest, double rate, double thr, double avgThr);

    struct HashSegment
    {
        unsigned start;
        unsigned end;

        HashSegment(): start(0), end(0xFFFF) {}
    } hashSegment;

    std::vector< boost::tuple<std::string, std::string, double, double, double> > filesMemStore;
};
