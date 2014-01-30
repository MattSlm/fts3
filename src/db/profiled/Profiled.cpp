#include <ctime>
#include "Profiled.h"
#include "profiler/Profiler.h"
#include "profiler/Macros.h"



void destroy_profiled_db(void *db)
{
    ProfiledDB* profiled = static_cast<ProfiledDB*>(db);
    delete profiled;
}

ProfiledDB::ProfiledDB(GenericDbIfce* db, void (*destroy_db)(void *)):
    db(db), destroy_db(destroy_db)
{
}

ProfiledDB::~ProfiledDB()
{
    destroy_db(db);
}

void ProfiledDB::init(std::string username, std::string password, std::string connectString, int pooledConn)
{
    db->init(username, password, connectString, pooledConn);
}

void ProfiledDB::submitPhysical(const std::string & jobId, std::vector<job_element_tupple> src_dest_pair, const std::string & paramFTP,
                                const std::string & DN, const std::string & cred, const std::string & voName, const std::string & myProxyServer,
                                const std::string & delegationID, const std::string & spaceToken, const std::string & overwrite,
                                const std::string & sourceSpaceToken, const std::string & sourceSpaceTokenDescription, int copyPinLifeTime,
                                const std::string & failNearLine, const std::string & checksumMethod, const std::string & reuse,
                                int bringonline, std::string metadata,
                                int retry, int retryDelay, std::string sourceSe, std::string destinationSe)
{
    PROFILE_PREFIXED("DB::", db->submitPhysical(jobId, src_dest_pair, paramFTP,
                     DN, cred, voName, myProxyServer,
                     delegationID, spaceToken, overwrite,
                     sourceSpaceToken, sourceSpaceTokenDescription, copyPinLifeTime,
                     failNearLine, checksumMethod, reuse,
                     bringonline, metadata,
                     retry, retryDelay, sourceSe, destinationSe));
}

void ProfiledDB::getTransferJobStatus(std::string requestID, bool archive, std::vector<JobStatus*>& jobs)
{
    PROFILE_PREFIXED("DB::", db->getTransferJobStatus(requestID, archive, jobs));
}



void ProfiledDB::getTransferFileStatus(std::string requestID, bool archive,
                                       unsigned offset, unsigned limit, std::vector<FileTransferStatus*>& files)
{
    PROFILE_PREFIXED("DB::", db->getTransferFileStatus(requestID, archive, offset, limit, files));
}


void ProfiledDB::listRequests(std::vector<JobStatus*>& jobs, std::vector<std::string>& inGivenStates,
                              std::string restrictToClientDN, std::string forDN, std::string VOname)
{
    PROFILE_PREFIXED("DB::", db->listRequests(jobs, inGivenStates, restrictToClientDN, forDN, VOname));
}


TransferJobs* ProfiledDB::getTransferJob(std::string jobId, bool archive)
{
    PROFILE_PREFIXED("DB::", return db->getTransferJob(jobId, archive));
}


void ProfiledDB::getByJobIdReuse(std::vector<TransferJobs*>& jobs, std::map< std::string, std::list<TransferFiles*> >& files, bool reuse)
{
    PROFILE_PREFIXED("DB::", db->getByJobIdReuse(jobs, files, reuse));
}


void ProfiledDB::getByJobId(std::map< std::string, std::list<TransferFiles*> >& files)
{
    PROFILE_PREFIXED("DB::", db->getByJobId(files));
}


void ProfiledDB::getSe(Se* &se, std::string seName)
{
    PROFILE_PREFIXED("DB::", db->getSe(se, seName));
}


unsigned int ProfiledDB::updateFileStatus(TransferFiles* file, const std::string status)
{
    PROFILE_PREFIXED("DB::", return db->updateFileStatus(file, status));
}


void ProfiledDB::addSe(std::string endpoint, std::string se_type, std::string site, std::string name, std::string state, std::string version, std::string host,
                       std::string se_transfer_type, std::string se_transfer_protocol, std::string se_control_protocol, std::string gocdb_id)
{
    PROFILE_PREFIXED("DB::", db->addSe(endpoint, se_type, site, name, state, version, host,
                                       se_transfer_type, se_transfer_protocol, se_control_protocol, gocdb_id));
}


void ProfiledDB::updateSe(std::string endpoint, std::string se_type, std::string site, std::string name, std::string state, std::string version, std::string host,
                          std::string se_transfer_type, std::string se_transfer_protocol, std::string se_control_protocol, std::string gocdb_id)
{
    PROFILE_PREFIXED("DB::", db->updateSe(endpoint, se_type, site, name, state, version, host,
                                          se_transfer_type, se_transfer_protocol, se_control_protocol, gocdb_id));
}


void ProfiledDB::deleteSe(std::string NAME)
{
    PROFILE_PREFIXED("DB::", db->deleteSe(NAME));
}


bool ProfiledDB::updateFileTransferStatus(double throughput, std::string job_id, int file_id, std::string transfer_status,
        std::string transfer_message, int process_id, double filesize, double duration)
{
    PROFILE_PREFIXED("DB::", return db->updateFileTransferStatus(throughput, job_id, file_id, transfer_status,
                                    transfer_message, process_id,
                                    filesize, duration));
}


bool ProfiledDB::updateJobTransferStatus(int file_id, std::string job_id, const std::string status)
{
    PROFILE_PREFIXED("DB::", return db->updateJobTransferStatus(file_id, job_id, status));
}


void ProfiledDB::updateFileTransferProgress(std::string job_id, int file_id, double throughput, double transferred)
{
    PROFILE_PREFIXED("DB::", db->updateFileTransferProgress(job_id, file_id, throughput, transferred));
}


void ProfiledDB::updateFileTransferProgressVector(std::vector<struct message_updater>& messages)
{
    PROFILE_PREFIXED("DB::", db->updateFileTransferProgressVector(messages));
}

void ProfiledDB::cancelJob(std::vector<std::string>& requestIDs)
{
    PROFILE_PREFIXED("DB::", db->cancelJob(requestIDs));
}


void ProfiledDB::getCancelJob(std::vector<int>& requestIDs)
{
    PROFILE_PREFIXED("DB::", db->getCancelJob(requestIDs));
}


void ProfiledDB::insertGrDPStorageCacheElement(std::string dlg_id, std::string dn, std::string cert_request, std::string priv_key, std::string voms_attrs)
{
    PROFILE_PREFIXED("DB::", db->insertGrDPStorageCacheElement(dlg_id, dn, cert_request, priv_key, voms_attrs));
}


void ProfiledDB::updateGrDPStorageCacheElement(std::string dlg_id, std::string dn, std::string cert_request, std::string priv_key, std::string voms_attrs)
{
    PROFILE_PREFIXED("DB::", db->updateGrDPStorageCacheElement(dlg_id, dn, cert_request, priv_key, voms_attrs));
}


CredCache* ProfiledDB::findGrDPStorageCacheElement(std::string delegationID, std::string dn)
{
    PROFILE_PREFIXED("DB::", return db->findGrDPStorageCacheElement(delegationID, dn));
}


void ProfiledDB::deleteGrDPStorageCacheElement(std::string delegationID, std::string dn)
{
    PROFILE_PREFIXED("DB::", db->deleteGrDPStorageCacheElement(delegationID, dn));
}


void ProfiledDB::insertGrDPStorageElement(std::string dlg_id, std::string dn, std::string proxy, std::string voms_attrs, time_t termination_time)
{
    PROFILE_PREFIXED("DB::", db->insertGrDPStorageElement(dlg_id, dn, proxy, voms_attrs, termination_time));
}


void ProfiledDB::updateGrDPStorageElement(std::string dlg_id, std::string dn, std::string proxy, std::string voms_attrs, time_t termination_time)
{
    PROFILE_PREFIXED("DB::", db->updateGrDPStorageElement(dlg_id, dn, proxy, voms_attrs, termination_time));
}


Cred* ProfiledDB::findGrDPStorageElement(std::string delegationID, std::string dn)
{
    PROFILE_PREFIXED("DB::", return db->findGrDPStorageElement(delegationID, dn));
}


void ProfiledDB::deleteGrDPStorageElement(std::string delegationID, std::string dn)
{
    PROFILE_PREFIXED("DB::", db->deleteGrDPStorageElement(delegationID, dn));
}


bool ProfiledDB::getDebugMode(std::string source_hostname, std::string destin_hostname)
{
    PROFILE_PREFIXED("DB::", return db->getDebugMode(source_hostname, destin_hostname));
}


void ProfiledDB::setDebugMode(std::string source_hostname, std::string destin_hostname, std::string mode)
{
    PROFILE_PREFIXED("DB::", db->setDebugMode(source_hostname, destin_hostname, mode));
}


void ProfiledDB::getSubmittedJobsReuse(std::vector<TransferJobs*>& jobs, const std::string & vos)
{
    PROFILE_PREFIXED("DB::", db->getSubmittedJobsReuse(jobs, vos));
}


void ProfiledDB::auditConfiguration(const std::string & dn, const std::string & config, const std::string & action)
{
    PROFILE_PREFIXED("DB::", db->auditConfiguration(dn, config, action));
}


void ProfiledDB::fetchOptimizationConfig2(OptimizerSample* ops, const std::string & source_hostname, const std::string & destin_hostname)
{
    PROFILE_PREFIXED("DB::", db->fetchOptimizationConfig2(ops, source_hostname, destin_hostname));
}


bool ProfiledDB::isCredentialExpired(const std::string & dlg_id, const std::string & dn)
{
    PROFILE_PREFIXED("DB::", return db->isCredentialExpired(dlg_id, dn));
}


bool ProfiledDB::isTrAllowed(const std::string & source_se, const std::string & dest)
{
    PROFILE_PREFIXED("DB::", return db->isTrAllowed(source_se, dest));
}

bool ProfiledDB::isTrAllowed2(const std::string & source_se, const std::string & dest)
{
    PROFILE_PREFIXED("DB::", return db->isTrAllowed2(source_se, dest));
}


int ProfiledDB::getSeOut(const std::string & source, const std::set<std::string> & destination)
{
    PROFILE_PREFIXED("DB::", return db->getSeOut(source, destination));
}


int ProfiledDB::getSeIn(const std::set<std::string> & source, const std::string & destination)
{
    PROFILE_PREFIXED("DB::", return db->getSeIn(source, destination));
}


void ProfiledDB::setAllowed(const std::string & job_id, int file_id, const std::string & source_se, const std::string & dest,
                            int nostreams, int timeout, int buffersize)
{
    PROFILE_PREFIXED("DB::", db->setAllowed(job_id, file_id, source_se, dest, nostreams, timeout, buffersize));
}


void ProfiledDB::setAllowedNoOptimize(const std::string & job_id, int file_id, const std::string & params)
{
    PROFILE_PREFIXED("DB::", db->setAllowedNoOptimize(job_id, file_id, params));
}


bool ProfiledDB::terminateReuseProcess(const std::string & jobId)
{
    PROFILE_PREFIXED("DB::", return db->terminateReuseProcess(jobId));
}


void ProfiledDB::forceFailTransfers(std::map<int, std::string>& collectJobs)
{
    PROFILE_PREFIXED("DB::", db->forceFailTransfers(collectJobs));
}


void ProfiledDB::setPid(const std::string & jobId, int fileId, int pid)
{
    PROFILE_PREFIXED("DB::", db->setPid(jobId, fileId, pid));
}


void ProfiledDB::setPidV(int pid, std::map<int,std::string>& pids)
{
    PROFILE_PREFIXED("DB::", db->setPidV(pid, pids));
}


void ProfiledDB::revertToSubmitted()
{
    PROFILE_PREFIXED("DB::", db->revertToSubmitted());
}


void ProfiledDB::backup()
{
    PROFILE_PREFIXED("DB::", db->backup());
}


void ProfiledDB::forkFailedRevertState(const std::string & jobId, int fileId)
{
    PROFILE_PREFIXED("DB::", db->forkFailedRevertState(jobId, fileId));
}


void ProfiledDB::forkFailedRevertStateV(std::map<int,std::string>& pids)
{
    PROFILE_PREFIXED("DB::", db->forkFailedRevertStateV(pids));
}


bool ProfiledDB::retryFromDead(std::vector<struct message_updater>& messages, bool diskFull)
{
    PROFILE_PREFIXED("DB::", return db->retryFromDead(messages, diskFull));
}


void ProfiledDB::blacklistSe(std::string se, std::string vo, std::string status, int timeout, std::string msg, std::string adm_dn)
{
    PROFILE_PREFIXED("DB::", db->blacklistSe(se, vo, status, timeout, msg, adm_dn));
}


void ProfiledDB::blacklistDn(std::string dn, std::string msg, std::string adm_dn)
{
    PROFILE_PREFIXED("DB::", db->blacklistDn(dn, msg, adm_dn));
}


void ProfiledDB::unblacklistSe(std::string se)
{
    PROFILE_PREFIXED("DB::", db->unblacklistSe(se));
}


void ProfiledDB::unblacklistDn(std::string dn)
{
    PROFILE_PREFIXED("DB::", db->unblacklistDn(dn));
}


bool ProfiledDB::isSeBlacklisted(std::string se, std::string vo)
{
    PROFILE_PREFIXED("DB::", return db->isSeBlacklisted(se, vo));
}


bool ProfiledDB::allowSubmitForBlacklistedSe(std::string se)
{
    PROFILE_PREFIXED("DB::", return db->allowSubmitForBlacklistedSe(se));
}


boost::optional<int> ProfiledDB::getTimeoutForSe(std::string se)
{
    PROFILE_PREFIXED("DB::", return db->getTimeoutForSe(se));
}


bool ProfiledDB::isDnBlacklisted(std::string dn)
{
    PROFILE_PREFIXED("DB::", return db->isDnBlacklisted(dn));
}


bool ProfiledDB::isFileReadyState(int fileID)
{
    PROFILE_PREFIXED("DB::", return db->isFileReadyState(fileID));
}


bool ProfiledDB::isFileReadyStateV(std::map<int,std::string>& fileIds)
{
    PROFILE_PREFIXED("DB::", return db->isFileReadyStateV(fileIds));
}


bool ProfiledDB::checkGroupExists(const std::string & groupName)
{
    PROFILE_PREFIXED("DB::", return db->checkGroupExists(groupName));
}


void ProfiledDB::getGroupMembers(const std::string & groupName, std::vector<std::string>& groupMembers)
{
    PROFILE_PREFIXED("DB::", db->getGroupMembers(groupName, groupMembers));
}


void ProfiledDB::addMemberToGroup(const std::string & groupName, std::vector<std::string>& groupMembers)
{
    PROFILE_PREFIXED("DB::", db->addMemberToGroup(groupName, groupMembers));
}


void ProfiledDB::deleteMembersFromGroup(const std::string & groupName, std::vector<std::string>& groupMembers)
{
    PROFILE_PREFIXED("DB::", db->deleteMembersFromGroup(groupName, groupMembers));
}


std::string ProfiledDB::getGroupForSe(const std::string se)
{
    PROFILE_PREFIXED("DB::", return db->getGroupForSe(se));
}



void ProfiledDB::submitHost(const std::string & jobId)
{
    PROFILE_PREFIXED("DB::", db->submitHost(jobId));
}


std::string ProfiledDB::transferHost(int fileId)
{
    PROFILE_PREFIXED("DB::", return db->transferHost(fileId));
}


std::string ProfiledDB::transferHostV(std::map<int,std::string>& fileIds)
{
    PROFILE_PREFIXED("DB::", return db->transferHostV(fileIds));
}


void ProfiledDB::addLinkConfig(LinkConfig* cfg)
{
    PROFILE_PREFIXED("DB::", db->addLinkConfig(cfg));
}

void ProfiledDB::updateLinkConfig(LinkConfig* cfg)
{
    PROFILE_PREFIXED("DB::", db->updateLinkConfig(cfg));
}

void ProfiledDB::deleteLinkConfig(std::string source, std::string destination)
{
    PROFILE_PREFIXED("DB::", db->deleteLinkConfig(source, destination));
}

LinkConfig* ProfiledDB::getLinkConfig(std::string source, std::string destination)
{
    PROFILE_PREFIXED("DB::", return db->getLinkConfig(source, destination));
}

bool ProfiledDB::isThereLinkConfig(std::string source, std::string destination)
{
    PROFILE_PREFIXED("DB::", return db->isThereLinkConfig(source, destination));
}

std::pair<std::string, std::string>* ProfiledDB::getSourceAndDestination(std::string symbolic_name)
{
    PROFILE_PREFIXED("DB::", return db->getSourceAndDestination(symbolic_name));
}

bool ProfiledDB::isGrInPair(std::string group)
{
    PROFILE_PREFIXED("DB::", return db->isGrInPair(group));
}

bool ProfiledDB::isShareOnly(std::string se)
{
    PROFILE_PREFIXED("DB::", return db->isShareOnly(se));
}


void ProfiledDB::addShareConfig(ShareConfig* cfg)
{
    PROFILE_PREFIXED("DB::", db->addShareConfig(cfg));
}

void ProfiledDB::updateShareConfig(ShareConfig* cfg)
{
    PROFILE_PREFIXED("DB::", db->updateShareConfig(cfg));
}

void ProfiledDB::deleteShareConfig(std::string source, std::string destination, std::string vo)
{
    PROFILE_PREFIXED("DB::", db->deleteShareConfig(source, destination, vo));
}

void ProfiledDB::deleteShareConfig(std::string source, std::string destination)
{
    PROFILE_PREFIXED("DB::", db->deleteShareConfig(source, destination));
}

ShareConfig* ProfiledDB::getShareConfig(std::string source, std::string destination, std::string vo)
{
    PROFILE_PREFIXED("DB::", return db->getShareConfig(source, destination, vo));
}

std::vector<ShareConfig*> ProfiledDB::getShareConfig(std::string source, std::string destination)
{
    PROFILE_PREFIXED("DB::", return db->getShareConfig(source, destination));
}


bool ProfiledDB::checkIfSeIsMemberOfAnotherGroup( const std::string & member)
{
    PROFILE_PREFIXED("DB::", return db->checkIfSeIsMemberOfAnotherGroup(member));
}


void ProfiledDB::addFileShareConfig(int file_id, std::string source, std::string destination, std::string vo)
{
    PROFILE_PREFIXED("DB::", db->addFileShareConfig(file_id, source, destination, vo));
}


void ProfiledDB::getFilesForNewSeCfg(std::string source, std::string destination, std::string vo, std::vector<int>& out)
{
    PROFILE_PREFIXED("DB::", db->getFilesForNewSeCfg(source, destination, vo, out));
}


void ProfiledDB::getFilesForNewGrCfg(std::string source, std::string destination, std::string vo, std::vector<int>& out)
{
    PROFILE_PREFIXED("DB::", db->getFilesForNewGrCfg(source, destination, vo, out));
}


void ProfiledDB::delFileShareConfig(int file_id, std::string source, std::string destination, std::string vo)
{
    PROFILE_PREFIXED("DB::", db->delFileShareConfig(file_id, source, destination, vo));
}


void ProfiledDB::delFileShareConfig(std::string group, std::string se)
{
    PROFILE_PREFIXED("DB::", db->delFileShareConfig(group, se));
}


bool ProfiledDB::hasStandAloneSeCfgAssigned(int file_id, std::string vo)
{
    PROFILE_PREFIXED("DB::", return db->hasStandAloneSeCfgAssigned(file_id, vo));
}


bool ProfiledDB::hasPairSeCfgAssigned(int file_id, std::string vo)
{
    PROFILE_PREFIXED("DB::", return db->hasPairSeCfgAssigned(file_id, vo));
}


bool ProfiledDB::hasStandAloneGrCfgAssigned(int file_id, std::string vo)
{
    PROFILE_PREFIXED("DB::", return db->hasStandAloneGrCfgAssigned(file_id, vo));
}


bool ProfiledDB::hasPairGrCfgAssigned(int file_id, std::string vo)
{
    PROFILE_PREFIXED("DB::", return db->hasPairGrCfgAssigned(file_id, vo));
}


int ProfiledDB::countActiveTransfers(std::string source, std::string destination, std::string vo)
{
    PROFILE_PREFIXED("DB::", return db->countActiveTransfers(source, destination, vo));
}


int ProfiledDB::countActiveOutboundTransfersUsingDefaultCfg(std::string se, std::string vo)
{
    PROFILE_PREFIXED("DB::", return db->countActiveOutboundTransfersUsingDefaultCfg(se, vo));
}


int ProfiledDB::countActiveInboundTransfersUsingDefaultCfg(std::string se, std::string vo)
{
    PROFILE_PREFIXED("DB::", return db->countActiveInboundTransfersUsingDefaultCfg(se, vo));
}


int ProfiledDB::sumUpVoShares (std::string source, std::string destination, std::set<std::string> vos)
{
    PROFILE_PREFIXED("DB::", return db->sumUpVoShares(source, destination, vos));
}


bool ProfiledDB::checkConnectionStatus()
{
    PROFILE_PREFIXED("DB::", return db->checkConnectionStatus());
}


void ProfiledDB::setPriority(std::string jobId, int priority)
{
    PROFILE_PREFIXED("DB::", db->setPriority(jobId, priority));
}


void ProfiledDB::setRetry(int retry)
{
    PROFILE_PREFIXED("DB::", db->setRetry(retry));
}


int ProfiledDB::getRetry(const std::string & jobId)
{
    PROFILE_PREFIXED("DB::", return db->getRetry(jobId));
}


int ProfiledDB::getRetryTimes(const std::string & jobId, int fileId)
{
    PROFILE_PREFIXED("DB::", return db->getRetryTimes(jobId, fileId));
}


int ProfiledDB::getMaxTimeInQueue()
{
    PROFILE_PREFIXED("DB::", return db->getMaxTimeInQueue());
}


void ProfiledDB::setMaxTimeInQueue(int afterXHours)
{
    PROFILE_PREFIXED("DB::", db->setMaxTimeInQueue(afterXHours));
}


void ProfiledDB::setToFailOldQueuedJobs(std::vector<std::string>& jobs)
{
    PROFILE_PREFIXED("DB::", db->setToFailOldQueuedJobs(jobs));
}


std::vector< std::pair<std::string, std::string> > ProfiledDB::getPairsForSe(std::string se)
{
    PROFILE_PREFIXED("DB::", return db->getPairsForSe(se));
}


std::vector<std::string> ProfiledDB::getAllStandAlloneCfgs()
{
    PROFILE_PREFIXED("DB::", return db->getAllStandAlloneCfgs());
}


std::vector<std::string> ProfiledDB::getAllShareOnlyCfgs()
{
    PROFILE_PREFIXED("DB::", return db->getAllShareOnlyCfgs());
}


int ProfiledDB::activeProcessesForThisHost()
{
    PROFILE_PREFIXED("DB::", return db->activeProcessesForThisHost());
}


std::vector< std::pair<std::string, std::string> > ProfiledDB::getAllPairCfgs()
{
    PROFILE_PREFIXED("DB::", return db->getAllPairCfgs());
}


void ProfiledDB::setFilesToNotUsed(std::string jobId, int fileIndex, std::vector<int>& files)
{
    PROFILE_PREFIXED("DB::", db->setFilesToNotUsed(jobId, fileIndex, files));
}


std::vector< boost::tuple<std::string, std::string, int> > ProfiledDB::getVOBringonlimeMax()
{
    PROFILE_PREFIXED("DB::", return db->getVOBringonlimeMax());
}


std::vector<struct message_bringonline> ProfiledDB::getBringOnlineFiles(std::string voName, std::string hostName, int maxValue)
{
    PROFILE_PREFIXED("DB::", return db->getBringOnlineFiles(voName, hostName, maxValue));
}


void ProfiledDB::bringOnlineReportStatus(const std::string & state, const std::string & message, struct message_bringonline msg)
{
    PROFILE_PREFIXED("DB::", db->bringOnlineReportStatus(state, message, msg));
}


void ProfiledDB::addToken(const std::string & job_id, int file_id, const std::string & token)
{
    PROFILE_PREFIXED("DB::", db->addToken(job_id, file_id, token));
}


void ProfiledDB::getCredentials(std::string & vo_name, const std::string & job_id, int file_id, std::string & dn, std::string & dlg_id)
{
    PROFILE_PREFIXED("DB::", db->getCredentials(vo_name, job_id, file_id, dn, dlg_id));
}


void ProfiledDB::setMaxStageOp(const std::string& se, const std::string& vo, int val)
{
    PROFILE_PREFIXED("DB::", db->setMaxStageOp(se, vo, val));
}


double ProfiledDB::getSuccessRate(std::string source, std::string destination)
{
    PROFILE_PREFIXED("DB::", return db->getSuccessRate(source, destination));
}


double ProfiledDB::getAvgThroughput(std::string source, std::string destination)
{
    PROFILE_PREFIXED("DB::", return db->getAvgThroughput(source, destination));
}


void ProfiledDB::updateProtocol(const std::string& jobId, int fileId, int nostreams, int timeout, int buffersize, double filesize)
{
    PROFILE_PREFIXED("DB::", db->updateProtocol(jobId, fileId, nostreams, timeout, buffersize, filesize));
}


void ProfiledDB::cancelFilesInTheQueue(const std::string& se, const std::string& vo, std::set<std::string>& jobs)
{
    PROFILE_PREFIXED("DB::", db->cancelFilesInTheQueue(se, vo, jobs));
}


void ProfiledDB::cancelJobsInTheQueue(const std::string& dn, std::vector<std::string>& jobs)
{
    PROFILE_PREFIXED("DB::", db->cancelJobsInTheQueue(dn, jobs));
}


void ProfiledDB::transferLogFile(const std::string& filePath, const std::string& jobId, int fileId, bool debug)
{
    PROFILE_PREFIXED("DB::", db->transferLogFile(filePath, jobId, fileId, debug));
}

void ProfiledDB::transferLogFileVector(std::map<int, struct message_log>& messagesLog)
{
    PROFILE_PREFIXED("DB::", db->transferLogFileVector(messagesLog));
}


std::vector<struct message_state> ProfiledDB::getStateOfTransfer(const std::string& jobId, int fileId)
{
    PROFILE_PREFIXED("DB::", return db->getStateOfTransfer(jobId, fileId));
}


void ProfiledDB::getFilesForJob(const std::string& jobId, std::vector<int>& files)
{
    PROFILE_PREFIXED("DB::", db->getFilesForJob(jobId, files));
}


void ProfiledDB::getFilesForJobInCancelState(const std::string& jobId, std::vector<int>& files)
{
    PROFILE_PREFIXED("DB::", db->getFilesForJobInCancelState(jobId, files));
}


void ProfiledDB::setFilesToWaiting(const std::string& se, const std::string& vo, int timeout)
{
    PROFILE_PREFIXED("DB::", db->setFilesToWaiting(se, vo, timeout));
}


void ProfiledDB::setFilesToWaiting(const std::string& dn, int timeout)
{
    PROFILE_PREFIXED("DB::", db->setFilesToWaiting(dn, timeout));
}


void ProfiledDB::cancelWaitingFiles(std::set<std::string>& jobs)
{
    PROFILE_PREFIXED("DB::", db->cancelWaitingFiles(jobs));
}


void ProfiledDB::revertNotUsedFiles()
{
    PROFILE_PREFIXED("DB::", db->revertNotUsedFiles());
}


void ProfiledDB::checkSanityState()
{
    PROFILE_PREFIXED("DB::", db->checkSanityState());
}


void ProfiledDB::checkSchemaLoaded()
{
    PROFILE_PREFIXED("DB::", db->checkSchemaLoaded());
}


void ProfiledDB::storeProfiling(const fts3::ProfilingSubsystem* prof)
{
    db->storeProfiling(prof);
}

void ProfiledDB::setOptimizerMode(int mode)
{
    db->setOptimizerMode(mode);
}

void ProfiledDB::setRetryTransfer(const std::string & jobId, int fileId,
                                  int retry, const std::string& reason)
{
    PROFILE_PREFIXED("DB::", db->setRetryTransfer(jobId, fileId, retry, reason));
}

void ProfiledDB::getTransferRetries(int fileId, std::vector<FileRetry*>& retries)
{
    PROFILE_PREFIXED("DB::", db->getTransferRetries(fileId, retries));
}

void ProfiledDB::updateHeartBeat(unsigned* index, unsigned* count, unsigned* start, unsigned* end)
{
    PROFILE_PREFIXED("DB::", db->updateHeartBeat(index, count, start, end));
}

unsigned int ProfiledDB::updateFileStatusReuse(TransferFiles* file, const std::string status)
{
    PROFILE_PREFIXED("DB::", return db->updateFileStatusReuse(file, status));
}
