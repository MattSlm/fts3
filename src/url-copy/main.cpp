/* Copyright @ Members of the EMI Collaboration, 2010.
See www.eu-emi.eu for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

#include <boost/algorithm/string/replace.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <boost/tokenizer.hpp>
#include <execinfo.h>
#include <gfal_api.h>
#include <string>
#include <transfer/gfal_transfer.h>

#include "args.h"
#include "definitions.h"
#include "errors.h"
#include "file_management.h"
#include "heuristics.h"
#include "logger.h"
#include "msg-ifce.h"
#include "name_to_uid.h"
#include "reporter.h"
#include "StaticSslLocking.h"
#include "transfer.h"
#include "UserProxyEnv.h"


using namespace std;
using boost::thread;

static FileManagement fileManagement;
static Reporter reporter;
static transfer_completed tr_completed;
static bool retry = true;
static std::string errorScope("");
static std::string errorPhase("");
static std::string reasonClass("");
static std::string errorMessage("");
static std::string readFile("");
static char hostname[1024] = {0};
static volatile bool propagated = false;
static volatile bool terminalState = false;
static std::string globalErrorMessage("");

time_t globalTimeout;

Transfer currentTransfer;

gfal_context_t handle = NULL;

static std::string replaceMetadataString(std::string text)
{
    text = boost::replace_all_copy(text, "?"," ");
    text = boost::replace_all_copy(text, "\\\"","");
    return text;
}



static void cancelTransfer()
{
    static volatile bool canceled = false;
    if (handle && !canceled)   // finish all transfer in a clean way
        {
            canceled = true;
            gfal2_cancel(handle);
        }
}


static std::string srmVersion(const std::string & url)
{
    if (url.compare(0, 6, "srm://") == 0)
        return std::string("2.2.0");

    return std::string("");
}

static void call_perf(gfalt_transfer_status_t h, const char*, const char*, gpointer)
{
    if (h)
        {
            size_t avg = gfalt_copy_get_average_baudrate(h, NULL);
            if (avg > 0)
                {
                    avg = avg / 1024;
                }
            else
                {
                    avg = 0;
                }
            size_t inst = gfalt_copy_get_instant_baudrate(h, NULL);
            if (inst > 0)
                {
                    inst = inst / 1024;
                }
            else
                {
                    inst = 0;
                }

            size_t trans = gfalt_copy_get_bytes_transfered(h, NULL);
            time_t elapsed = gfalt_copy_get_elapsed_time(h, NULL);
            Logger::getInstance().INFO() << "bytes: " << trans
                                         << ", avg KB/sec:" << avg
                                         << ", inst KB/sec:" << inst
                                         << ", elapsed:" << elapsed
                                         << std::endl;
            currentTransfer.throughput       = (double) avg;
            currentTransfer.transferredBytes = trans;
        }

}

std::string getDefaultScope()
{
    return errorScope.length() == 0 ? AGENT : errorScope;
}

std::string getDefaultReasonClass()
{
    return reasonClass.length() == 0 ? ALLOCATION : reasonClass;
}

std::string getDefaultErrorPhase()
{
    return errorPhase.length() == 0 ? GENERAL_FAILURE : errorPhase;
}

void abnormalTermination(const std::string& classification, const std::string&, const std::string& finalState)
{
    terminalState = true;

    if(globalErrorMessage.length() > 0)
        {
            errorMessage += " " + globalErrorMessage;
        }

    if(classification != "CANCELED")
        retry = true;

    Logger::getInstance().ERROR() << errorMessage << std::endl;

    msg_ifce::getInstance()->set_transfer_error_scope(&tr_completed, getDefaultScope());
    msg_ifce::getInstance()->set_transfer_error_category(&tr_completed, getDefaultReasonClass());
    msg_ifce::getInstance()->set_failure_phase(&tr_completed, getDefaultErrorPhase());
    msg_ifce::getInstance()->set_transfer_error_message(&tr_completed, errorMessage);
    msg_ifce::getInstance()->set_final_transfer_state(&tr_completed, finalState);
    msg_ifce::getInstance()->set_tr_timestamp_complete(&tr_completed, msg_ifce::getInstance()->getTimestamp());
    if(UrlCopyOpts::getInstance().monitoringMessages)
        msg_ifce::getInstance()->SendTransferFinishMessage(&tr_completed);

    reporter.timeout = UrlCopyOpts::getInstance().timeout;
    reporter.nostreams = UrlCopyOpts::getInstance().nStreams;
    reporter.buffersize = UrlCopyOpts::getInstance().tcpBuffersize;


    reporter.sendTerminal(currentTransfer.throughput, retry,
                          currentTransfer.jobId, currentTransfer.fileId,
                          classification, errorMessage,
                          currentTransfer.getTransferDurationInSeconds(),
                          currentTransfer.fileSize);

    std::string moveFile = fileManagement.archive();

    reporter.sendLog(currentTransfer.jobId, currentTransfer.fileId, fileManagement._getLogArchivedFileFullPath(),
                     UrlCopyOpts::getInstance().debug);

    if (moveFile.length() != 0)
        {
            Logger::getInstance().ERROR() << "Failed to archive file: " << moveFile
                                          << std::endl;
        }
    if (UrlCopyOpts::getInstance().areTransfersOnFile() && readFile.length() > 0)
        unlink(readFile.c_str());

    cancelTransfer();
    sleep(1);
    exit(1);
}

void canceler()
{
    errorMessage = "Transfer " + currentTransfer.jobId + " was canceled because it was not responding";

    Logger::getInstance().WARNING() << errorMessage << std::endl;

    abnormalTermination("FAILED", errorMessage, "Abort");
}

/**
 * This thread reduces one by one the value pointed by timeout,
 * until it reaches 0.
 * Using a pointer allow us to reset the timeout if we happen to hit
 * a file bigger than initially expected.
 */
void taskTimer(time_t* timeout)
{
    while (*timeout)
        {
            boost::this_thread::sleep(boost::posix_time::seconds(1));
            *timeout -= 1;
        }
    canceler();
}

void taskStatusUpdater(int time)
{
    while (time)
        {
            Logger::getInstance().INFO() << "Sending back to the server url-copy is still alive : "
                                         <<  currentTransfer.throughput << "  " <<  currentTransfer.transferredBytes
                                         << std::endl;

            if(currentTransfer.fileId > 0)
                reporter.sendPing(currentTransfer.jobId, currentTransfer.fileId, currentTransfer.throughput, currentTransfer.transferredBytes);

            boost::this_thread::sleep(boost::posix_time::seconds(time));
        }
}


std::string log_stack(int sig)
{
    std::string stackTrace;

    if(sig == SIGSEGV || sig == SIGBUS || sig == SIGABRT)
        {
            const int stack_size = 25;
            void * array[stack_size]= {0};
            int nSize = backtrace(array, stack_size);
            char ** symbols = backtrace_symbols(array, nSize);
            for (register int i = 0; i < nSize; ++i)
                {
                    if(symbols && symbols[i])
                        {
                            stackTrace += std::string(symbols[i]) + '\n';
                        }
                }
            if(symbols)
                {
                    free(symbols);
                }
        }
    return stackTrace;
}


void signalHandler(int signum)
{
    Logger& logger = Logger::getInstance();

    logger.WARNING() << "Received signal " << signum << std::endl;

    std::string stackTrace = log_stack(signum);
    if (stackTrace.length() > 0)
        {
            propagated = true;

            logger.ERROR() << "Transfer process died " << currentTransfer.jobId << std::endl;
            logger.ERROR() << "Received signal " << signum << std::endl;
            logger.ERROR() << stackTrace << std::endl;

            errorMessage = "Transfer process died " + currentTransfer.jobId;
            errorMessage += stackTrace;
            abnormalTermination("FAILED", errorMessage, "Error");
        }
    else if (signum == SIGINT || signum == SIGTERM)
        {
            if (propagated == false)
                {
                    propagated = true;
                    errorMessage = "Transfer " + currentTransfer.jobId + " canceled by the user";
                    logger.WARNING() << errorMessage << std::endl;
                    abnormalTermination("CANCELED", errorMessage, "Abort");
                }
        }
    else if (signum == SIGUSR1)
        {
            if (propagated == false)
                {
                    propagated = true;
                    errorMessage = "Transfer " + currentTransfer.jobId + " has been forced-canceled because it was stalled";
                    logger.WARNING() << errorMessage << std::endl;
                    abnormalTermination("FAILED", errorMessage, "Abort");
                }
        }
    else
        {
            if (propagated == false)
                {
                    propagated = true;
                    errorMessage = "Transfer " + currentTransfer.jobId + " aborted, check log file for details, received signum " + boost::lexical_cast<std::string>(signum);
                    logger.WARNING() << errorMessage << std::endl;
                    abnormalTermination("FAILED", errorMessage, "Abort");
                }
        }
}

// Callback used to populate the messaging with the different stages

static void event_logger(const gfalt_event_t e, gpointer /*udata*/)
{
    static const char* sideStr[] = {"SRC", "DST", "BTH"};
    static const GQuark SRM_DOMAIN = g_quark_from_static_string("SRM");

    msg_ifce* msg = msg_ifce::getInstance();
    std::string timestampStr = boost::lexical_cast<std::string>(e->timestamp);

    Logger::getInstance().INFO() << '[' << timestampStr << "] "
                                 << sideStr[e->side] << ' '
                                 << g_quark_to_string(e->domain) << '\t'
                                 << g_quark_to_string(e->stage) << '\t'
                                 << e->description << std::endl;

    if (e->stage == GFAL_EVENT_TRANSFER_ENTER)
        {
            msg->set_timestamp_transfer_started(&tr_completed, timestampStr);
            currentTransfer.startTime = e->timestamp;
        }
    else if (e->stage == GFAL_EVENT_TRANSFER_EXIT)
        {
            msg->set_timestamp_transfer_completed(&tr_completed, timestampStr);
            currentTransfer.finishTime = e->timestamp;
        }

    else if (e->stage == GFAL_EVENT_CHECKSUM_ENTER && e->side == GFAL_EVENT_SOURCE)
        msg->set_timestamp_checksum_source_started(&tr_completed, timestampStr);
    else if (e->stage == GFAL_EVENT_CHECKSUM_EXIT && e->side == GFAL_EVENT_SOURCE)
        msg->set_timestamp_checksum_source_ended(&tr_completed, timestampStr);

    else if (e->stage == GFAL_EVENT_CHECKSUM_ENTER && e->side == GFAL_EVENT_DESTINATION)
        msg->set_timestamp_checksum_dest_started(&tr_completed, timestampStr);
    else if (e->stage == GFAL_EVENT_CHECKSUM_EXIT && e->side == GFAL_EVENT_DESTINATION)
        msg->set_timestamp_checksum_dest_ended(&tr_completed, timestampStr);

    else if (e->stage == GFAL_EVENT_PREPARE_ENTER && e->domain == SRM_DOMAIN)
        msg->set_time_spent_in_srm_preparation_start(&tr_completed, timestampStr);
    else if (e->stage == GFAL_EVENT_PREPARE_EXIT && e->domain == SRM_DOMAIN)
        msg->set_time_spent_in_srm_preparation_end(&tr_completed, timestampStr);

    else if (e->stage == GFAL_EVENT_CLOSE_ENTER && e->domain == SRM_DOMAIN)
        msg->set_time_spent_in_srm_finalization_start(&tr_completed, timestampStr);
    else if (e->stage == GFAL_EVENT_CLOSE_EXIT && e->domain == SRM_DOMAIN)
        msg->set_time_spent_in_srm_finalization_end(&tr_completed, timestampStr);
}

static void log_func(const gchar *, GLogLevelFlags, const gchar *message, gpointer)
{
    if (message)
        {
            Logger::getInstance().DEBUG() << message << std::endl;
        }
}

void myunexpected()
{
    if (propagated == false)
        {
            propagated = true;
            errorMessage = "Transfer unexpected handler called " + currentTransfer.jobId;
            errorMessage += " Source: " + UrlCopyOpts::getInstance().sourceUrl;
            errorMessage += " Dest: " + UrlCopyOpts::getInstance().destUrl;
            Logger::getInstance().ERROR() << errorMessage << std::endl;

            abnormalTermination("FAILED", errorMessage, "Abort");
        }
}

void myterminate()
{
    if (propagated == false)
        {
            propagated = true;
            errorMessage = "Transfer terminate handler called:" + currentTransfer.jobId;
            errorMessage += " Source: " + UrlCopyOpts::getInstance().sourceUrl;
            errorMessage += " Dest: " + UrlCopyOpts::getInstance().destUrl;
            Logger::getInstance().ERROR() << errorMessage << std::endl;

            abnormalTermination("FAILED", errorMessage, "Abort");
        }
}


int statWithRetries(gfal_context_t handle, const std::string& category, const std::string& url, off_t* size, std::string* errMsg)
{
    struct stat statBuffer;
    GError* statError = NULL;
    bool canBeRetried = false;

    int errorCode = 0;

    errMsg->clear();
    for (int attempt = 0; attempt < 4; attempt++)
        {
            if (gfal2_stat(handle, url.c_str(), &statBuffer, &statError) < 0)
                {
                    errorCode = statError->code;
                    errMsg->assign(statError->message);
                    g_clear_error(&statError);

                    canBeRetried = retryTransfer(errorCode, category, std::string(*errMsg));
                    if (!canBeRetried)
                        return errorCode;
                }
            else
                {
                    *size = statBuffer.st_size;
                    return 0;
                }

            Logger::getInstance().WARNING() << "Stat failed with " << *errMsg << "(" << errorCode << ")" << std::endl;
            Logger::getInstance().WARNING() << "Stat the file will be retried" << std::endl;
            sleep(3); //give it some time to breath
        }

    Logger::getInstance().ERROR() << "No more retries for stat the file" << std::endl;
    return errorCode;
}

void setRemainingTransfersToFailed(std::vector<Transfer>& transferList, unsigned currentIndex)
{
    for (unsigned i = currentIndex + 1; i < transferList.size(); ++i)
        {
            Transfer& t = transferList[i];
            Logger::getInstance().INFO() << "Report FAILED back to the server for " << t.fileId << std::endl;

            msg_ifce::getInstance()->set_source_srm_version(&tr_completed, srmVersion(t.sourceUrl));
            msg_ifce::getInstance()->set_destination_srm_version(&tr_completed, srmVersion(t.destUrl));
            msg_ifce::getInstance()->set_source_url(&tr_completed, t.sourceUrl);
            msg_ifce::getInstance()->set_dest_url(&tr_completed, t.destUrl);
            msg_ifce::getInstance()->set_transfer_error_scope(&tr_completed, TRANSFER);
            msg_ifce::getInstance()->set_transfer_error_category(&tr_completed, GENERAL_FAILURE);
            msg_ifce::getInstance()->set_failure_phase(&tr_completed, TRANSFER);
            msg_ifce::getInstance()->set_transfer_error_message(&tr_completed, "Not executed because a previous hop failed");
            if(UrlCopyOpts::getInstance().monitoringMessages)
                msg_ifce::getInstance()->SendTransferFinishMessage(&tr_completed);

            reporter.sendTerminal(0, false,
                                  t.jobId, t.fileId,
                                  "FAILED", "Not executed because a previous hop failed",
                                  0, 0);
        }
}


__attribute__((constructor)) void begin(void)
{
    //switch to non-priviledged user to avoid reading the hostcert
    uid_t pw_uid = name_to_uid();
    setuid(pw_uid);
    seteuid(pw_uid);
    StaticSslLocking::init_locks();
}

int main(int argc, char **argv)
{
    Logger &logger = Logger::getInstance();

    // register signals handler
    signal(SIGINT, signalHandler);
    signal(SIGUSR1, signalHandler);
    signal(SIGABRT, signalHandler);
    signal(SIGSEGV, signalHandler);
    signal(SIGTERM, signalHandler);
    signal(SIGILL, signalHandler);
    signal(SIGBUS, signalHandler);
    signal(SIGTRAP, signalHandler);
    signal(SIGSYS, signalHandler);

    //set_terminate(myterminate);
    //set_unexpected(myunexpected);

    UrlCopyOpts &opts = UrlCopyOpts::getInstance();
    if (opts.parse(argc, argv) < 0)
        {
            std::cerr << opts.getErrorMessage() << std::endl;
            return 1;
        }

    currentTransfer.jobId = opts.jobId;

    UserProxyEnv* cert = NULL;

    hostname[1023] = '\0';
    gethostname(hostname, 1023);

    if(argc < 4)
        {
            errorMessage = "Failed to read url-copy process arguments";
            abnormalTermination("FAILED", errorMessage, "Abort");
        }


    try
        {
            /*send an update message back to the server to indicate it's alive*/
            boost::thread btUpdater(taskStatusUpdater, 60);
        }
    catch (std::exception& e)
        {
            globalErrorMessage = e.what();
            throw;
        }
    catch(...)
        {
            globalErrorMessage = "Failed to create boost thread, boost::thread_resource_error";
            throw;
        }

    if (opts.proxy.length() > 0)
        {
            // Set Proxy Env
            cert = new UserProxyEnv(opts.proxy);
        }

    // Populate the transfer list
    std::vector<Transfer> transferList;

    if (opts.areTransfersOnFile())
        {
            readFile = "/var/lib/fts3/" + opts.jobId;
            Transfer::initListFromFile(opts.jobId, readFile, &transferList);
        }
    else
        {
            transferList.push_back(Transfer::createFromOptions(opts));
        }


    //cancelation point
    long unsigned int numberOfFiles = transferList.size();
    globalTimeout = numberOfFiles * 6000;

    try
        {
            boost::thread bt(taskTimer, &globalTimeout);
        }
    catch (std::exception& e)
        {
            globalErrorMessage = e.what();
            throw;
        }
    catch(...)
        {
            globalErrorMessage = "Failed to create boost thread, boost::thread_resource_error";
            throw;
        }

    if (opts.areTransfersOnFile() && transferList.empty() == true)
        {
            errorMessage = "Transfer " + currentTransfer.jobId + " contains no urls with session reuse/multihop enabled";

            abnormalTermination("FAILED", errorMessage, "Error");
        }


    GError* handleError = NULL;
    GError *tmp_err = NULL;
    gfalt_params_t params;
    handle = gfal_context_new(&handleError);
    params = gfalt_params_handle_new(NULL);
    gfalt_set_event_callback(params, event_logger, NULL);


    //reuse session
    if (opts.areTransfersOnFile())
        {
            gfal2_set_opt_boolean(handle, "GRIDFTP PLUGIN", "SESSION_REUSE", TRUE, NULL);
        }

    // Enable UDT
    if (opts.enable_udt)
        {
            gfal2_set_opt_boolean(handle, "GRIDFTP PLUGIN", "ENABLE_UDT", TRUE, NULL);
        }

    if (!handle)
        {
            errorMessage = "Failed to create the gfal2 handle: ";
            if (handleError && handleError->message)
                {
                    errorMessage += handleError->message;
                    abnormalTermination("FAILED", errorMessage, "Error");
                }
        }


    for (register unsigned int ii = 0; ii < numberOfFiles; ii++)
        {
            errorScope = std::string("");
            reasonClass = std::string("");
            errorPhase = std::string("");
            retry = true;
            errorMessage = std::string("");
            currentTransfer.throughput = 0.0;

            currentTransfer = transferList[ii];

            fileManagement.setSourceUrl(currentTransfer.sourceUrl);
            fileManagement.setDestUrl(currentTransfer.destUrl);
            fileManagement.setFileId(currentTransfer.fileId);
            fileManagement.setJobId(currentTransfer.jobId);

            reporter.timeout = opts.timeout;
            reporter.nostreams = opts.nStreams;
            reporter.buffersize = opts.tcpBuffersize;
            reporter.source_se = fileManagement.getSourceHostname();
            reporter.dest_se = fileManagement.getDestHostname();
            fileManagement.generateLogFile();

            msg_ifce::getInstance()->set_tr_timestamp_start(&tr_completed, msg_ifce::getInstance()->getTimestamp());
            msg_ifce::getInstance()->set_agent_fqdn(&tr_completed, hostname);
            msg_ifce::getInstance()->set_t_channel(&tr_completed, fileManagement.getSePair());
            msg_ifce::getInstance()->set_transfer_id(&tr_completed, fileManagement.getLogFileName());
            msg_ifce::getInstance()->set_source_srm_version(&tr_completed, srmVersion(currentTransfer.sourceUrl));
            msg_ifce::getInstance()->set_destination_srm_version(&tr_completed, srmVersion(currentTransfer.destUrl));
            msg_ifce::getInstance()->set_source_url(&tr_completed, currentTransfer.sourceUrl);
            msg_ifce::getInstance()->set_dest_url(&tr_completed, currentTransfer.destUrl);
            msg_ifce::getInstance()->set_source_hostname(&tr_completed, fileManagement.getSourceHostnameFile());
            msg_ifce::getInstance()->set_dest_hostname(&tr_completed, fileManagement.getDestHostnameFile());
            msg_ifce::getInstance()->set_channel_type(&tr_completed, "urlcopy");
            msg_ifce::getInstance()->set_vo(&tr_completed, opts.vo);
            msg_ifce::getInstance()->set_source_site_name(&tr_completed, opts.sourceSiteName);
            msg_ifce::getInstance()->set_dest_site_name(&tr_completed, opts.destSiteName);
            msg_ifce::getInstance()->set_number_of_streams(&tr_completed, opts.nStreams);
            msg_ifce::getInstance()->set_tcp_buffer_size(&tr_completed, opts.tcpBuffersize);
            msg_ifce::getInstance()->set_block_size(&tr_completed, opts.blockSize);
            msg_ifce::getInstance()->set_srm_space_token_dest(&tr_completed, opts.destTokenDescription);
            msg_ifce::getInstance()->set_srm_space_token_source(&tr_completed, opts.sourceTokenDescription);

            if(opts.monitoringMessages)
                msg_ifce::getInstance()->SendTransferStartMessage(&tr_completed);

            if (!opts.logToStderr)
                {
                    int checkError = Logger::getInstance().redirectTo(fileManagement.getLogFilePath(), opts.debug);
                    if (checkError != 0)
                        {
                            std::string message = mapErrnoToString(checkError);
                            errorMessage = "Failed to create transfer log file, error was: " + message;
                            goto stop;
                        }
                }

            // Scope
            {
                reporter.sendLog(opts.jobId, currentTransfer.fileId, fileManagement.getLogFilePath(),
                                 opts.debug);

                gfalt_set_user_data(params, NULL, NULL);

                logger.INFO() << "Transfer accepted" << std::endl;
                logger.INFO() << "Proxy:" << opts.proxy << std::endl;
                logger.INFO() << "VO:" << opts.vo << std::endl; //a
                logger.INFO() << "Job id:" << opts.jobId << std::endl;
                logger.INFO() << "File id:" << currentTransfer.fileId << std::endl;
                logger.INFO() << "Source url:" << currentTransfer.sourceUrl << std::endl;
                logger.INFO() << "Dest url:" << currentTransfer.destUrl << std::endl;
                logger.INFO() << "Overwrite enabled:" << opts.overwrite << std::endl;
                logger.INFO() << "Tcp buffer size:" << opts.tcpBuffersize << std::endl;
                logger.INFO() << "Dest space token:" << opts.destTokenDescription << std::endl;
                logger.INFO() << "Source space token:" << opts.sourceTokenDescription << std::endl;
                logger.INFO() << "Pin lifetime:" << opts.copyPinLifetime << std::endl;
                logger.INFO() << "BringOnline:" << opts.bringOnline << std::endl;
                logger.INFO() << "Checksum:" << currentTransfer.checksumValue << std::endl;
                logger.INFO() << "Checksum enabled:" << currentTransfer.checksumMethod << std::endl;
                logger.INFO() << "User filesize:" << currentTransfer.userFileSize << std::endl;
                logger.INFO() << "File metadata:" << replaceMetadataString(currentTransfer.fileMetadata) << std::endl;
                logger.INFO() << "Job metadata:" << replaceMetadataString(opts.jobMetadata) << std::endl;
                logger.INFO() << "Bringonline token:" << currentTransfer.tokenBringOnline << std::endl;
                logger.INFO() << "Multihop: " << opts.multihop << std::endl;
                logger.INFO() << "UDT: " << opts.enable_udt << std::endl;

                //set to active only for reuse
                if (opts.areTransfersOnFile())
                    {
                        logger.INFO() << "Set the transfer to ACTIVE, report back to the server" << std::endl;
                        reporter.setMultipleTransfers(true);
                        reporter.sendMessage(currentTransfer.throughput, false,
                                             opts.jobId, currentTransfer.fileId,
                                             "ACTIVE", "", 0,
                                             currentTransfer.fileSize);
                    }

                if (access(opts.proxy.c_str(), F_OK) != 0)
                    {
                        errorMessage = "Proxy doesn't exist, probably expired and not renewed " + opts.proxy;
                        errorScope = SOURCE;
                        reasonClass = mapErrnoToString(errno);
                        errorPhase = TRANSFER_PREPARATION;
                        logger.ERROR() << errorMessage << std::endl;
                        goto stop;
                    }

                /*set infosys to gfal2*/
                if (handle)
                    {
                        logger.INFO() << "BDII:" << opts.infosys << std::endl;
                        if (opts.infosys.compare("false") == 0)
                            {
                                gfal2_set_opt_boolean(handle, "BDII", "ENABLED", false, NULL);
                            }
                        else
                            {
                                gfal2_set_opt_string(handle, "BDII", "LCG_GFAL_INFOSYS", (char *) opts.infosys.c_str(), NULL);
                            }
                    }

                /*gfal2 debug logging*/
                if (opts.debug == true)
                    {
                        logger.INFO() << "Set the transfer to debug mode" << std::endl;
                        gfal_set_verbose(GFAL_VERBOSE_TRACE | GFAL_VERBOSE_VERBOSE | GFAL_VERBOSE_TRACE_PLUGIN);
                        gfal_log_set_handler((GLogFunc) log_func, NULL);
                    }

                if (!opts.sourceTokenDescription.empty())
                    gfalt_set_src_spacetoken(params, opts.sourceTokenDescription.c_str(), NULL);

                if (!opts.destTokenDescription.empty())
                    gfalt_set_dst_spacetoken(params, opts.destTokenDescription.c_str(), NULL);

                gfalt_set_create_parent_dir(params, TRUE, NULL);

                //get checksum timeout from gfal2
                logger.INFO() << "Get checksum timeout" << std::endl;
                int checksumTimeout = gfal2_get_opt_integer(handle, "GRIDFTP PLUGIN", "CHECKSUM_CALC_TIMEOUT", NULL);
                msg_ifce::getInstance()->set_checksum_timeout(&tr_completed, checksumTimeout);

                /*Checksuming*/
                if (currentTransfer.checksumMethod)
                    {
                        // Set checksum check
                        gfalt_set_checksum_check(params, TRUE, NULL);
                        if (currentTransfer.checksumMethod == UrlCopyOpts::CompareChecksum::CHECKSUM_RELAXED)
                            {
                                gfal2_set_opt_boolean(handle, "SRM PLUGIN", "ALLOW_EMPTY_SOURCE_CHECKSUM", TRUE, NULL);
                                gfal2_set_opt_boolean(handle, "GRIDFTP PLUGIN", "SKIP_SOURCE_CHECKSUM", TRUE, NULL);
                            }

                        if (!currentTransfer.checksumValue.empty() && currentTransfer.checksumValue != "x")   //user provided checksum
                            {
                                logger.INFO() << "User  provided checksum" << std::endl;
                                gfalt_set_user_defined_checksum(params,
                                                                currentTransfer.checksumAlgorithm.c_str(),
                                                                currentTransfer.checksumValue.c_str(),
                                                                NULL);
                            }
                        else    //use auto checksum
                            {
                                logger.INFO() << "Calculate checksum auto" << std::endl;
                            }
                    }

                /* Stat source file */
                logger.INFO() << "Stat the source surl start" << std::endl;
                int errorCode = statWithRetries(handle, "SOURCE", currentTransfer.sourceUrl, &currentTransfer.fileSize, &errorMessage);
                if (errorCode != 0)
                    {
                        logger.ERROR() << "Failed to get source file size, errno:"
                                       << errorCode << ", " << errorMessage << std::endl;

                        errorMessage = "Failed to get source file size: " + errorMessage;
                        errorScope = SOURCE;
                        reasonClass = mapErrnoToString(errorCode);
                        errorPhase = TRANSFER_PREPARATION;
                        retry = retryTransfer(errorCode, "SOURCE", errorMessage);
                        goto stop;
                    }

                if (currentTransfer.fileSize == 0)
                    {
                        errorMessage = "Source file size is 0";
                        logger.ERROR() << errorMessage << std::endl;
                        errorScope = SOURCE;
                        reasonClass = mapErrnoToString(gfal_posix_code_error());
                        errorPhase = TRANSFER_PREPARATION;
                        retry = true;
                        goto stop;
                    }

                if (currentTransfer.userFileSize != 0 && currentTransfer.userFileSize != currentTransfer.fileSize)
                    {
                        std::stringstream error_;
                        error_ << "User specified source file size is " << currentTransfer.userFileSize << " but stat returned " << currentTransfer.fileSize;
                        errorMessage = error_.str();
                        logger.ERROR() << errorMessage << std::endl;
                        errorScope = SOURCE;
                        reasonClass = mapErrnoToString(gfal_posix_code_error());
                        errorPhase = TRANSFER_PREPARATION;
                        retry = true;
                        goto stop;
                    }

                logger.INFO() << "Source file size: " << currentTransfer.fileSize << std::endl;
                msg_ifce::getInstance()->set_file_size(&tr_completed, currentTransfer.fileSize);

                //overwrite dest file if exists
                if (opts.overwrite)
                    {
                        logger.INFO() << "Overwrite is enabled" << std::endl;
                        gfalt_set_replace_existing_file(params, TRUE, NULL);
                    }
                else
                    {
                        struct stat statbufdestOver;
                        //if overwrite is not enabled, check if  exists and stop the transfer if it does
                        logger.INFO() << "Stat the dest surl to check if file already exists" << std::endl;
                        errorMessage = ""; //reset
                        if (gfal2_stat(handle, (currentTransfer.destUrl).c_str(), &statbufdestOver, &tmp_err) == 0)
                            {
                                double dest_sizeOver = (double) statbufdestOver.st_size;
                                if(dest_sizeOver > 0)
                                    {
                                        errorMessage = "Destination file already exists and overwrite is not enabled";
                                        logger.ERROR() << errorMessage << std::endl;
                                        errorScope = DESTINATION;
                                        reasonClass = mapErrnoToString(gfal_posix_code_error());
                                        errorPhase = TRANSFER_PREPARATION;
                                        retry = false;
                                        goto stop;
                                    }
                            }
                        else
                            {
                                g_clear_error(&tmp_err); //don't do anything
                            }
                    }

                unsigned int experimentalTimeout = adjustTimeoutBasedOnSize(currentTransfer.fileSize, opts.timeout);
                if(!opts.manualConfig || opts.autoTunned || opts.timeout==0)
                    opts.timeout = experimentalTimeout;
                gfalt_set_timeout(params, opts.timeout, NULL);
                msg_ifce::getInstance()->set_transfer_timeout(&tr_completed, opts.timeout);
                logger.INFO() << "Timeout:" << opts.timeout << std::endl;
                globalTimeout = experimentalTimeout + 500;
                logger.INFO() << "Resetting global timeout thread to " << globalTimeout << " seconds" << std::endl;

                unsigned int experimentalNstreams = adjustStreamsBasedOnSize(currentTransfer.fileSize, opts.nStreams);
                if(!opts.manualConfig || opts.autoTunned || opts.nStreams==0)
                    {
                        if(true == lanTransfer(fileManagement.getSourceHostname(), fileManagement.getDestHostname()))
                            opts.nStreams = (experimentalNstreams * 2) > 16? 16: experimentalNstreams * 2;
                        else
                            opts.nStreams = experimentalNstreams;
                    }
                gfalt_set_nbstreams(params, opts.nStreams, NULL);
                msg_ifce::getInstance()->set_number_of_streams(&tr_completed, opts.nStreams);
                logger.INFO() << "nbstreams:" << opts.nStreams << std::endl;

                //update protocol stuff
                logger.INFO() << "Update protocol stuff, report back to the server" << std::endl;
                reporter.timeout = opts.timeout;
                reporter.nostreams = opts.nStreams;
                reporter.buffersize = opts.tcpBuffersize;
                reporter.sendMessage(currentTransfer.throughput, false,
                                     opts.jobId, currentTransfer.fileId,
                                     "UPDATE", "",
                                     0, currentTransfer.fileSize);

                gfalt_set_tcp_buffer_size(params, opts.tcpBuffersize, NULL);
                gfalt_set_monitor_callback(params, &call_perf, NULL);

                //check all params before passed to gfal2
                if ((currentTransfer.sourceUrl).c_str() == NULL || (currentTransfer.destUrl).c_str() == NULL)
                    {
                        errorMessage = "Failed to get source or dest surl";
                        logger.ERROR() << errorMessage << std::endl;
                        errorScope = TRANSFER;
                        reasonClass = GENERAL_FAILURE;
                        errorPhase = TRANSFER;
                        goto stop;
                    }


                logger.INFO() << "Transfer Starting" << std::endl;
                if (gfalt_copy_file(handle, params, (currentTransfer.sourceUrl).c_str(), (currentTransfer.destUrl).c_str(), &tmp_err) != 0)
                    {
                        if (tmp_err != NULL && tmp_err->message != NULL)
                            {
                                logger.ERROR() << "Transfer failed - errno: " << tmp_err->code
                                               << " Error message:" << tmp_err->message
                                               << std::endl;
                                if (tmp_err->code == 110)
                                    {
                                        errorMessage = std::string(tmp_err->message);
                                        errorMessage += ", operation timeout";
                                    }
                                else
                                    {
                                        errorMessage = std::string(tmp_err->message);
                                    }
                                errorScope = TRANSFER;
                                reasonClass = mapErrnoToString(tmp_err->code);
                                errorPhase = TRANSFER;
                            }
                        else
                            {
                                logger.ERROR() << "Transfer failed - Error message: Unresolved error" << std::endl;
                                errorMessage = std::string("Unresolved error");
                                errorScope = TRANSFER;
                                reasonClass = GENERAL_FAILURE;
                                errorPhase = TRANSFER;
                            }
                        if(tmp_err)
                            {
                                std::string message;
                                if (tmp_err->message)
                                    message.assign(tmp_err->message);
                                retry = retryTransfer(tmp_err->code, "TRANSFER", message);
                            }
                        g_clear_error(&tmp_err);
                        goto stop;
                    }
                else
                    {
                        logger.INFO() << "Transfer completed successfully" << std::endl;
                    }


                currentTransfer.transferredBytes = currentTransfer.fileSize;
                msg_ifce::getInstance()->set_total_bytes_transfered(&tr_completed, currentTransfer.transferredBytes);

                logger.INFO() << "Stat the dest surl start" << std::endl;
                off_t dest_size;
                errorCode = statWithRetries(handle, "DESTINATION", currentTransfer.destUrl, &dest_size, &errorMessage);
                if (errorCode != 0)
                    {
                        logger.ERROR() << "Failed to get dest file size, errno:" << errorCode << ", "
                                       << errorMessage << std::endl;
                        errorMessage = "Failed to get dest file size: " + errorMessage;
                        errorScope = DESTINATION;
                        reasonClass = mapErrnoToString(errorCode);
                        errorPhase = TRANSFER_FINALIZATION;
                        retry = retryTransfer(errorCode, "DESTINATION", errorMessage);
                        goto stop;
                    }

                if (dest_size <= 0)
                    {
                        errorMessage = "Destination file size is 0";
                        logger.ERROR() << errorMessage << std::endl;
                        errorScope = DESTINATION;
                        reasonClass = mapErrnoToString(gfal_posix_code_error());
                        errorPhase = TRANSFER_FINALIZATION;
                        retry = true;
                        goto stop;
                    }

                if (currentTransfer.userFileSize != 0 && currentTransfer.userFileSize != dest_size)
                    {
                        std::stringstream error_;
                        error_ << "User specified destination file size is " << currentTransfer.userFileSize << " but stat returned " << dest_size;
                        errorMessage = error_.str();
                        logger.ERROR() << errorMessage << std::endl;
                        errorScope = DESTINATION;
                        reasonClass = mapErrnoToString(gfal_posix_code_error());
                        errorPhase = TRANSFER_FINALIZATION;
                        retry = true;
                        goto stop;
                    }

                logger.INFO() << "Destination file size: " << dest_size << std::endl;

                //check source and dest file sizes
                if (currentTransfer.fileSize == dest_size)
                    {
                        logger.INFO() << "Source and destination file size matching" << std::endl;
                    }
                else
                    {
                        logger.ERROR() << "Source and destination file size are different" << std::endl;
                        errorMessage = "Source and destination file size mismatch";
                        errorScope = DESTINATION;
                        reasonClass = mapErrnoToString(gfal_posix_code_error());
                        errorPhase = TRANSFER_FINALIZATION;
                        goto stop;
                    }

                gfalt_set_user_data(params, NULL, NULL);
            }//logStream
stop:
            msg_ifce::getInstance()->set_transfer_error_scope(&tr_completed, errorScope);
            msg_ifce::getInstance()->set_transfer_error_category(&tr_completed, reasonClass);
            msg_ifce::getInstance()->set_failure_phase(&tr_completed, errorPhase);
            msg_ifce::getInstance()->set_transfer_error_message(&tr_completed, errorMessage);
            if (errorMessage.length() > 0)
                {
                    msg_ifce::getInstance()->set_final_transfer_state(&tr_completed, "Error");
                    reporter.timeout = opts.timeout;
                    reporter.nostreams = opts.nStreams;
                    reporter.buffersize = opts.tcpBuffersize;
                    if (!terminalState)
                        {
                            logger.INFO() << "Report FAILED back to the server" << std::endl;
                            reporter.sendTerminal(currentTransfer.throughput, retry,
                                                  opts.jobId, currentTransfer.fileId,
                                                  "FAILED", errorMessage,
                                                  currentTransfer.getTransferDurationInSeconds(),
                                                  currentTransfer.fileSize);
                        }

                    // In case of failure, if this is a multihop transfer, set to fail
                    // all the remaining transfers
                    if (opts.multihop)
                        {
                            logger.ERROR() << "Setting to fail the remaining transfers" << std::endl;
                            setRemainingTransfersToFailed(transferList, ii);
                            break; // exit the loop
                        }
                }
            else
                {
                    msg_ifce::getInstance()->set_final_transfer_state(&tr_completed, "Ok");
                    reporter.timeout = opts.timeout;
                    reporter.nostreams = opts.nStreams;
                    reporter.buffersize = opts.tcpBuffersize;
                    logger.INFO() << "Report FINISHED back to the server" << std::endl;
                    reporter.sendTerminal(currentTransfer.throughput, false,
                                          opts.jobId, currentTransfer.fileId,
                                          "FINISHED", errorMessage,
                                          currentTransfer.getTransferDurationInSeconds(),
                                          currentTransfer.fileSize);
                    /*unpin the file here and report the result in the log file...*/
                    g_clear_error(&tmp_err);

                    if (opts.bringOnline > 0)
                        {
                            logger.INFO() << "Token will be unpinned: " << currentTransfer.tokenBringOnline << std::endl;
                            if(gfal2_release_file(handle, (currentTransfer.sourceUrl).c_str(), (currentTransfer.tokenBringOnline).c_str(), &tmp_err) < 0)
                                {
                                    if (tmp_err && tmp_err->message)
                                        {
                                            logger.WARNING() << "Failed unpinning the file: " << std::string(tmp_err->message) << std::endl;
                                        }
                                }
                            else
                                {
                                    logger.INFO() << "Token unpinned: " << currentTransfer.tokenBringOnline << std::endl;
                                }
                        }
                }
            logger.INFO() << "Send monitoring complete message" << std::endl;
            msg_ifce::getInstance()->set_tr_timestamp_complete(&tr_completed, msg_ifce::getInstance()->getTimestamp());

            if(opts.monitoringMessages)
                msg_ifce::getInstance()->SendTransferFinishMessage(&tr_completed);

            std::string archiveErr = fileManagement.archive();
            if (!archiveErr.empty())
                logger.ERROR() << "Could not archive: " << archiveErr << std::endl;
            reporter.sendLog(opts.jobId, currentTransfer.fileId, fileManagement._getLogArchivedFileFullPath(),
                             opts.debug);
        }//end for reuse loop

    if (params)
        {
            gfalt_params_handle_delete(params, NULL);
            params = NULL;
        }
    if (handle)
        {
            gfal_context_free(handle);
            handle = NULL;
        }

    if (cert)
        {
            delete cert;
            cert = NULL;
        }

    if (opts.areTransfersOnFile() && readFile.length() > 0)
        unlink(readFile.c_str());

    StaticSslLocking::kill_locks();

    sleep(1);
    return EXIT_SUCCESS;
}
