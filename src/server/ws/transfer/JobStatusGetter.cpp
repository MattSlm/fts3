/*
 * Copyright (c) CERN 2013-2015
 *
 * Copyright (c) Members of the EMI Collaboration. 2010-2013
 *  See  http://www.eu-emi.eu/partners for details on the copyright
 *  holders.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "JobStatusGetter.h"

#include "common/JobStatusHandler.h"
#include "common/error.h"


using namespace fts3::common;


namespace fts3
{
namespace ws
{

template <>
tns3__FileTransferStatus* JobStatusGetter::make_status<tns3__FileTransferStatus>()
{
    return soap_new_tns3__FileTransferStatus(ctx, -1);
}

template <>
tns3__FileTransferStatus2* JobStatusGetter::make_status<tns3__FileTransferStatus2>()
{
    return soap_new_tns3__FileTransferStatus2(ctx, -1);
}

template <>
tns3__TransferJobSummary* JobStatusGetter::make_summary<tns3__TransferJobSummary>()
{
    return soap_new_tns3__TransferJobSummary(ctx, -1);
}

template <>
tns3__TransferJobSummary2* JobStatusGetter::make_summary<tns3__TransferJobSummary2>()
{
    return soap_new_tns3__TransferJobSummary2(ctx, -1);
}

template void JobStatusGetter::file_status<tns3__FileTransferStatus>(std::vector<tns3__FileTransferStatus*> &, bool);
template void JobStatusGetter::file_status<tns3__FileTransferStatus2>(std::vector<tns3__FileTransferStatus2*> &, bool);

template <typename STATUS>
void JobStatusGetter::file_status(std::vector<STATUS*> & ret, bool glite)
{
    bool dm_job = db.isDmJob(jobId);
    std::vector<FileTransferStatus> fileStatuses;

    if (dm_job)
        db.getDmStatuses(jobId, archive, offset, limit, fileStatuses);
    else
        db.getTransferStatuses(jobId, archive, offset, limit, fileStatuses);

    for (auto it = fileStatuses.begin(); it != fileStatuses.end(); ++it)
        {
            FileTransferStatus& tmp = *it;
            tmp.fileState = to_glite_state(tmp.fileState, glite);

            STATUS* status = make_status<STATUS>();

            status->destSURL = soap_new_std__string(ctx, -1);
            *status->destSURL = tmp.destSurl;

            status->logicalName = soap_new_std__string(ctx, -1);
            *status->logicalName = tmp.logicalName;

            status->reason = soap_new_std__string(ctx, -1);
            *status->reason = tmp.reason;

            status->reason_USCOREclass = soap_new_std__string(ctx, -1);
            *status->reason_USCOREclass = tmp.reasonClass;

            status->sourceSURL = soap_new_std__string(ctx, -1);
            *status->sourceSURL = tmp.sourceSurl;

            status->transferFileState = soap_new_std__string(ctx, -1);
            *status->transferFileState = tmp.fileState;

            if(tmp.fileState == "NOT_USED")
                {
                    status->duration = 0;
                    status->numFailures = 0;
                }
            else
                {
                    if (tmp.finishTime > 0 && tmp.startTime > 0)
                        status->duration = tmp.finishTime - tmp.startTime;
                    else if(tmp.finishTime <= 0 && tmp.startTime > 0)
                        status->duration = time(NULL) - tmp.startTime;
                    else
                        status->duration = 0;
                    status->numFailures = tmp.numFailures;
                }

            status->staging = (int64_t*)soap_malloc(ctx, sizeof(int64_t));
            if (tmp.stagingFinished > 0 && tmp.stagingStart > 0)
                *status->staging = tmp.stagingFinished - tmp.stagingStart;
            else if (tmp.stagingFinished <= 0 && tmp.stagingStart > 0)
                *status->staging = time(NULL) - tmp.stagingStart;
            else
                status->staging = 0;

            // Retries only on request! This type of information exists only for transfer jobs
            if (retry && !dm_job)
                {
                    std::vector<FileRetry> retries;
                    db.getTransferRetries(tmp.fileId, retries);

                    for (auto ri = retries.begin(); ri != retries.end(); ++ri)
                        {
                            tns3__FileTransferRetry* retry = soap_new_tns3__FileTransferRetry(ctx, -1);
                            retry->attempt  = ri->attempt;
                            retry->datetime = ri->datetime;
                            retry->reason   = ri->reason;
                            status->retries.push_back(retry);
                        }
                }
            ret.push_back(status);
        }
}

template void JobStatusGetter::job_summary<tns3__TransferJobSummary>(tns3__TransferJobSummary * & ret, bool glite);
template void JobStatusGetter::job_summary<tns3__TransferJobSummary2>(tns3__TransferJobSummary2 * & ret, bool glite);

template <typename SUMMARY>
void JobStatusGetter::job_summary(SUMMARY * & ret, bool glite)
{
    boost::optional<Job> job(db.getJob(jobId, archive));

    if(job)
        {
            bool dm_job = db.isDmJob(jobId);
            std::vector<FileTransferStatus> fileStatuses;

            if (dm_job)
                db.getDmStatuses(jobId, archive, 0, 0, fileStatuses);
            else
                db.getTransferStatuses(jobId, archive, 0, 0, fileStatuses);

            ret = make_summary<SUMMARY>();
            ret->jobStatus = to_gsoap_status(*job.get_ptr(), fileStatuses.size(), glite);

            JobStatusHandler& handler = JobStatusHandler::getInstance();
            ret->numActive = handler.countInState(JobStatusHandler::FTS3_STATUS_ACTIVE, fileStatuses);
            ret->numCanceled = handler.countInState(JobStatusHandler::FTS3_STATUS_CANCELED, fileStatuses);
            ret->numSubmitted = handler.countInState(JobStatusHandler::FTS3_STATUS_SUBMITTED, fileStatuses);
            ret->numFinished = handler.countInState(JobStatusHandler::FTS3_STATUS_FINISHED, fileStatuses);
            count_ready(ret, handler.countInState(JobStatusHandler::FTS3_STATUS_READY, fileStatuses));
            ret->numFailed = handler.countInState(JobStatusHandler::FTS3_STATUS_FAILED, fileStatuses);
            if (glite)
                {
                    ret->numSubmitted += handler.countInState(JobStatusHandler::FTS3_STATUS_STAGING, fileStatuses);
                    ret->numSubmitted += handler.countInState(JobStatusHandler::FTS3_STATUS_DELETE, fileStatuses);
                    ret->numActive += handler.countInState(JobStatusHandler::FTS3_STATUS_STARTED, fileStatuses);
                }
            else
                {
                    ret->numStaging = handler.countInState(JobStatusHandler::FTS3_STATUS_STAGING, fileStatuses);
                    ret->numStarted = handler.countInState(JobStatusHandler::FTS3_STATUS_STARTED, fileStatuses);
                    ret->numDelete = handler.countInState(JobStatusHandler::FTS3_STATUS_DELETE, fileStatuses);
                }
        }
    else
        {
            if (!glite) throw Err_Custom("requestID <" + jobId + "> was not found");
            ret = make_summary<SUMMARY>();
            ret->jobStatus = handleStatusExceptionForGLite();
        }
}

std::string JobStatusGetter::to_glite_state(std::string const & state, bool glite)
{
    // if it is not for glite
    if (!glite) return state;
    // check if it is fts3 only state
    if (state == "STARTED") return "ACTIVE";
    if (state == "STAGING") return "SUBMITTED";
    if (state == "DELETE") return "SUBMITTED";
    // if not there's nothing to do
    return state;
}

tns3__JobStatus * JobStatusGetter::to_gsoap_status(Job const & job, int numFiles, bool glite)
{
    tns3__JobStatus * status = soap_new_tns3__JobStatus(ctx, -1);

    status->clientDN = soap_new_std__string(ctx, -1);
    *status->clientDN = job.userDn;

    status->jobID = soap_new_std__string(ctx, -1);
    *status->jobID = job.jobId;

    status->jobStatus = soap_new_std__string(ctx, -1);
    *status->jobStatus = to_glite_state(job.jobState, glite);

    status->reason = soap_new_std__string(ctx, -1);
    *status->reason = job.reason;

    status->voName = soap_new_std__string(ctx, -1);
    *status->voName = job.voName;

    // change sec precision to msec precision so it is compatible with old glite cli
    status->submitTime = job.submitTime * 1000;
    status->numFiles = numFiles;
    status->priority = job.priority;

    return status;
}

void JobStatusGetter::job_status(tns3__JobStatus * & status, bool glite)
{
    boost::optional<Job> job(db.getJob(jobId, archive));

    if(job)
        {
            std::vector<FileTransferStatus> fileStatuses;
            db.getTransferStatuses(jobId, archive, 0, 0, fileStatuses);
            status = to_gsoap_status(*job.get_ptr(), fileStatuses.size(), glite);
        }
    else
        {
            if (!glite) throw Err_Custom("requestID <" + jobId + "> was not found");
            status = handleStatusExceptionForGLite();
        }
}

tns3__JobStatus* JobStatusGetter::handleStatusExceptionForGLite()
{
    // the value to be returned
    tns3__JobStatus* status;

    // For now since the glite clients are not compatible with exception from our gsoap version
    // we do not rise an exception, instead we send the error string as the job status
    // and at the begining of it we attache backspaces in order to erase 'Unknown transfer state '

    // the string that should be erased
    std::string replace = "Unknown transfer state ";

    // backspace
    const char bs = 8;
    // error message
    std::string msg = "getTransferJobStatus: RequestID <" + jobId + "> was not found";

    // add backspaces at the begining of the error message
    for (size_t i = 0; i < replace.size(); i++)
        msg = bs + msg;

    // create the status object
    status = soap_new_tns3__JobStatus(ctx, -1);

    // create the string for the error message
    status->jobStatus = soap_new_std__string(ctx, -1);
    *status->jobStatus = msg;

    // set all the rest to NULLs
    status->clientDN = 0;
    status->jobID = 0;
    status->numFiles = 0;
    status->priority = 0;
    status->reason = 0;
    status->voName = 0;
    status->submitTime = 0;

    return status;
}

JobStatusGetter::~JobStatusGetter()
{
}


} /* namespace ws */
} /* namespace fts3 */
