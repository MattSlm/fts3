/*
 *	Copyright notice:
 *	Copyright © Members of the EMI Collaboration, 2010.
 *
 *	See www.eu-emi.eu for details on the copyright holders
 *
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or impltnsied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 *
 * JobSubmitter.h
 *
 *  Created on: Mar 7, 2012
 *      Author: Michal Simon
 */

#ifndef JOBSUBMITTER_H_
#define JOBSUBMITTER_H_

#include "ws-ifce/gsoap/gsoap_stubs.h"
#include "db/generic/GenericDbIfce.h"
#include "common/JobParameterHandler.h"

#include "BlacklistInspector.h"

#include <string>
#include <map>
#include <vector>
#include <list>
#include <utility>

#include <boost/regex.hpp>
#include <boost/tuple/tuple.hpp>

namespace fts3
{
namespace ws
{

using namespace std;
using namespace fts3::common;
using namespace boost;

/**
 * The JobSubmitter class takes care of submitting transfers
 *
 * Depending on the request (submitTransfer, submitTransfer2, submitTransfer3)
 * different constructors should be used
 *
 */
class JobSubmitter
{

public:
    /**
     * Constructor - creates a submitter object that should be used
     * by submitTransfer and submitTransfer2 requests
     *
     * @param soap - the soap object that is serving the given request
     * @param job - the job that has to be submitted
     * @param delegation - should be true if delegation is being used
     */
    JobSubmitter(soap* ctx, tns3__TransferJob *job, bool delegation);

    /**
     * Constructor - creates a submitter object that should be used
     * by submitTransfer3 requests
     *
     * @param soap - the soap object that is serving the given request
     * @param job - the job that has to be submitted
     */
    JobSubmitter(soap* ctx, tns3__TransferJob2 *job);

    /**
     * Constructor - creates a submitter object that should be used
     * by submitTransfer4 requests
     *
     * @param soap - the soap object that is serving the given request
     * @param job - the job that has to be submitted
     */
    JobSubmitter(soap* ctx, tns3__TransferJob3 *job);

    /**
     * Destructor
     */
    virtual ~JobSubmitter();

    /**
     * submits the job
     */
    string submit();

    /**
     * extracts SE name from URL
     */
    static string fileUrlToSeName(string url, bool source = false); // it is not the best place for it!

private:

    /// DB instance
    GenericDbIfce* db;

    /// job ID
    string id;
    /// user DN
    string dn;
    /// user VO
    string vo;
    /// delegation ID
    string delegationId;
    /// copy lifetime pin
    int copyPinLifeTime;

    /// maps job parameter values to their names
    JobParameterHandler params;

    /**
     * the job elements that have to be submitted (each job is a tuple of source,
     * destination, and optionally checksum)
     */
    list<job_element_tupple> jobs;

    /**
     * The common initialisation for both parameterised constructors
     *
     * @param job - the transfer job
     */
    template <typename JOB>
    void init(soap* ctx, JOB* job);

    /**
     * Extracts the activity name from file metadata
     * in case if there is no activity it returns 'default'
     */
    string getActivity(std::string const * const activity);

    /**
     * Checks whether the right protocol has been used
     *
     * @file - source or destination file
     * @return true if right protol has been used
     */
    static void checkProtocol(string file, bool source);

    /// the regular expression for parsing URLs
    static const regex fileUrlRegex;
    /// srm protocol prefix
    static const string srm_protocol;
    /// true if at least one file was using srm
    bool srm_source;

    /// source SE
    string sourceSe;
    /// destination SE
    string destinationSe;

    /// initial state
    string initialState;
};

}
}

#endif /* JOBSUBMITTER_H_ */