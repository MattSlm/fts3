/*
 *	Copyright notice:
 *	Copyright © Members of the EMI Collaboration, 2010.
 *
 *	See www.eu-emi.eu for details on the copyright holders
 *
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use soap file except in compliance with the License.
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
 * JobStatusHandler.h
 *
 *  Created on: Mar 9, 2012
 *      Author: Michał Simon
 */

#ifndef JOBSTATUSHANDLER_H_
#define JOBSTATUSHANDLER_H_

#include "common/ThreadSafeInstanceHolder.h"

#include <map>
#include <string>
#include <iostream>

#include "db/generic/JobStatus.h"


using namespace std;
using namespace boost;

namespace fts3 { namespace common {

/**
 * The JobStatusHandler class takes care of job status related operations.
 *
 * The class implements Singleton design pattern (derives from ThreadSafeInstanceHolder),
 * and grands access to a map containing all state names and the corresponding
 * IDs. The class inherits after MonitorObject and is thread safe.
 */
class JobStatusHandler: public ThreadSafeInstanceHolder<JobStatusHandler> {

	friend class ThreadSafeInstanceHolder<JobStatusHandler>;

public:

	/**
	 * Possible Job statuses.
	 *
	 * Value lower than 0 indicates that the job failed, value equal to 0 indicates
	 * that the job has been finished, value greater than 0 indicates that the job
	 * is still being processed
	 */
	enum JobStatusEnum {
		FTS3_STATUS_FINISHEDDIRTY_ID = -4,
		FTS3_STATUS_CANCELED_ID = -3,
		FTS3_STATUS_UNKNOWN_ID = -2,
	    FTS3_STATUS_FAILED_ID = -1,
	    FTS3_STATUS_FINISHED_ID = 0,
	    FTS3_STATUS_SUBMITTED_ID,
	    FTS3_STATUS_READY_ID,
	    FTS3_STATUS_ACTIVE_ID,
	    FTS3_STATUS_STAGING_ID
	};

	///@{
	/**
	 * names of transfer job statuses
	 */
	static const string FTS3_STATUS_FINISHEDDIRTY;
    static const string FTS3_STATUS_CANCELED;
	static const string FTS3_STATUS_UNKNOWN;
    static const string FTS3_STATUS_FAILED;
    static const string FTS3_STATUS_FINISHED;
    static const string FTS3_STATUS_SUBMITTED;
    static const string FTS3_STATUS_READY;
    static const string FTS3_STATUS_ACTIVE;
    static const string FTS3_STATUS_STAGING;
	///@}

	/**
	 * Destructor (empty).
	 */
	virtual ~JobStatusHandler() {}

	/**
	 * Check whether the given status name indicates that a transfer is ready
	 *
	 * @return true if the transfer is ready
	 */
	bool isTransferFinished(string status);

	/**
	 * Check weather the given state is valid
	 * (is one of the recognized states)
	 *
	 * @return true if the the state is valid
	 */
	bool isStatusValid(string& status);

	/**
	 * Counts how many states in the given vector are equal to given state
	 *
	 * @param STATE
	 * @param states
	 *
	 * @return
	 */
	int countInState(string status, vector<JobStatus*>& statuses);

protected:

	/**
	 * Default constructor
	 *
	 * Private, should not be used
	 */
	JobStatusHandler();

private:

	/**
	 * Copying constructor
	 *
	 * Private, should not be used
	 */
	JobStatusHandler(JobStatusHandler const&);

	/**
	 * Assignment operator
	 *
	 * Private, should not be used
	 */
	JobStatusHandler & operator=(JobStatusHandler const&);

	/// maps job status name to job status id
	const map<string, JobStatusEnum> statusNameToId;
};


}
}
#endif /* JOBSTATUSHANDLER_H_ */
