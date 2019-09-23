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

#pragma once
#ifndef ARCHIVINGPOLLTASK_H_
#define ARCHIVINGPOLLTASK_H_

#include <algorithm>
#include <iterator>
#include <set>
#include <string>
#include <unordered_map>

#include <boost/thread.hpp>

#include "db/generic/SingleDbInstance.h"

#include "ArchivingTask.h"


/**
 * A poll task: checks whether a list of url has been archived
 *
 * If the archive operation is not finished yet spawns another ArchivingPollTask.
 * If the operation fails and retries are set spawns another ArchivingPollTask.
 *
 *
 * @see ArchivingTask
 */
class ArchivingPollTask : public ArchivingTask
{
public:
    /**
     * Creates a ArchivingPollTask from ArchiveContext (for recovery purposes only)
     *
     * @param ctx : staging context (recover from DB after crash)
     */
	ArchivingPollTask(const ArchivingContext &ctx) :
		ArchivingTask(ctx), nPolls(0), wait_until(0)
    {
        auto surls = ctx.getSurls();
        boost::unique_lock<boost::shared_mutex> lock(mx);
        active_urls.insert(surls.begin(), surls.end());
    }

    /**
     * Creates a new ArchivingPollTask task from a ArchivingTask
     *
     * @param copy : a archive task (stills the gfal2 context of this object)
     */
	ArchivingPollTask(ArchivingTask && copy) :
		ArchivingTask(std::move(copy)),  nPolls(0), wait_until()
    {
    }

    /**
     * Move constructor
     */
	ArchivingPollTask(ArchivingPollTask && copy) :
		ArchivingTask(std::move(copy)),  nPolls(copy.nPolls), wait_until(
            copy.wait_until)
    {
    }

    /**
     * Destructor
     */
    virtual ~ArchivingPollTask() {}

    /**
     * The routine is executed by the thread pool
     */
    virtual void run(const boost::any &);

    /**
     * @return : true if the task is still waiting, false otherwise
     */
    bool waiting(time_t now)
    {
        return wait_until > now;
    }

private:
    /// checks if the archive  task was cancelled and removes those URLs that were from the context
    void handle_canceled();

    /// checks if the archive  task timed-out and removes respective URLs from the context
    bool timeout_occurred();

    /// aborts the operation for the given URLs
    void abort(std::set<std::string> const & urls, bool report = true);

    /**
     * Gets the interval after next polling should be done
     *
     * @param nPolls : number of polls already done
     */
    static time_t getPollInterval(int nPolls)
    {
        if (nPolls > 9)
            return 600;
        else
            return (2 << nPolls);
    }

    /// number of archive task polls
    int nPolls;

    /// wait in the wait room until given time
    time_t wait_until;
};

#endif // ARCHIVINGPOLLTASK_H_
