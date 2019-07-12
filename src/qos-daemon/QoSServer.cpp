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

#include <qos-daemon/task/PollTask.h>
#include <qos-daemon/task/ArchivingPollTask.h>
#include <qos-daemon/task/CDMIPollTask.h>
#include <qos-daemon/task/WaitingRoom.h>
#include "QoSServer.h"

#include "common/Logger.h"
#include "config/ServerConfig.h"

#include "server/DrainMode.h"


#include "fetch/FetchStaging.h"
#include "fetch/FetchCancelStaging.h"
#include "fetch/CDMIFetchQosTransition.h"
#include "fetch/FetchArchiving.h"
#include "fetch/FetchDeletion.h"
#include "state/StagingStateUpdater.h"
#include "state/DeletionStateUpdater.h"

using namespace fts3::common;
using namespace fts3::config;


/// Run in the background updating the status of this server
/// This is a thread! Do not let exceptions exit the scope
static void heartBeat(void)
{
    unsigned myIndex=0, count=0;
    unsigned hashStart=0, hashEnd=0;
    const std::string service_name = "fts_qosdaemon";
    int heartBeatInterval;
    try {
        heartBeatInterval = ServerConfig::instance().get<int>("HeartBeatInterval");
    }
    catch (...) {
        FTS3_COMMON_LOGGER_NEWLOG(CRIT) << "Could not get the heartbeat interval" << commit;
        _exit(1);
    }
    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Using heartbeat interval " << heartBeatInterval << commit;

    while (!boost::this_thread::interruption_requested()) {
        try {
            //check if draining is on
            if (fts3::server::DrainMode::instance()) {
                FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Set to drain mode, no more checking files for this instance!" << commit;
                boost::this_thread::sleep(boost::posix_time::seconds(15));
                continue;
            }

            db::DBSingleton::instance().getDBObjectInstance()->updateHeartBeat(
                &myIndex, &count, &hashStart, &hashEnd, service_name);

            FTS3_COMMON_LOGGER_NEWLOG(DEBUG) << "Systole: host " << myIndex << " out of " << count
                << " [" << std::hex << hashStart << ':' << std::hex << hashEnd << ']'
                << std::dec
                << commit;

            boost::this_thread::sleep(boost::posix_time::seconds(heartBeatInterval));
        }
        catch (const std::exception& ex) {
            FTS3_COMMON_LOGGER_NEWLOG(ERR) << ex.what() << commit;
            boost::this_thread::sleep(boost::posix_time::seconds(1));
        }
        catch (const boost::thread_interrupted&) {
            FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Hearbeat interruption requested" << commit;
            return;
        }
        catch (...) {
            boost::this_thread::sleep(boost::posix_time::seconds(1));
            FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Unhandled exception" << commit;
        }
    }
}


QoSServer::QoSServer(): threadpool(10)
{
}


QoSServer::~QoSServer()
{
    stop();
    wait();
}


void QoSServer::start(void)
{
    setenv("GLOBUS_THREAD_MODEL", "pthread", 1);

    std::string infosys = ServerConfig::instance().get<std::string>("Infosys");
    Gfal2Task::createPrototype (infosys);

    FetchStaging fs(threadpool);
    //CDMIFetchQosTransition cdmifs(threadpool);
    FetchCancelStaging fcs(threadpool);
    FetchDeletion fd(threadpool);
    FetchArchiving fa(threadpool);

    waitingRoom.attach(threadpool);
    //cdmiWaitingRoom.attach(threadpool);

    systemThreads.create_thread(boost::bind(&WaitingRoom<PollTask>::run, &waitingRoom));
    systemThreads.create_thread(boost::bind(&FetchStaging::fetch, fs));

    //disable CDMI threads for now
    //systemThreads.create_thread(boost::bind(&WaitingRoom<CDMIPollTask>::run, &cdmiWaitingRoom));
    //systemThreads.create_thread(boost::bind(&CDMIFetchQosTransition::fetch, cdmifs));

    systemThreads.create_thread(boost::bind(&WaitingRoom<ArchivingPollTask>::run, &archivingWaitingRoom));
    systemThreads.create_thread(boost::bind(&FetchArchiving::fetch, fa));

    systemThreads.create_thread(boost::bind(&FetchCancelStaging::fetch, fcs));
    systemThreads.create_thread(boost::bind(&FetchDeletion::fetch, fd));
    systemThreads.create_thread(boost::bind(&DeletionStateUpdater::run, &deletionStateUpdater));
    systemThreads.create_thread(boost::bind(&StagingStateUpdater::run, &stagingStateUpdater));
    systemThreads.create_thread(heartBeat);
}


void QoSServer::wait(void)
{
    systemThreads.join_all();
    threadpool.join();
}


void QoSServer::stop(void)
{
    stagingStateUpdater.recover();
    deletionStateUpdater.recover();
    threadpool.interrupt();
    systemThreads.interrupt_all();
}
