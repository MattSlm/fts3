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

/** \file server.h FTS3 server class interface. */

#pragma once

#include "generic_server.h"
#include "threadpool.h"
#include "transfer_web_service.h"
#include "process_service.h"
#include "process_updater_service.h"
#include "process_log_service.h"
#include "process_updater_db_service.h"
#include "process_queue.h"

FTS3_SERVER_NAMESPACE_START

/* -------------------------------------------------------------------------- */

struct ServerTraits
{
    typedef TransferWebService TransferWebServiceType;
    typedef ProcessService ProcessServiceType;
    typedef ProcessUpdaterService ProcessUpdaterServiceType;
    typedef ProcessUpdaterDBService ProcessUpdaterDBServiceType;
    typedef ProcessLogService ProcessLogServiceType;
    typedef ProcessQueue ProcessQueueType;
    typedef ThreadPool::ThreadPool ThreadPoolType;
};

/* -------------------------------------------------------------------------- */

typedef GenericServer<ServerTraits> Server;

/* -------------------------------------------------------------------------- */

/** Singleton instance of the FTS 3 server. */
inline Server& theServer()
{
    static Server s;
    return s;
}

FTS3_SERVER_NAMESPACE_END

