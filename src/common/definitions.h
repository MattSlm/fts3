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

#include <glib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <ctime>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <math.h>

#define JOB_ID_LEN 36+1
#define FILE_ID_LEN 36
#define TRANFER_STATUS_LEN 50
#define TRANSFER_MESSAGE 1024
#define MAX_NUM_MSGS 50000
#define SOURCE_SE_ 100
#define DEST_SE_ 100


/**
 * Return MegaBytes per second from the given transferred bytes and duration
 */
inline double convertBtoM( double byte,  double duration)
{
    return ((((byte / duration) / 1024) / 1024) * 100) / 100;
}


/**
 * Round a double with a given number of digits
 */
inline double pround(double x, unsigned int digits)
{
    double shift = pow(10, digits);
    return rint(x * shift) / shift;
}

/**
 * Return MegaBytes per second given throughput in KiloBytes per second
 */
inline double convertKbToMb(double throughput)
{
    return throughput != 0.0? pround((throughput / 1024), 3): 0.0;
}

struct message_base
{
public:
    message_base(): msg_errno(false)
    {
    }
    int  msg_errno;

    void set_error(int errcode)
    {
        msg_errno = errcode;
    }
};

struct message: public message_base
{
public:

    message():file_id(0),
        process_id(0),
        timeInSecs(0.0),
        filesize(0),
        nostreams(2),
        timeout(3600),
        buffersize(0),
        timestamp(0),
        retry(false),
        throughput(0.0)
    {
        memset(job_id, 0, sizeof (job_id));
        memset(transfer_status, 0, sizeof (transfer_status));
        memset(transfer_message, 0, sizeof (transfer_message));
        memset(source_se, 0, sizeof (source_se));
        memset(dest_se, 0, sizeof (dest_se));
    }

    ~message()
    {
    }
    char job_id[JOB_ID_LEN];
    char transfer_status[TRANFER_STATUS_LEN];
    char transfer_message[TRANSFER_MESSAGE];
    char source_se[SOURCE_SE_];
    char dest_se[DEST_SE_];
    int file_id;
    pid_t process_id;
    double timeInSecs;
    double filesize;
    unsigned int nostreams;
    unsigned int timeout;
    unsigned int buffersize;
    boost::posix_time::time_duration::tick_type timestamp;
    bool retry;
    double throughput;
};


struct message_updater: public message_base
{
public:
    message_updater():file_id(0),process_id(0),timestamp(0), throughput(0.0), transferred(0.0)
    {
        memset(job_id, 0, sizeof (job_id));
        memset(source_surl, 0, sizeof (source_surl));
        memset(dest_surl, 0, sizeof (dest_surl));
        memset(source_turl, 0, sizeof (source_turl));
        memset(dest_turl, 0, sizeof (dest_turl));
        memset(transfer_status, 0, sizeof (transfer_status));
    }

    ~message_updater()
    {
    }
    char job_id[JOB_ID_LEN];
    int file_id;
    pid_t process_id;
    boost::posix_time::time_duration::tick_type timestamp;
    double throughput;
    double transferred;
    char source_surl[150];
    char dest_surl[150];
    char source_turl[150];
    char dest_turl[150];
    char transfer_status[TRANFER_STATUS_LEN];
};


struct message_log: public message_base
{
public:
    message_log():file_id(0), debugFile(false),timestamp(0)
    {
        memset(job_id, 0, sizeof (job_id));
        memset(host, 0, 255);
        memset(filePath, 0, 1024);
    }

    ~message_log()
    {
    }
    char job_id[JOB_ID_LEN];
    int file_id;
    char host[255];
    char filePath[1024];
    bool debugFile;
    boost::posix_time::time_duration::tick_type timestamp;
};


struct message_bringonline: public message_base
{
public:
    message_bringonline(): file_id(0)
    {
        memset(job_id, 0, sizeof (job_id));
        memset(transfer_status, 0, sizeof (transfer_status));
        memset(transfer_message, 0, sizeof (transfer_message));
    }

    ~message_bringonline()
    {
    }

    int file_id;
    char job_id[JOB_ID_LEN];
    char transfer_status[TRANFER_STATUS_LEN];
    char transfer_message[TRANSFER_MESSAGE];

};


struct message_state: public message_base
{
public:

    message_state():vo_name(""),source_se(""),dest_se(""),job_id(""),file_id(0),job_state(""),file_state(""),retry_counter(0),retry_max(0),job_metadata(""),file_metadata(""),timestamp("")
    {
    }

    ~message_state()
    {
    }

    std::string vo_name;
    std::string source_se;
    std::string dest_se;
    std::string job_id;
    int file_id;
    std::string job_state;
    std::string file_state;
    int retry_counter;
    int retry_max;
    std::string job_metadata;
    std::string file_metadata;
    std::string timestamp;
    std::string user_dn;
    std::string source_url;
    std::string dest_url;

};


struct message_monitoring: public message_base
{
public:
    message_monitoring():timestamp(0)
    {
        memset(msg, 0, sizeof (msg));
    }

    ~message_monitoring()
    {
    }
    char msg[5000];
    boost::posix_time::time_duration::tick_type timestamp;
};

struct message_sanity
{
public:
    message_sanity(): revertToSubmitted(false),
        cancelWaitingFiles(false),
        revertNotUsedFiles(false),
        forceFailTransfers(false),
        setToFailOldQueuedJobs(false),
        checkSanityState(false),
        cleanUpRecords(false),
        msgCron(false)
    {
    }

    ~message_sanity()
    {
    }
    bool revertToSubmitted;
    bool cancelWaitingFiles;
    bool revertNotUsedFiles;
    bool forceFailTransfers;
    bool setToFailOldQueuedJobs;
    bool checkSanityState;
    bool cleanUpRecords;
    bool msgCron;
};




#define DEFAULT_TIMEOUT 4000
#define MID_TIMEOUT 4000
const int timeouts[] = {4000};
const size_t timeoutslen = (sizeof (timeouts) / sizeof *(timeouts));

#define DEFAULT_NOSTREAMS 4
const int nostreams[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
const size_t nostreamslen = (sizeof (nostreams) / sizeof *(nostreams));


#define DEFAULT_BUFFSIZE 0
const int buffsizes[] = {1048576, 4194304, 5242880, 7340032, 8388608, 9437184, 11534336, 12582912, 14680064, 67108864};
const size_t buffsizeslen = (sizeof (buffsizes) / sizeof *(buffsizes));

/*low active / high active / jobs / files*/
const int mode_1[] = {2,4,3,5};
const int mode_2[] = {4,6,5,8};
const int mode_3[] = {6,8,7,10};
