/********************************************
 * Copyright @ Members of the EMI Collaboration, 2013.
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

#include <list>
#include <netdb.h>
#include <sstream>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#include "producer_consumer_common.h"
#include "config/serverconfig.h"
#include "GenericDbIfce.h"

using namespace FTS3_CONFIG_NAMESPACE;

namespace db
{

const int MAX_ACTIVE_PER_LINK = 70;

inline bool is_mreplica(std::list<job_element_tupple>& src_dest_pair)
{
    // if it has less than 2 pairs it wont be a m-replica
    if (src_dest_pair.size() < 2) return false;

    std::string destSurl = src_dest_pair.begin()->destination;

    std::list<job_element_tupple>::const_iterator iter;
    for (iter = src_dest_pair.begin(); iter != src_dest_pair.end(); ++iter)
        {
            if(destSurl != iter->destination)
                {
                    return false;
                }
        }

    return true;   
}


inline bool is_mhop(std::list<job_element_tupple>& src_dest_pair)
{
    // if it has less than 2 pairs it wont be a multi-hop
    if (src_dest_pair.size() < 2) return false;

    std::list<job_element_tupple>::const_iterator iter = src_dest_pair.begin();
    std::list<job_element_tupple>::const_iterator iter2 = src_dest_pair.begin();
    std::advance(iter2, 1);

    for (; iter2 != src_dest_pair.end(); ++iter, ++iter2)
		{
    		if (iter->destination != iter2->source) return false;
		}

    return true;
}




/*borrowed from http://oroboro.com/irregular-ema/*/
inline double exponentialMovingAverage( double sample, double alpha, double cur )
{
    cur = ( sample * alpha ) + (( 1-alpha) * cur );
    return cur;
}



/**
 * Returns the current time, plus the difference specified
 * in advance, in UTC time
 */
inline time_t getUTC(int advance)
{
    time_t now;
    if(advance > 0)
        now = time(NULL) + advance;
    else
        now = time(NULL);

    struct tm *utc;
    utc = gmtime(&now);
    return timegm(utc);
}

/**
 * Get timestamp in milliseconds, UTC, as a string
 */
static inline std::string getStrUTCTimestamp()
{
    //the number of milliseconds since the epoch
    time_t msec = getUTC(0) * 1000;
    std::ostringstream oss;
    oss << std::fixed << msec;
    return oss.str();
}



/**
 * From a transfer parameters string, return the timeout
 */
inline int extractTimeout(std::string & str)
{
    size_t found;
    found = str.find("timeout:");
    if (found != std::string::npos)
        {
            str = str.substr(found, str.length());
            size_t found2;
            found2 = str.find(",buffersize:");
            if (found2 != std::string::npos)
                {
                    str = str.substr(0, found2);
                    str = str.substr(8, str.length());
                    return atoi(str.c_str());
                }

        }
    return 0;
}



inline int extractStreams(std::string & str)
{
    size_t found;
    found = str.find("nostreams:");
    if (found != std::string::npos)
        {
            size_t found2;
            found2 = str.find(",timeout:");
            if (found2 != std::string::npos)
                {
                    str = str.substr(0, found2);
                    str = str.substr(10, str.length());
                    return atoi(str.c_str());
                }
        }
    return 0;
}


static inline void constructJSONMsg(struct message_state* state)
{
    bool monitoringMessages = true;
    std::string monitoringMessagesStr = theServerConfig().get<std::string > ("MonitoringMessaging");
    if(monitoringMessagesStr == "false")
        monitoringMessages = false;

    if(!monitoringMessages)
        return;

    std::ostringstream json_message;
    json_message << "SS {";
    json_message << "\"vo_name\":" << "\"" << state->vo_name << "\",";
    json_message << "\"source_se\":" << "\"" << state->source_se << "\",";
    json_message << "\"dest_se\":" << "\"" << state->dest_se << "\",";
    json_message << "\"job_id\":" << "\"" << state->job_id << "\",";
    json_message << "\"file_id\":" << "\"" << state->file_id << "\",";
    json_message << "\"job_state\":" << "\"" << state->job_state << "\",";
    json_message << "\"file_state\":" << "\"" << state->file_state << "\",";
    json_message << "\"retry_counter\":" << "\"" << state->retry_counter << "\",";
    json_message << "\"retry_max\":" << "\"" << state->retry_max << "\",";
    if(state->job_metadata.length() > 0 )
        json_message << "\"job_metadata\":" << state->job_metadata << ",";
    else
        json_message << "\"job_metadata\":\"\",";
    if(state->file_metadata.length() > 0 )
        json_message << "\"file_metadata\":" << state->file_metadata << ",";
    else
        json_message << "\"file_metadata\":\"\",";
    json_message << "\"timestamp\":" << "\"" << getStrUTCTimestamp() << "\"";
    json_message << "}";

    struct message_monitoring message;

    if(json_message.str().length() < 3000)
        {
            strncpy(message.msg, std::string(json_message.str()).c_str(), sizeof(message.msg));
            message.msg[sizeof(message.msg) - 1] = '\0';
            message.timestamp = milliseconds_since_epoch();
            runProducerMonitoring( message );
        }
}


}

