/*
 *	Copyright notice:
 *	Copyright � Members of the EMI Collaboration, 2010.
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
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 *
 * BdiiBrowser.h
 *
 *  Created on: Oct 25, 2012
 *      Author: Michał SimonS
 */

#ifndef BDIIBROWSER_H_
#define BDIIBROWSER_H_

#include "common/logger.h"
#include "common/ThreadSafeInstanceHolder.h"
#include <sys/types.h>
#include <signal.h>
#include "config/serverconfig.h"

#include <ldap.h>

#include <sys/types.h>

#include <string>
#include <list>
#include <map>


#include <boost/thread.hpp>
#include <boost/thread/locks.hpp>

namespace fts3
{
namespace infosys
{

using namespace std;
using namespace boost;
using namespace common;
using namespace config;

/**
 * BdiiBrowser class is responsible for browsing BDII.
 *
 * It has a singleton access, the BDII should be queried using 'browse' method.
 *
 * @see ThreadSafeInstanceHolder
 */
class BdiiBrowser: public ThreadSafeInstanceHolder<BdiiBrowser>
{

    friend class ThreadSafeInstanceHolder<BdiiBrowser>;

public:

    /// object class
    static const char* ATTR_OC;

    /// glue1 base
    static const string GLUE1;
    /// glue2 base
    static const string GLUE2;

    /// the object class for glue2
    static const char* CLASS_SERVICE_GLUE2;
    /// the object class for glue1
    static const char* CLASS_SERVICE_GLUE1;

    /// destructor
    virtual ~BdiiBrowser();

    /**
     * Checks if given VO is allowed for the given SE
     *
     * @param se - SE name
     * @param vo - VO name
     *
     * @return true if the VO is allowed, flase otherwise
     */
    bool isVoAllowed(string se, string vo);

    /**
     * Checks if the SE status allows for submitting a transfer
     *
     * @param se - SE name
     *
     * @return true if yes, otherwise false
     */
    bool getSeStatus(string se);

    // if we want all available attributes we leave attr = 0
    /**
     * Allows to browse the BDII
     *
     * Each time a browse action is taken the fts3config file is check for the right BDII endpoint
     *
     * @param base - either GLUE1 or GLUE2
     * @param query - the filter you want to apply
     * @param attr - the attributes you are querying for (by default 0, meaning you are interested in all attributes)
     *
     * @return the result set containing all the outcomes
     */
    template<typename R>
    list< map<string, R> > browse(string base, string query, const char **attr = 0);

    /**
     * Looks for attribute names in a foreign key
     *
     * @param values the values returned for the given foreign key
     * @param attr - the attributes you are querying for
     */
    static string parseForeingKey(list<string> values, const char *attr);

    /**
     * Checks in the fts3config if the given base (glue1 or glue2) is currently in use
     *
     * @param base - the base (glue1 or glue2) that will be checked
     *
     * @return true if the base is in use, false otherwise
     */
    bool checkIfInUse(const string& base);

private:

    /**
     * Connects to the BDII
     *
     * @param infosys - the BDII endpoint
     * @param sec - connection timeout in secounds (by default 15)
     *
     * @return true is connection was successful, otherwise false
     */
    bool connect(string infosys, time_t sec = 15);

    /**
     * Reconnect in case that the current connection is not valid any more
     *
     * @return true is it has been possible to reconnect successfully, false otherwise
     */
    bool reconnect();

    /**
     * Closes the current connection to the BDII
     */
    void disconnect();

    /**
     * Checks if the current connection is valid
     *
     * @return true if the curret connection is valid, false otherwise
     */
    bool isValid();

    /**
     * Converts the base string from BDII to ordinary string
     *
     * @param base - BDII base
     *
     * @return respective string value
     */
    string baseToStr(const string& base);

    /**
     * Parses BDII reply
     *
     * @param reply - the reply from BDII query
     *
     * @return result set containing the data retrived from BDII
     */
    template<typename R>
    list< map<string, R> > parseBdiiResponse(LDAPMessage *reply);

    /**
     * Parses single entry retrieved from BDII query
     *
     * @param entry - single entry returned from BDII query
     *
     * @return map that holds entry names and respective values
     */
    template<typename R>
    map<string, R> parseBdiiSingleEntry(LDAPMessage *entry);

    /**
     * Parses BDII attribute
     *
     * @param value - attribute value
     *
     * @return parsed value
     */
    template<typename R>
    R parseBdiiEntryAttribute(berval **value);


    /// LDAP context
    LDAP *ld;

    /// connection timeout
    timeval timeout;

    /// search timeout
    timeval search_timeout;

    /// full LDAP URL (including protocol)
    string url;

    /// the BDII endpoint
    string infosys;

    /**
     *  semaphore
     *  positive value gives the number of threads browsing BDII at the moment
     *  negative value gives the number of threads trying to reconnect
     */
    int querying;

    /// the mutex preventing concurrent browsing and reconnecting
    shared_mutex qm;

    /// conditional variable preventing concurrent browsing and reconnecting
//    condition_variable qv;

    /**
     * Blocks until all threads finish browsing
     */
//    void waitIfBrowsing();

    /**
     * Notify all threads that want to do browsing after reconnection happened
     */
//    void notifyBrowsers();

    /**
     * Blocks until the reconnection is finished
     */
//    void waitIfReconnecting();

    /**
     * Notify all threads that want to perform reconnection after all the browsing is finished
     */
//    void notifyReconnector();

    /// not used for now
    static const char* ATTR_STATUS;
    /// not used for now
    static const char* ATTR_SE;

    /// not used for now
    static const string FIND_SE_STATUS(string se);
    /// not used for now
    static const char* FIND_SE_STATUS_ATTR[];

    /// a string 'false'
    static const string false_str;
    /// flag - true if connected, false if not
    bool connected;

    /// Constructor
    BdiiBrowser() : ld(NULL), querying(0), connected(false)
    {
        memset(&timeout, 0, sizeof(timeout));
        memset(&search_timeout, 0, sizeof(search_timeout));
    };
    /// not implemented
    BdiiBrowser(BdiiBrowser const&);
    /// not implemented
    BdiiBrowser& operator=(BdiiBrowser const&);

    /// maximum number of reconnection tries
    static const int max_reconnect = 3;

    ///@{
    /// keep alive LDAP parameters
    static const int keepalive_idle = 120;
    static const int keepalive_probes = 3;
    static const int keepalive_interval = 60;
    ///@}
};

template<typename R>
list< map<string, R> > BdiiBrowser::browse(string base, string query, const char **attr)
{
    signal(SIGPIPE, SIG_IGN);
    // check in the fts3config file if the 'base' (glue1 or glue2) is in use, if no return an empty result set
    if (!checkIfInUse(base)) return list< map<string, R> >();

    // check in the fts3config if the host name for BDII was specified, if no return an empty result set
    if (!theServerConfig().get<bool>("Infosys")) return list< map<string, R> >();

    // check if the connection is valid
    if (!isValid())
        {

            bool reconnected = false;
            int reconnect_count = 0;

            // try to reconnect 3 times
            for (reconnect_count = 0; reconnect_count < max_reconnect; reconnect_count++)
                {
                    reconnected = reconnect();
                    if (reconnected) break;
                }

            // if it has not been possible to reconnect return an empty result set
            if (!reconnected)
                {
                    FTS3_COMMON_LOGGER_NEWLOG (ERR) << "LDAP error: it has not been possible to reconnect to the BDII" << commit;
                    return list< map<string, R> >();
                }
        }

    int rc = 0;
    LDAPMessage *reply = 0;

    // used shared lock - many concurrent reads are allowed
    {
        shared_lock<shared_mutex> lock(qm);
        rc = ldap_search_ext_s(ld, base.c_str(), LDAP_SCOPE_SUBTREE, query.c_str(), const_cast<char**>(attr), 0, 0, 0, &timeout, 0, &reply);
    }

    if (rc != LDAP_SUCCESS)
        {
            if (reply && rc > 0) ldap_msgfree(reply);
            FTS3_COMMON_LOGGER_NEWLOG (ERR) << "LDAP error: " << ldap_err2string(rc) << commit;
            return list< map<string, R> > ();
        }

    list< map<string, R> > ret = parseBdiiResponse<R>(reply);
    if (reply) ldap_msgfree(reply);

    return ret;
}

template<typename R>
list< map<string, R> > BdiiBrowser::parseBdiiResponse(LDAPMessage *reply)
{

    list< map<string, R> > ret;
    for (LDAPMessage *entry = ldap_first_entry(ld, reply); entry != 0; entry = ldap_next_entry(ld, entry))
        {

            ret.push_back(
                parseBdiiSingleEntry<R>(entry)
            );
        }

    return ret;
}

template<typename R>
map<string, R> BdiiBrowser::parseBdiiSingleEntry(LDAPMessage *entry)
{

    BerElement *berptr = 0;
    char* attr = 0;
    map<string, R> m_entry;

    for (attr = ldap_first_attribute(ld, entry, &berptr); attr != 0; attr = ldap_next_attribute(ld, entry, berptr))
        {

            berval **value = ldap_get_values_len(ld, entry, attr);
            R val = parseBdiiEntryAttribute<R>(value);
            ldap_value_free_len(value);

            if (!val.empty())
                {
                    m_entry[attr] = val;
                }
            ldap_memfree(attr);
        }

    if (berptr) ber_free(berptr, 0);

    return m_entry;
}

template<>
inline string BdiiBrowser::parseBdiiEntryAttribute<string>(berval **value)
{

    if (value && value[0] && value[0]->bv_val) return value[0]->bv_val;
    return string();
}

template<>
inline list<string> BdiiBrowser::parseBdiiEntryAttribute< list<string> >(berval **value)
{

    list<string> ret;
    for (int i = 0; value && value[i] && value[i]->bv_val; i++)
        {
            ret.push_back(value[i]->bv_val);
        }
    return ret;
}

} /* namespace fts3 */
} /* namespace infosys */
#endif /* BDIIBROWSER_H_ */
