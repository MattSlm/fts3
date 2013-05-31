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
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implcfgied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 *
 * AuthorizationManager.h
 *
 *  Created on: Jul 18, 2012
 *      Author: simonm
 */

#ifndef AUTHORIZATIONMANAGER_H_
#define AUTHORIZATIONMANAGER_H_

#include "common/ThreadSafeInstanceHolder.h"
#include "ws-ifce/gsoap/gsoap_stubs.h"

#include <set>
#include <vector>
#include <utility>

#include <boost/tokenizer.hpp>

namespace fts3
{
namespace ws
{

using namespace fts3::common;
using namespace std;
using namespace boost;

/**
 * AuthorizationManager facilitates the authorization of a fts operation
 *
 * AuthorizationManager implements the Singleton design pattern (ThreadSafeInstanceHolder).
 *
 * @see ThreadSafeInstanceHolder
 */
class AuthorizationManager : public ThreadSafeInstanceHolder<AuthorizationManager>
{

    /**
     * friend to the singleton holder
     */
    friend class ThreadSafeInstanceHolder<AuthorizationManager>;

public:

    /**
     * Authorization level
     */
    enum Level
    {
        NONE, //< access has not been granted
        PRV,  //< access granted only for private resources
        VO,   //< access granted only for VO's resources
        ALL   //< access granted
    };

    /**
     * The operation types
     */
    enum Operation
    {
        DELEG, 	  //< delegation
        TRANSFER, //< transfer
        CONFIG 	  //< configuration
    };

    /// it is used as resource ID for all authorization operations for which the rsc_id doesn't matter
    /// but the required level should be the default for operation (not NONE)
    static const string dummy;

    /**
     * Destructor
     */
    virtual ~AuthorizationManager();


    /**
     * Authorizes the given operation on a given resource
     *
     * @param ctx - the gSOAP context
     * @param op - the operation that is being authorize
     * @param rsc_id - the ID of the resource that is being the subject of the operation
     *
     * @return authorization level at which access has been granted
     */
    Level authorize(soap* ctx, Operation op, string rsc_id = string());

private:

    /// authorization level 'all' corresponding to global access
    static const string ALL_LVL;
    /// authorization level 'vo' corresponding to access at VO level
    static const string VO_LVL;
    /// authorization level '' corresponding to private access (only resources the user is directly responsible of)
    static const string PRV_LVL;

    /// public access string in fts3config file
    static const string PUBLIC_ACCESS;

    /// 'deleg' string corresponding to delegation operations
    static const string DELEG_OP;
    /// 'transfer' string corresponding to transfer operations
    static const string TRANSFER_OP;
    /// 'config' string corresponding to configuration operations
    static const string CONFIG_OP;
    /// operation wild-card ('*') - covers all above operations
    static const string WILD_CARD;

    /// the prefix corresponding to the 'roles' section
    static const string ROLES_SECTION_PREFIX;

    /**
     * Returns the access level that has been granted for the given operation
     * If access at NONE level has been granted an exception is thrown
     *
     * A root user of the server hosting the fts3 service if automatically
     * authorized at 'ALL' level, an exception is the transfer-submit operation!
     *
     * @param ctx - the gSOAP context
     * @param op - operation type
     *
     * @return the access level that has been granted
     */
    Level getGrantedLvl(soap* ctx, Operation op);

    /**
     * Returns the access level that is required for a given operation on a given resource
     *
     * @param ctx - the gSOAP context
     * @param op - operation type
     * @rsc_id - the resource being the subject of the operation
     *
     * @return the access level required to execute operation 'op' on resource 'rsc_id'
     * 			If the rsc_id is not specified 'NONE' is returned!
     */
    Level getRequiredLvl(soap* ctx, Operation op, string rsc_id = string());

    /**
     * Converts string to access level
     *
     * @param s - the string to be converted
     *
     * @return the level corresponding to the string
     */
    Level stringToLvl(string s);

    /**
     * Converts access level to string
     *
     * @param lvl - the access level to be converted
     *
     * @return string corresponding to the given access level
     */
    string lvlToString(Level lvl);

    /**
     * Converts operation type to string
     *
     * @param op - the operation type to be converted
     *
     * @return string corresponding to the given operation type
     */
    string operationToStr(Operation op);

    /**
     * Method for extracting values from roles/authorization entries in the fts3config file
     *
     * @param R - return type,
     * 		the configuration entry may be parsed to 'Level', 'string' or 'vector<string>'
     * @param cfg - config entry
     *
     * @return parsed config entry
     */
    template<typename R> R get(string cfg);

    /**
     * Checks the access level for a given role and operation
     *
     * @param role - client's role
     * @param operation - the operation that is being authorized
     *
     * @return access level configured in the fts3config file
     */
    Level check(string role, string operation);

    /**
     * Private constructor
     *
     * Initializes the object from fts3config file
     */
    AuthorizationManager();

    /**
     * Coping constructor, should not be used;
     */
    AuthorizationManager(const AuthorizationManager&);

    /**
     * Assignment operator, should not be used.
     */
    AuthorizationManager& operator=(const AuthorizationManager&);

    /// a set containing authorized VOs
    set<string> vos;
    /// a map mapping roles to operations, for each operation access level is defined
    map<string, map<string, Level> > access;

    /**
     * Method used to initialize authorized VOs set
     *
     * @return authorized VOs set
     */
    set<string> vostInit();

    /**
     * Method used to initialized access map
     *
     * @return access map
     */
    map<string, map<string, Level> > accessInit();

    time_t cfgReadTime;

};


}
}

#endif /* AUTHORIZATIONMANAGER_H_ */
