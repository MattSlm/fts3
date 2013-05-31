/*	Copyright notice:
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
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 *
 * BlacklistCli.h
 *
 *  Created on: Nov 7, 2012
 *      Author: simonm
 */

#ifndef BLACKLISTCLI_H_
#define BLACKLISTCLI_H_

#include "CliBase.h"

#include <string>

namespace fts3
{
namespace cli
{

using namespace std;

class BlacklistCli : public CliBase
{

public:

    static const string ON;
    static const string OFF;

    static const string SE;
    static const string DN;

    /**
     * Default Constructor.
     *
     * Creates the debud-set specific command line option: on/off.
     */
    BlacklistCli();

    /**
     * Destructor.
     */
    virtual ~BlacklistCli();

    /**
     * Validates command line options.
     *	Checks that the debug mode was set correctly.
     *
     * @return GSoapContexAdapter instance, or null if all activities
     * 				requested using program options have been done.
     */
    virtual optional<GSoapContextAdapter&> validate(bool init = true);

    /**
     * Prints help message if the -h option has been used.
     *
     * @param tool - the name of the executive that has been called (in most cases argv[0])
     *
     * @return true if the help message has been printed
     */
    virtual bool printHelp(string tool);

    /**
     * Gives the instruction how to use the command line tool.
     *
     * @return a string with instruction on how to use the tool
     */
    string getUsageString(string tool);

    /**
     * Gets the debug mode.
     *
     * @return true is the debug mode is on, false if the debud mode is off
     */
    bool getBlkMode()
    {
        return mode == ON;
    }

    string getSubjectName()
    {
        return subject;
    }

    string getSubjectType()
    {
        return type;
    }

    string getVo()
    {
        return vo;
    }

    string getStatus()
    {
        return status;
    }

    int getTimeout()
    {
        return timeout;
    }

private:

    /// blacklist mode, either ON or OFF
    string mode;

    /// the DN or SE that is the subject of blacklisting
    string subject;

    /// type of the subject, either SE or DN
    string type;

    string vo;

    string status;

    int timeout;
};

} /* namespace cli */
} /* namespace fts3 */
#endif /* BLACKLISTCLI_H_ */
