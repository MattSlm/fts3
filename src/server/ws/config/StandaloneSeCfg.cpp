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
 * StandaloneSeCfg.cpp
 *
 *  Created on: Nov 19, 2012
 *      Author: Michal Simon
 */

#include "StandaloneSeCfg.h"

#include <sstream>

#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>

namespace fts3
{
namespace ws
{

using namespace boost;

StandaloneSeCfg::StandaloneSeCfg(string dn, string name) : StandaloneCfg(dn), se(name)
{

    if (notAllowed.count(se))
        throw Err_Custom("The SE name is not a valid!");

    // replace any with wildcard
    if (se == any) se = wildcard;

    // get SE active state
    Se* seobj = 0;
    db->getSe(seobj, se);
    if (seobj)
        {
            active = seobj->STATE == on;
            delete seobj;
        }
    else
        throw Err_Custom("The SE: " + name + " does not exist!");

    init(se);
}

StandaloneSeCfg::StandaloneSeCfg(string dn, CfgParser& parser) : StandaloneCfg(dn, parser)
{

    se = parser.get<string>("se");
    all = json();

    if (notAllowed.count(se))
        throw Err_Custom("The SE name is not a valid!");

    // replace any with wildcard
    if (se == any) se = wildcard;
}

StandaloneSeCfg::~StandaloneSeCfg()
{

}

string StandaloneSeCfg::json()
{

    stringstream ss;

    ss << "{";
    ss << "\"" << "se" << "\":\"" << (se == wildcard ? any : se) << "\",";
    ss << StandaloneCfg::json();
    ss << "}";

    return ss.str();
}

void StandaloneSeCfg::save()
{
    addSe(se, active);
    StandaloneCfg::save(se);
}

void StandaloneSeCfg::del()
{
    eraseSe(se);
    StandaloneCfg::del(se);
}

} /* namespace common */
} /* namespace fts3 */
