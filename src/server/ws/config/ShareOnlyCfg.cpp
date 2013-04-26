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
 * ShareOnlyCfg.cpp
 *
 *  Created on: Apr 26, 2013
 *      Author: Michal Simon
 */

#include "ShareOnlyCfg.h"

namespace fts3 {
namespace ws {

ShareOnlyCfg::ShareOnlyCfg(string dn, string name) : Configuration(dn), se(name) {

	if (notAllowed.count(se))
		throw Err_Custom("The SE name is not a valid!");

	// replace any with wildcard
	if (se == any) se = wildcard;

	// get SE active state
	Se* seobj = 0;
	db->getSe(seobj, se);
	if (seobj) {
		active = seobj->STATE == on;
		delete seobj;
	} else
		throw Err_Custom("The SE: " + name + " does not exist!");

	init(se);
}

ShareOnlyCfg::ShareOnlyCfg(string dn, CfgParser& parser) : Configuration(dn) {

	se = parser.get<string>("se");
	all = json();

	if (notAllowed.count(se))
		throw Err_Custom("The SE name is not a valid!");

	// replace any with wildcard
	if (se == any) se = wildcard;

	active = parser.get<bool>("active");

	in_share = parser.get< map<string, int> >("in.share");
	out_share = parser.get< map<string, int> >("out.share");
}

ShareOnlyCfg::~ShareOnlyCfg() {

}

string ShareOnlyCfg::json() {

	stringstream ss;

	ss << "{";
	ss << "\"" << "se" << "\":\"" << (se == wildcard ? any : se) << "\",";
	ss << "\"" << "active" << "\":" << (active ? "true" : "false") << ",";
	ss << "\"" << "in" << "\":" << Configuration::json(in_share) << ",";
	ss << "\"" << "out" << "\":" << Configuration::json(out_share) << ",";
	ss << "}";

	return ss.str();
}

void ShareOnlyCfg::save() {
	addSe(se, active);
	// TODO
}

void ShareOnlyCfg::del() {
	// TODO
}

void ShareOnlyCfg::init(string se) {
	// TODO
}

} /* namespace ws */
} /* namespace fts3 */
