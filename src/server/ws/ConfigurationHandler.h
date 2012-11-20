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
 * ConfigurationHandler.h
 *
 *  Created on: Jun 26, 2012
 *      Author: Michał Simon
 */

#ifndef CONFIGURATIONHANDLER_H_
#define CONFIGURATIONHANDLER_H_

#include <string>
#include <vector>
#include <map>
#include <set>

#include <boost/assign.hpp>
#include <boost/regex.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/tuple/tuple.hpp>

#include "common/error.h"
#include "common/logger.h"
#include "common/CfgParser.h"

#include "db/generic/SingleDbInstance.h"

#include "Configuration.h";

namespace fts3 { namespace ws {

using namespace boost;
using namespace boost::assign;
using namespace std;
using namespace fts3::common;
using namespace db;


/**
 * The ConfigurationHandler class is used for handling
 * Configuration WebServices' requests
 */
class ConfigurationHandler {

	/// TODO move to common/json, json() method that will return the configuration for the given object
	struct StandaloneSeCfg1 {

		string se;
		bool active;

		map<string, int> in_share;
		map<string, int> in_protocol;

		map<string, int> out_share;
		map<string, int> out_protocol;
	};

	/// TODO move to common/json, json() method that will return the configuration for the given object
	struct StandaloneGrCfg1 {

		string group;
		bool active;

		vector<string> members;

		map<string, int> in_share;
		map<string, int> in_protocol;

		map<string, int> out_share;
		map<string, int> out_protocol;
	};

	/// TODO move to common/json, json() method that will return the configuration for the given object
	struct PairCfg1 {

		string symbolic_name;
		bool active;

		string source;
		string destination;

		map<string, int> share;
		map<string, int> protocol;
	};

public:

	/**
	 * Constructor
	 *
	 * initializes the regular expression objects and the 'parameterNameToId' map
	 */
	ConfigurationHandler(string dn);

	/**
	 * Destructor
	 */
	virtual ~ConfigurationHandler();

	/**
	 * Parses the configuration string (JSON format)
	 * 	Throws an Err_Custom if the given configuration is in wrong format.
	 *
	 * It has to be called before 'add'!
	 *
	 * 	@param configuration - string containing the configuration in JSON format
	 */
	void parse(string configuration);

	/**
	 * Adds a configuration to the DB.
	 * 	First the 'parse' method has to be called
	 *
	 * @see parse
	 */
	void add();

	/**
	 * Gets the whole configuration regarding all SEs and all SE groups from the DB.
	 * 	The configuration can also be restricted to a given SE and/or VO.
	 *
	 * @param vo - VO name
	 * @param se - SE name
	 *
	 * @return vector containing single configuration entries in JSON format
	 */

	vector<string> getStandalone(string name);

	/**
	 *
	 */
	vector<string> getSymbolic(string cfg_name);

	/**
	 *
	 */
	vector<string> getPair(string src, string dest);

	/**
	 * Deletes the configuration specified by the argument
	 * 	Only share and protocol specific configurations will be deleted.
	 *
	 * @param cfg - the configuration that should be deleted
	 */
	void del ();

	/**
	 * Gets protocol configuration for the SE or SE group,
	 * 	First the 'parse' method has to be called
	 *
	 * @return a shared_ptr pointing on a SeProtocolConfig object
	 *
	 * @see parse
	 */
	shared_ptr<SeProtocolConfig> getProtocolConfig(map<string, int> protocol);

	/**
	 * Gets the SE / SE group name
	 *
	 * @return se name
	 */
	string getName() {
		return "dummy"; // TODO it's used for authorization! should be the se name (?)
	}

private:

	vector<string> getGroupCfg(string cfg_name, string name, string vo);

	vector<string> doGet(SeConfig* cfg);

	/**
	 * Converts boolean to string:
	 * 	true 	-> 'true'
	 * 	false 	-> 'false'
	 *
	 * 	@param b - boolean value
	 *
	 * 	@return if the parameter was true - 'true', otherwise 'false'
	 */
	string str(bool b);

	/// Pointer to the 'GenericDbIfce' singleton
	GenericDbIfce* db;

	/// the whole cfg comand
	string all;

	/// a standalone se cfg
	StandaloneSeCfg1 seCfg;
	///
	StandaloneGrCfg1 grCfg;
	///
	PairCfg1 pairCfg;

	/// type of the configuration that is being submitted
	CfgParser::CfgType type;

	/// number of available protocol parameters
	static const int PARAMETERS_NMB = 16;

	/// user DN
	string dn;

	/// number of SQL updates triggered by configuration command
	int updateCount;
	/// number of SQL inserts triggered by configuration command
	int insertCount;
	/// number of SQL deletes triggered by configuration command
	int deleteCount;
	/// number of debug cmd triggered by configuration command
	int debugCount;

	Configuration* cfg;
};

}
}

#endif /* TEST_H_ */
