/*
 *	Copyright notice:
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
 * CliBase.h
 *
 *  Created on: Feb 2, 2012
 *      Author: Michal Simon
 */

#ifndef CLIBASE_H_
#define CLIBASE_H_

#include <boost/program_options.hpp>
#include <fstream>

using namespace boost::program_options;
using namespace std;


namespace fts3 { namespace cli {

/**
 * CliBase is a base class for developing FTS3 command line tools.
 *
 * The class provides following functionalities:
 *  	- help (-h)
 *  	- quite (-q) and verbose (-v)
 *  	- version (-V)
 *  	- service (-s)
 *
 *  The CliBase is an abstract class, each class that inherits after CliBase
 *  has to implement 'getUsageString()', which returns an instruction on how
 *  to use the given command line tool.
 */
class CliBase {
public:

	/**
	 * Default constructor.
	 *
	 * Initializes service discovery parameters. Moreover creates the basic
	 * command line options, and marks them as visible.
	 */
	CliBase();

	/**
	 * Destructor
	 */
	virtual ~CliBase();

	/**
	 * Initializes the object with command line options.
	 *
	 * Parses the command line options that were passed to the command line tool.
	 * In addition check whether the format of the FTS3 service is correct. If
	 * The FTS3 service is not specified the object tries to discover it.
	 *
	 * @param ac - argument count
	 * @param av - argument array
	 */
	virtual void initCli(int ac, char* av[]);

	/**
	 * Prints help message if the -h option has been used.
	 *
	 * @param tool - the name of the executive that has been called (in most cases argv[0])
	 *
	 * @return true if the help message has been printed
	 */
	bool printHelp(string tool);

	/**
	 * Prints version if the -V option has been used.
	 *
	 * @return true if the version has been printed
	 */
	bool printVersion();

	/**
	 * TODO should be moved to SrvManager !!!
	 * Prints general information about the FTS3 service.
	 * Should be used if the -v option has been used.
	 */
	void printGeneralInfo();

	/**
	 * Checks whether the -v option was used.
	 *
	 * @return true if -v option has been used
	 */
	bool isVerbose();

	/**
	 * Checks whether the -q option was used.
	 *
	 * @return true if -q option has been used
	 */
	bool isQuite();

	/**
	 * Gets the FTS3 service string
	 *
	 * @return FTS3 service string
	 */
	string getService();

	/**
	 * Pure virtual method, it aim is to give the instruction how to use the command line tool.
	 *
	 * @param tool - name of the fts3 tool that is using this utility (e.g. 'fts3-transfer-submit')
	 *
	 * @return implementing class should return a string with instruction on how to use the tool
	 */
	virtual string getUsageString(string tool);

protected:

	/**
	 * Discovers the FTS3 service (if the -s option has not been used).
	 *
	 * Uses ServiceDiscoveryIfce to find a FTS3 service.
	 *
	 * @return FTS3 service string
	 */
	string discoverService();

	/**
	 * Mutes the cout stream
	 * 	(used in unit tests)
	 */
	void mute();

	/**
	 * Unmutes the cout stream
	 * 	(used in unit tests)
	 */
	void unmute();

	/**
	 * a map containing parsed options
	 */
	variables_map vm;

	/**
	 * basic command line options provided by CliBase
	 */
	options_description basic;

	/**
	 * command line options that are printed if -h option has been used
	 */
	options_description visible;

	/**
	 * all command line options, inheriting class should add its options to 'cli_option'
	 */
	options_description all;

	/**
	 * command line parameters that are passed without any switch option e.g. -p
	 */
	positional_options_description p;

	/**
	 * command line options specific for fts3-transfer-status
	 */
	options_description specific;

	/**
	 * hidden command line options (not printed in help)
	 */
	options_description hidden;

	/**
	 * the endpoint FTS3 service
	 */
	string endpoint;

private:

	///@{
	/**
	 * string values used for discovering the FTS3 service
	 */
	string FTS3_CA_SD_TYPE;
	string FTS3_SD_ENV;
	string FTS3_SD_TYPE;
	string FTS3_IFC_VERSION;
	string FTS3_INTERFACE_VERSION;
	///@}

	/**
	 * standard output stream (cout)
	 */
	streambuf* cout_sbuf;

	/**
	 * replacement output stream
	 * 	(devours things send to cout)
	 */
	ofstream fout;
};

}
}

#endif /* CLIBASE_H_ */
