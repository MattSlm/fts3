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
 * SrcDestCli.cpp
 *
 *  Created on: Aug 2, 2012
 *      Author: Michał Simon
 */


#include "SrcDestCli.h"

using namespace fts3::cli;

SrcDestCli::SrcDestCli() {

	// add commandline options specific for fts3-transfer-submit
	specific.add_options()
			("source", value<string>(), "Specify source site name.")
			("destination", value<string>(), "Specify destination site name.")
			;

	// add positional (those used without an option switch) command line options
	p.add("source", 1);
	p.add("destination", 1);
}

SrcDestCli::~SrcDestCli() {

}

string SrcDestCli::getSource() {

	// check if source was passed via command line options
	if (vm.count("source")) {
		return vm["source"].as<string>();
	}
	return "";
}

string SrcDestCli::getDestination() {

	// check if destination was passed via command line options
	if (vm.count("destination")) {
		return vm["destination"].as<string>();
	}
	return "";
}
