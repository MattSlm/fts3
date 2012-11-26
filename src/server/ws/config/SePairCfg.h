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
 * SePairCfg.h
 *
 *  Created on: Nov 19, 2012
 *      Author: Michal Simon
 */

#ifndef SEPAIRCFG_H_
#define SEPAIRCFG_H_

#include "PairCfg.h"

namespace fts3 {
namespace ws {

/**
 * SE pair configuration,
 * 	it's derived from PairCfg
 *
 * 	@see PairCfg
 */
class SePairCfg : public PairCfg {

public:
	/**
	 * Constructor.
	 *
	 * Retrieves the configuration data from the DB from the given source-destination pair.
	 *
	 * @param dn - client's DN
	 * @param source - the source SE
	 * @param destination - the destination SE
	 */
	SePairCfg(string dn, string source, string destination) : PairCfg(dn, source, destination) {};

	/**
	 * Constructor.
	 *
	 * Retrieves the configuration data from the given CfgParser.
	 *
	 * @param dn - client's DN
	 * @param parser - the parser used for parsing JSON configuration
	 */
	SePairCfg(string dn, CfgParser& parser);

	/**
	 * Destructor.
	 */
	virtual ~SePairCfg();

	/**
	 * Creates a string containing current configuration in JSON format.
	 *
	 * @return string containing the configuration.
	 */
	virtual string json();

	/**
	 * Saves the configuration into the DB.
	 */
	virtual void save();
};

} /* namespace ws */
} /* namespace fts3 */
#endif /* SEPAIRCFG_H_ */
