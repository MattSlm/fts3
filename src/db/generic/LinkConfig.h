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
 *
 * LinkConfig.h
 *
 *  Created on: Nov 21, 2012
 *      Author: simonm
 */

#ifndef LINKCONFIG_H_
#define LINKCONFIG_H_

#include <string>


class LinkConfig {
public:
	LinkConfig(){};
	~LinkConfig(){};

	std::string source;
	std::string destination;
	std::string state;
	std::string symbolic_name;

    int NOSTREAMS;
    int TCP_BUFFER_SIZE;
    int URLCOPY_TX_TO;
    int NO_TX_ACTIVITY_TO;

    std::string auto_protocol;
};

#endif /* LINKCONFIG_H_ */
