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
 */

/*
 * TransferStatusCli.h
 *
 *  Created on: Feb 13, 2012
 *      Author: simonm
 */

#ifndef TRANSFERSTATUSCLI_H_
#define TRANSFERSTATUSCLI_H_

#include "CliBase.h"
#include <vector>
#include <string>


namespace fts { namespace cli {

class TransferStatusCli: public CliBase {
public:
	TransferStatusCli();
	virtual ~TransferStatusCli();

	string getUsageString();
	vector<string> getJobIds();

	bool list();

private:
	options_description specific;
	options_description hidden;
};

}
}

#endif /* TRANSFERSTATUSCLI_H_ */
