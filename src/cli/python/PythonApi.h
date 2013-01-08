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
 * GSoapContextAdapter.h
 *
 * PythonCliWrapper.h
 *
 *  Created on: Sep 11, 2012
 *      Author: Michał Simon
 */

#ifndef PYTHONCLIWRAPPER_H_
#define PYTHONCLIWRAPPER_H_

#include "GSoapContextAdapter.h"
#include "python/Job.h"

#include <boost/python.hpp>



namespace fts3 { namespace cli {

namespace py = boost::python;

class PythonApi {

public:
	PythonApi(py::str endpoint);
	virtual ~PythonApi();

	py::str submit(Job job); // deleg only
	void cancel(py::list ids);
	py::str getStatus(py::str id);

private:
	GSoapContextAdapter ctx;
};

}
}

#endif /* PYTHONCLIWRAPPER_H_ */
