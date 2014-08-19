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
 * fts3-transfer-list.cpp
 *
 *  Created on: Mar 1, 2012
 *      Author: Michal Simon
 */


#include "ServiceAdapter.h"
#include "GSoapContextAdapter.h"
#include "RestContextAdapter.h"

#include "ui/ListTransferCli.h"
#include "rest/HttpRequest.h"
#include "rest/ResponseParser.h"

#include "common/JobStatusHandler.h"

#include "exception/cli_exception.h"
#include "JsonOutput.h"

#include <algorithm>
#include <sstream>
#include <memory>

using namespace fts3::cli;
using namespace fts3::common;


/**
 * This is the entry point for the fts3-transfer-list command line tool.
 */
int main(int ac, char* av[])
{
    try
        {
            ListTransferCli cli;
            // Initialise the command line utility
            cli.parse(ac, av);
            if (!cli.validate()) return 1;

            std::string const endpoint = cli.getService();

            std::unique_ptr<ServiceAdapter> ctx;
            if (cli.rest())
                {
                    std::string const capath = cli.capath();
                    std::string const proxy = cli.proxy();
                    ctx.reset(new RestContextAdapter(endpoint, capath, proxy));
                }
            else
                {
                    ctx.reset(new GSoapContextAdapter(endpoint));
                }

            // print API details if verbose
            cli.printApiDetails(*ctx.get());

            std::vector<std::string> array = cli.getStatusArray();
            std::vector<fts3::cli::JobStatus> statuses =
                ctx->listRequests(array, cli.getUserDn(), cli.getVoName(), cli.source(), cli.destination());

            MsgPrinter::instance().print(statuses);
        }
    catch(cli_exception const & ex)
        {
            MsgPrinter::instance().print(ex);
            return 1;
        }
    catch(std::exception& ex)
        {
            MsgPrinter::instance().print(ex);
            return 1;
        }
    catch(...)
        {
            MsgPrinter::instance().print("error", "exception of unknown type!");
            return 1;
        }

    return 0;
}
