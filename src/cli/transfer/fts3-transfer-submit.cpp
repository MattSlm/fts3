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

#include "GSoapContextAdapter.h"
#include "ProxyCertificateDelegator.h"
#include "ui/SubmitTransferCli.h"
#include "rest/HttpRequest.h"
#include "common/JobStatusHandler.h"

#include "TransferTypes.h"

#include <exception>

#include <boost/scoped_ptr.hpp>

using namespace std;
using namespace boost;
using namespace fts3::cli;
using namespace fts3::common;

/**
 * This is the entry point for the fts3-transfer-submit command line tool.
 */
int main(int ac, char* av[])
{

    scoped_ptr<SubmitTransferCli> cli;

    try
        {
            // create and initialize the command line utility
            cli.reset (
                getCli<SubmitTransferCli>(ac, av)
            );
            if (!cli->validate()) return 0;

            if (cli->rest())
                {
                    string url = cli->getService() + "/jobs";
                    string job = cli->getFileName();

                    HttpRequest http (url, cli->capath(), cli->proxy(), cout);
                    http.put(job);
                    return 0;
                }

            // validate command line options, and return respective gSOAP context
            GSoapContextAdapter& ctx = cli->getGSoapContext();

            string jobId("");

//		if (cli->useDelegation()) {

            vector<File> files = cli->getFiles();
            if (files.empty())
                {
                    throw string ("No transfer job has been specified.");
                }

            map<string, string> params = cli->getParams();

            // delegate Proxy Certificate
            ProxyCertificateDelegator handler (
                cli->getService(),
                cli->getDelegationId(),
                cli->getExpirationTime(),
                cli->printer()
            );

            handler.delegate();

            // submit the job
            jobId = ctx.transferSubmit (
                        files,
                        params/*,
					cli->useCheckSum()*/
                    );
//
//		} else {
//			// submit the job
//			jobId = ctx.transferSubmit (
//					cli->getJobElements(),
//					cli->getParams(),
//					cli->getPassword()
//				);
//		}

            cli->printer().job_id(jobId);

            // check if the -b option has been used
            if (cli->isBlocking())
                {

                    fts3::cli::JobStatus status;
                    // wait until the transfer is ready
                    do
                        {
                            sleep(2);
                            status = ctx.getTransferJobStatus(jobId, false);
                        }
                    while (!JobStatusHandler::getInstance().isTransferFinished(status.jobStatus));
                }

        }
    catch(std::exception& ex)
        {
            if (cli.get())
                cli->printer().error_msg(ex.what());
            else
                std::cerr << ex.what() << std::endl;
            return 1;
        }
    catch(string& ex)
        {
            if (cli.get())
                cli->printer().gsoap_error_msg(ex);
            else
                std::cerr << ex << std::endl;
            return 1;
        }
    catch(...)
        {
            if (cli.get())
                cli->printer().error_msg("Exception of unknown type!");
            else
                std::cerr << "Exception of unknown type!" << std::endl;
            return 1;
        }

    return 0;
}
