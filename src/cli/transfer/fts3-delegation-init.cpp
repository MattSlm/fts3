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

#include "ui/DelegationCli.h"

#include <exception>

#include <boost/scoped_ptr.hpp>

using namespace std;
using namespace boost;
using namespace fts3::cli;

/**
 * This is the entry point for the fts3-transfer-submit command line tool.
 */
int main(int ac, char* av[])
{

    scoped_ptr<DelegationCli> cli;

    try
        {
            // create and initialize the command line utility
            cli.reset (
                getCli<DelegationCli>(ac, av)
            );
            if (!cli->validate()) return 0;

            // validate command line options, and return respective gSOAP context
            cli->getGSoapContext();

            // delegate Proxy Certificate
            ProxyCertificateDelegator handler (
                cli->getService(),
                cli->getDelegationId(),
                cli->getExpirationTime(),
                cli->printer()
            );

            handler.delegate();

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
