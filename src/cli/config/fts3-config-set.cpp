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
 * fts3-config-set.cpp
 *
 *  Created on: Apr 3, 2012
 *      Author: Michał Simon
 */


#include "GSoapContextAdapter.h"
#include "ui/SetCfgCli.h"
#include "common/error.h"

#include <string>
#include <vector>
#include <memory>

using namespace std;
using namespace fts3::cli;
using namespace fts3::common;

/**
 * This is the entry point for the fts3-config-set command line tool.
 */
int main(int ac, char* av[])
{

    try
        {
            // create and initialize the command line utility
            unique_ptr<SetCfgCli> cli (
                getCli<SetCfgCli>(ac, av)
            );
            if (!cli->validate()) return 0;

            // validate command line options, and return respective gsoap context
            GSoapContextAdapter& ctx = cli->getGSoapContext();

            optional<std::tuple<string, string, string>> protocol = cli->getProtocol();
            if (protocol.is_initialized())
                {
                    string udt = std::get<0>(*protocol);
                    string se = std::get<1>(*protocol);
                    string state = std::get<2>(*protocol);
                    ctx.setSeProtocol(udt, se, state);
                    return 0;
                }

            optional< pair<string, int> > maxActivePerSe = cli->getMaxSrcSeActive();
            if (maxActivePerSe.is_initialized())
            	{
            		ctx.setMaxSrcSeActive(maxActivePerSe.get().first, maxActivePerSe.get().second);
            	}

            maxActivePerSe = cli->getMaxDstSeActive();
            if (maxActivePerSe.is_initialized())
            	{
            		ctx.setMaxDstSeActive(maxActivePerSe.get().first, maxActivePerSe.get().second);
            	}

            optional<bool> drain = cli->drain();
            if (drain.is_initialized())
                {
                    ctx.doDrain(drain.get());
                }

            optional<int> retry = cli->retry();
            if (retry.is_initialized())
                {
                    ctx.retrySet(*retry);
                }

            optional<int> mode = cli->optimizer_mode();
            if (mode.is_initialized())
                {
                    ctx.optimizerModeSet(*mode);
                }

            optional<int> secPerMb = cli->getSecPerMb();
            if (secPerMb.is_initialized())
				{
            		ctx.setSecPerMb(*secPerMb);
				}

            optional<int> globalTimeout = cli->getGlobalTimeout();
            if (globalTimeout.is_initialized())
				{
            		 ctx.setGlobalTimeout(*globalTimeout);
				}

            optional<unsigned> queueTimeout = cli->queueTimeout();
            if (queueTimeout.is_initialized())
                {
                    ctx.queueTimeoutSet(*queueTimeout);
                }

            map<string, int> bring_online = cli->getBringOnline();
            if (!bring_online.empty())
                {
                    ctx.setBringOnline(bring_online);
                    // if bring online was used normal config was not!
                    return 0;
                }

            optional<std::tuple<std::string, std::string, int> > bandwidth_limitation = cli->getBandwidthLimitation();
            if (bandwidth_limitation)
                {
                    ctx.setBandwidthLimit(std::get<0>(*bandwidth_limitation),
                                          std::get<1>(*bandwidth_limitation),
                                          std::get<2>(*bandwidth_limitation));
                    return 0;
                }

            config__Configuration *config = soap_new_config__Configuration(ctx, -1);
            config->cfg = cli->getConfigurations();
            if (config->cfg.empty()) return 0;

            implcfg__setConfigurationResponse resp;
            ctx.setConfiguration(config, resp);

        }
    catch(string& ex)
        {
            cout << ex << endl;
            return 1;
        }
    catch(Err& ex)
        {
            cout << ex.what() << endl;
            return 1;
        }
    catch(std::exception& e)
        {
            cerr << "error: " << e.what() << "\n";
            return 1;
        }
    catch(...)
        {
            cerr << "Exception of unknown type!\n";
            return 1;
        }

    return 0;
}
