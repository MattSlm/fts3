/*
 * RestCli.cpp
 *
 *  Created on: Feb 6, 2014
 *      Author: simonm
 */

#include "RestCli.h"

using namespace fts3::cli;

RestCli::RestCli()
{
    // add fts3-transfer-status specific options
    specific.add_options()
    ("rest", "Use the RESTful interface.")
    ("capath", value<string>(),  "Path to the GRID security certificates (e.g. /etc/grid-security/certificates).")
    ("proxy", value<string>(),  "Path to the proxy certificate (e.g. /tmp/x509up_u500).")
    ;
}

RestCli::~RestCli()
{

}

bool RestCli::rest()
{
    // return true if rest is in the map
    return vm.count("rest");
}

string RestCli::capath()
{
	if (vm.count("capath"))
		{
			return vm["capath"].as<string>();
		}

	throw string("The CA certificates path has to be specified!");
}

string RestCli::proxy()
{
	if (vm.count("proxy"))
		{
			return vm["proxy"].as<string>();
		}

	throw string("The path to the proxy certificate has to be specified!");
}
