/* Copyright @ Members of the EMI Collaboration, 2010.
See www.eu-emi.eu for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License"); 
you may not use this file except in compliance with the License. 
You may obtain a copy of the License at 

    http://www.apache.org/licenses/LICENSE-2.0 

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" BASIS, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
See the License for the specific language governing permissions and 
limitations under the License. */

/** \file serverconfig.cpp Implementation of FTS3 server configuration. */

#include <boost/program_options.hpp>

#include "common/error.h"
#include "serverconfig.h"
#include "serverconfigreader.h"

#ifdef FTS3_COMPILE_WITH_UNITTEST
    #include "unittest/testsuite.h"
#endif // FTS3_COMPILE_WITH_UNITTESTS

/* ---------------------------------------------------------------------- */

FTS3_CONFIG_NAMESPACE_START

using namespace FTS3_COMMON_NAMESPACE;

/* ---------------------------------------------------------------------- */

ServerConfig::ServerConfig()
{
    // EMPTY
}

/* ---------------------------------------------------------------------- */

ServerConfig::~ServerConfig()
{
    // EMPTY
}

/* ========================================================================== */

const std::string& ServerConfig::_get_str(const std::string& aVariable)
{
    _t_vars::iterator itr = _vars.find(aVariable);

    if (itr == _vars.end())
    {
    	FTS3_COMMON_EXCEPTION_THROW(Err_Custom("Server config variable " + aVariable + " not defined."));
    }

    // No worry, it will not be 0 pointer due to the exception
    return itr->second;
}

/* ---------------------------------------------------------------------- */

#ifdef FTS3_COMPILE_WITH_UNITTEST

bool Config_ServerConfig_get_str_CheckMessage (const Err_Custom&)
{
    return true;
}


/* ---------------------------------------------------------------------- */

BOOST_FIXTURE_TEST_CASE (Config_ServerConfig_get_str, ServerConfig)
{
    const std::string f_key = "key";
    const std::string f_val = "value";
    _vars[f_key] = f_val;

    // Test if key can be found
    std::string val = _get_str (f_key);
    BOOST_CHECK_EQUAL (val, f_val);

    // Test if key not found.
    BOOST_CHECK_EXCEPTION
    (
        val = _get_str ("notkey"),
        Err_Custom, 
        Config_ServerConfig_get_str_CheckMessage
    );
}

/* ---------------------------------------------------------------------- */

struct Mock_ServerConfigReader
{
    typedef std::map<std::string, std::string> type_vars;

    type_vars operator () (int, char**)
    {
         type_vars ret;
         ret["key"] = "val";
         return ret;
    }
};

/* ---------------------------------------------------------------------- */

BOOST_FIXTURE_TEST_CASE (Config_ServerConfig_read, ServerConfig)
{
    _read<Mock_ServerConfigReader> (0, NULL);
    BOOST_CHECK_EQUAL (_vars["key"], "val");
}

/* ---------------------------------------------------------------------- */

BOOST_FIXTURE_TEST_CASE (Config_ServerConfig_get, ServerConfig)
{
    _vars["key"] = "10";
    int val = get<int> ("key");
    BOOST_CHECK_EQUAL (val, 10);
}

#endif // FTS3_COMPILE_WITH_UNITTESTS

/* ---------------------------------------------------------------------- */

void ServerConfig::read
(
    int argc,
    char** argv
)
{
    _read<ServerConfigReader> (argc, argv);
}

/* ---------------------------------------------------------------------- */

#ifdef FTS3_COMPILE_WITH_UNITTEST 
    
void ServerConfig::read
(
    const std::string& aFileName
)
{
    static const int argc = 2; 
    char *argv[argc];  
    argv[0] = const_cast<char*> ("executable");
    std::string confpar = std::string("--configfile=") + aFileName;
    argv[1] = const_cast<char*> (confpar.c_str());
    read (argc, argv);
}

#endif // FTS3_COMPILE_WITH_UNITTEST

FTS3_CONFIG_NAMESPACE_END

