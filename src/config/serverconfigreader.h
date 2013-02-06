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

/** \file serverconfig.h Define FTS3 server configuration. */

#pragma once

#include "config_dev.h"
#include "common/logger.h"
#include <boost/program_options.hpp>
#include <map>

FTS3_CONFIG_NAMESPACE_START

namespace po = boost::program_options;
using namespace FTS3_COMMON_NAMESPACE;

/* ========================================================================== */

/** \brief Class reading the server server configuration, from the command line
 * and from the config file. */
class ServerConfigReader
{
public:
    // Return type of the functional operator.
    typedef std::map<std::string, std::string> type_return;

    /* ---------------------------------------------------------------------- */

    /// Read configuration(s).
    type_return operator ()
    (
        int argc, /**< The command line arguments (from main) */
        char** argv /**< The command line arguments (from main) */
    );

    /* ---------------------------------------------------------------------- */

    /** Convert all the non-string parameters to strings, and store
     * them accordingly. */
    void storeValuesAsStrings();

    /**
     *
     */
    void storeRoles();

    /* ---------------------------------------------------------------------- */

    /** Store a processes option value in _vars as string.
     *
     * Logic behind: the processor reads command line options, checks if
     * they correspond to the required types (int, etc.), and stores them
     * converted to the type. But, we want to store all the values as string -
     * so, we convert them back. */
    void storeAsString
    (
        const std::string& aName /**< Name of the config property */
    );

protected:

    /* ---------------------------------------------------------------------- */

    /** Read the config from the command line. */
    template<class DEPENDENCIES>
    void _readCommandLineOptions
    (
        int argc,  /**< The command line arguments (from main) */
        char *argv[],  /**< The command line arguments (from main) */
        po::options_description &desc /**< Description of command line options */
    )
    {
        po::store(po::parse_command_line(argc, argv, desc), _vm);
        po::notify(_vm);

        if (_vm.count("help"))
        {
            DEPENDENCIES::stream() << desc << "\n";
            return;
            //DEPENDENCIES::exit(1);
        }

        if (_vm.count("version"))
        {
            DEPENDENCIES::stream() << FTS3_SERVER_VERSION << "\n";
            return;
            //DEPENDENCIES::exit(1);
        }

        bool isNoDaemon = _vm.count ("no-daemon");
        _vars["no-daemon"] = isNoDaemon ? "true" : "false";

        DEPENDENCIES::processVariables(*this);
    }

    /* ---------------------------------------------------------------------- */

    /** Read the config from the config file. */
    template<class DEPENDENCIES>
    void _readConfigFile
    (
        po::options_description &desc /**< Description of command line options */
    )
    {
        type_return::iterator config = _vars.find ("configfile");
        assert (config != _vars.end());
        boost::shared_ptr<std::istream> in = DEPENDENCIES::getStream (config->second);
        assert (in.get());

        try
        {
            po::store(po::parse_config_file(*in, desc), _vm);
        }
        catch (std::exception& e)
        {
            FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Error in parsing config file: " << e.what() << commit;
            throw;
            //DEPENDENCIES::exit(1);
        }
        catch (...)
        {
            FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Unknown error when parsing config file." << commit;
            throw;
            //DEPENDENCIES::exit(1);
        }

        po::notify(_vm);
        DEPENDENCIES::processVariables(*this);
    }

    /* ---------------------------------------------------------------------- */

    /** Define generic options (no equivalent in config file) */
    po::options_description _defineGenericOptions();

    /* ---------------------------------------------------------------------- */

    /** Define config options (both in command line and config file) */
    po::options_description _defineConfigOptions();

    /* ---------------------------------------------------------------------- */

    /** Define hidden options: available both on command line and in config file,
     * but will not be shown to the user. */
    po::options_description _defineHiddenOptions();

    /* ---------------------------------------------------------------------- */

    /** Store the final option set as strings */
    type_return _vars;

    /* ---------------------------------------------------------------------- */

    /** Internal variable map that the parsers use */
    po::variables_map _vm;
};

FTS3_CONFIG_NAMESPACE_END

