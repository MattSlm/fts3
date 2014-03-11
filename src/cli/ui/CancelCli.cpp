/*
 * CancelCli.cpp
 *
 *  Created on: Mar 5, 2014
 *      Author: simonm
 */

#include "CancelCli.h"

#include <boost/regex.hpp>

namespace fts3
{
namespace cli
{

using namespace boost;

CancelCli::CancelCli()
{

    specific.add_options()
    ("file,f", value<string>(&bulk_file), "Name of a configuration file.")
    ;
}

CancelCli::~CancelCli()
{

}


optional<GSoapContextAdapter&> CancelCli::validate(bool init)
{

    // do the standard validation
    if (!CliBase::validate(init).is_initialized()) return optional<GSoapContextAdapter&>();

    // check whether to use delegation
    if (vm.count("file") && vm.count("jobid"))
        {
            cout << "Either the bulk file or job ID list may be used!" << endl;
            return optional<GSoapContextAdapter&>();
        }

    prepareJobIds();

    return *ctx;
}

void CancelCli::prepareJobIds()
{
    // first check if the -f option was used, try to open the file with bulk-job description
    ifstream ifs(bulk_file.c_str());
    if (ifs)
        {
            // Parse the file
            int lineCount = 0;
            string line;
            // read and parse the lines one by one
            do
                {
                    lineCount++;
                    getline(ifs, line);

                    if (line.empty()) continue;

                    // regular expression for matching job ID
                    // job ID example: 11d24106-a24b-4d9f-9360-7c36c5176327
                    static const regex re("^\\w+-\\w+-\\w+-\\w+-\\w+$");
                    smatch what;
                    if (!regex_match(line, what, re, match_extra)) throw string("Wrong job ID format: " + line);

                    jobIds.push_back(line);

                }
            while (!ifs.eof());
        }
    else
        {
            // check whether jobid has been given as a parameter
            if (vm.count("jobid"))
                {
                    jobIds = vm["jobid"].as< vector<string> >();
                }
        }
}

} /* namespace cli */
} /* namespace fts3 */
