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

/** \file main.cpp FTS3 server entry point. */

#include "server_dev.h"

#include <signal.h>
#include <unistd.h>
#include <iostream>
#include "common/error.h"
#include "common/logger.h"
#include "config/serverconfig.h"
#include "db/generic/SingleDbInstance.h"
#include <fstream>
#include "server.h"
#include "daemonize.h"

using namespace FTS3_SERVER_NAMESPACE;
using namespace FTS3_COMMON_NAMESPACE;

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
static int fexists(const char *filename) {
    struct stat buffer;
    if (stat(filename, &buffer) == 0) return 0;
    return -1;
}


/// Handler of SIGCHLD
void _handle_sigint(int)
{
    theServer().stop();
    exit(EXIT_SUCCESS);
}

/* -------------------------------------------------------------------------- */
void fts3_initialize_logfile(){
	std::ifstream indata; 
	std::ofstream outputfile;
	string line("");
        std::vector<std::string> pathV;
        std::vector<std::string>::iterator iter;
	char *token = NULL;
		
	char*  path = "/etc/rsyslog.d/fts3syslog.conf";
	if (fexists(path) == 0){
			indata.open(path); // opens the file
   			if(!indata) { // file couldn't be opened
      				std::cerr << "Error: file " << path <<" <<could not be opened" << endl;
				return;
			   }
			   if ( indata.good() ){
      				getline (indata,line);
      				std::cout << line << endl;
    			   }
    			    indata.close();
			    if(line.length() > 0){
			        char *copy = (char *) malloc(strlen(line.c_str()) + 1);
				strcpy(copy, line.c_str());
			    	token = strtok(copy, " ");
				while ( (token = strtok(0, " ")) != NULL) {
            				pathV.push_back(std::string(token));
        			}
				free(copy);
        			copy = NULL;
				if(pathV.size() == 3){
					std::string f = pathV[2];
					if(f.length() > 0){
						if (fexists(f.c_str()) != 0){
							 outputfile.open(f.c_str());
							 outputfile.close();
						}
					}
				}
			    }
	}
}

void fts3_initialize_db_backend()
{
    std::string dbUserName = theServerConfig().get<std::string>("DbUserName");
    std::string dbPassword = theServerConfig().get<std::string>("DbPassword");
    std::string dbConnectString = theServerConfig().get<std::string>("DbConnectString");
    
    try{
    	db::DBSingleton::instance().getDBObjectInstance()->init(dbUserName, dbPassword, dbConnectString);
    } 
    catch(Err_Custom& e)
    {            
        exit(1);
    }
    catch(...){
    	FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Something is going on with the database, check username/password/connstring" << commit;
	exit(1);
    }

   
}





static bool checkUrlCopy(){
        std::string p("");
        std::vector<std::string> pathV;
        std::vector<std::string>::iterator iter;
        char *token;
        const char *path = getenv("PATH");
        char *copy = (char *) malloc(strlen(path) + 1);
        strcpy(copy, path);
        token = strtok(copy, ":");

        while ( (token = strtok(0, ":")) != NULL) {
            pathV.push_back(std::string(token));
        }

        for (iter = pathV.begin(); iter < pathV.end(); iter++) {
            p = *iter + "/fts3_url_copy";
            if (fexists(p.c_str()) == 0){
        	free(copy);
        	copy = NULL;
        	pathV.clear();
		return true;
	        }
        }

	free(copy);
	return false;
}



int main (int argc, char** argv)
{
    char *hostcert = "/etc/grid-security/hostcert.pem";

    try 
    {

    	fts3_initialize_logfile();
	
        FTS3_CONFIG_NAMESPACE::theServerConfig().read(argc, argv);
	if(false == checkUrlCopy()){
		FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Check if fts3_url_copy process is set in the PATH env variable" << commit;
		exit(1);
	}
		
        if (fexists(hostcert) != 0){
		FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Check if hostcert is installed" << commit;
		exit(1);	
	}


        fts3_initialize_db_backend();
        struct sigaction action;
        action.sa_handler = _handle_sigint;
        sigemptyset(&action.sa_mask);
        action.sa_flags = SA_RESTART;
        int res = sigaction(SIGINT, &action, NULL);
        
	std::string infosys = theServerConfig().get<std::string>("Infosys");
	if(infosys.length() > 0)
		setenv("LCG_GFAL_INFOSYS",infosys.c_str(),1);
            
        if (res == -1) 
        {
            FTS3_COMMON_EXCEPTION_THROW(Err_System());
        }

        bool isDaemon = ! FTS3_CONFIG_NAMESPACE::theServerConfig().get<bool> ("no-daemon");

        if (isDaemon)
        {            
            daemonize();
        }
        
        FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Starting server..." << commit;

        theServer().start();
    }
    catch(Err& e)
    {
        std::string msg = "Fatal error, exiting...";
        FTS3_COMMON_LOGGER_NEWLOG(ERR) << msg << commit;
        return EXIT_FAILURE;
    }
    catch(...)
    {
        std::string msg = "Fatal error (unknown origin), exiting...";
        FTS3_COMMON_LOGGER_NEWLOG(ERR) << msg << commit;
        return EXIT_FAILURE;
    }
    
    return EXIT_SUCCESS;
}

