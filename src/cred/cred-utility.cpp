/********************************************//**
 * Copyright @ Members of the EMI Collaboration, 2010.
 * See www.eu-emi.eu for details on the copyright holders.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 ***********************************************/

#include "cred-utility.h"
#include "CredService.h"
#include "common/logger.h"
#include "common/error.h"
#include "DelegCred.h"

using namespace FTS3_COMMON_NAMESPACE; 

#ifdef __cplusplus
extern "C"{
#endif //__cplusplus
// Fix Warning
#ifdef IOV_MAX 
#undef IOV_MAX
#endif //IOV_MAX
#include "globus_gsi_credential.h"
#ifdef __cplusplus
}
#endif //__cplusplus

#include <boost/scoped_ptr.hpp>
#include <unistd.h>


using boost::scoped_ptr;
    
/*
 * get_proxy_dn
 *
 * Get Proxy DN
 */
std::string get_proxy_dn(const std::string& filename) /*throw (CredServiceException)*/{
    
    std::string dn;
    
    // Activate Module
    globus_module_activate(GLOBUS_GSI_CREDENTIAL_MODULE);
    
    // Check if it's valid
    globus_gsi_cred_handle_t        proxy_handle = 0;
    globus_gsi_cred_handle_attrs_t  handle_attrs = 0;
    try {
        // Init handle attributes     
        globus_result_t result = globus_gsi_cred_handle_attrs_init(&handle_attrs);
        if(0 != result){
            //throw CredServiceException("Cannot Init Handle Attributes");
	    FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Cannot Init Handle Attributes" << commit;
        }
        // Init handle
        result = globus_gsi_cred_handle_init(&proxy_handle,handle_attrs);
        if(0 != result){
            //throw CredServiceException("Cannot Init Handle");
	    FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Cannot Init Handle" << commit;
        }
        if(false == filename.empty()){
            //_log_trace(CRED_MODULE,"Reading proxy certificate " << filename);
    	    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Reading proxy certificate " << filename << commit;
            // Load Proxy
            result = globus_gsi_cred_read_proxy(proxy_handle,const_cast<char *>(filename.c_str()));
            if(0 != result){
                //throw CredServiceException("Cannot Load Proxy File");
		FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Cannot Load Proxy File" << commit;
            }
        } else {
            //_log_trace(CRED_MODULE,"Reading default certificate");
	    FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Reading default certificate" << filename << commit;
            // Read Default
            result = globus_gsi_cred_read(proxy_handle,0);
            if(0 != result){
                //throw CredServiceException("Cannot Read Proxy");
		FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Cannot Read Proxy" << commit;
		
            }
        }
        //_log_trace(CRED_MODULE,"Get the Subject Name for agent certificate");
	FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Get the Subject Name for agent certificate" << commit;	
        char * subject_name = 0;
        result = globus_gsi_cred_get_subject_name(proxy_handle,&subject_name);
        if(0 != result){
            //throw CredServiceException("Cannot get Subject Name");
       	    FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Cannot get Subject Name" << commit;	
        }
        if(0 != subject_name){
            dn = subject_name;
            free(subject_name);
        }
    } catch(...){
        // Destroy handles
        if(0 != proxy_handle){
            globus_gsi_cred_handle_destroy(proxy_handle);
        }
        if(0 != handle_attrs){
            globus_gsi_cred_handle_attrs_destroy(handle_attrs);
        }
        // Deactivate Module
        globus_module_deactivate(GLOBUS_GSI_CREDENTIAL_MODULE);
        throw;
    }
    
    // Destroy handles
    if(0 != proxy_handle){
        globus_gsi_cred_handle_destroy(proxy_handle);
    }
    if(0 != handle_attrs){
        globus_gsi_cred_handle_attrs_destroy(handle_attrs);
    }
    // Deactivate Module
    globus_module_deactivate(GLOBUS_GSI_CREDENTIAL_MODULE);

    return dn;
}

/*
 * get_proxy_cert
 *
 * Get Proxy Certificate File Name for the given job
 */
std::string get_proxy_cert(const std::string& user_dn,
                           const std::string& user_cred,
                           const std::string& ,
                           const std::string& ,
                           const std::string& ,
                           const std::string& ,
                           bool  disable_delegation,
                           const std::string& ) /*throw (LogicError, CredServiceException)*/ {
    std::string proxy_file;

    // Check if the job contains information for retrieving delegated credential 
    if((disable_delegation == false) && 
       (false == user_dn.empty()) ) {
        //_log_trace(CRED_MODULE,"Get the Proxy Certificate for that user");
	FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Get the Proxy Certificate for that user" << commit;
           
        // Get CertProxy Service                           
        scoped_ptr<CredService> cred_service;
	cred_service.reset(new DelegCred);
        try {           
            // Get Proxy Certificate
            cred_service->get(user_dn,user_cred,proxy_file);
            //_log_trace(CRED_MODULE,"Proxy Certificate is " << proxy_file);
		//FTS3_COMMON_LOGGER_NEWLOG(INFO) << "Proxy Certificate is " << proxy_file << commit;	    
        } catch(const std::exception& exc){
            //_log_trace(CRED_MODULE,"Cannot Retrieve Proxy Certificate");
	    FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Cannot Retrieve Proxy Certificate" << proxy_file << commit;	    
            throw;
        }
    } else {
        // Don't Use Delegated Credentials
        //_log_trace(CRED_MODULE,"Delegated Credentials not used");
	FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Delegated Credentials not used" << commit;
    }
    return proxy_file;
}

/*
 * get_proxy_lifetime
 *
 * Return the proxy lifetime. In case of other errors, returns -1
 */
time_t get_proxy_lifetime(const std::string& filename) /*throw ()*/ {
    
    time_t lifetime = (time_t)-1;

    // Check that the file Exists
    int result = access(filename.c_str(), R_OK);
    if(0 != result){
        //_log_trace(CRED_MODULE,"Requested Proxy doesn't exist. A new one should be created. Reason is " << strerror(errno));
	FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Requested Proxy doesn't exist. A new one should be created. Reason is " << strerror(errno) << commit;	
        return lifetime;
    }
    // Check if it's valid
    globus_gsi_cred_handle_t        proxy_handle = 0;
    globus_gsi_cred_handle_attrs_t  handle_attrs = 0;
    try {
        // Init handle attributes     
        globus_result_t result = globus_gsi_cred_handle_attrs_init(&handle_attrs);
        if(0 != result){
            //throw RuntimeError("Cannot Init Handle Attributes");
	    FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Cannot Init Handle Attributes" << commit;
        }
        // Init handle
        result =  globus_gsi_cred_handle_init(&proxy_handle,handle_attrs);
        if(0 != result){
            //throw RuntimeError("Cannot Init Handle");
	    FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Cannot Init Handle" << commit;
        }
        // Load Proxy
        result = globus_gsi_cred_read_proxy(proxy_handle,const_cast<char *>(filename.c_str()));
        if(0 != result){
            //throw RuntimeError("Cannot Load Proxy File");
	    FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Cannot Load Proxy File" << commit;
        }
        // Get Lifetime (in seconds)
        result = globus_gsi_cred_get_lifetime(proxy_handle,&lifetime);
        if(0 != result){
            //throw RuntimeError("Cannot Get Proxy Lifetime");
	    FTS3_COMMON_LOGGER_NEWLOG(ERR) << "Cannot Get Proxy Lifetime" << commit;	    
        }
    } catch(const std::exception& exc) {
        //_log_trace(CRED_MODULE,"Cannot Check Proxy Validity. Reason is: " << exc.what());
	FTS3_COMMON_LOGGER_NEWLOG(ERR) <<" Cannot Check Proxy Validity. Reason is: " << exc.what() << commit;	    	
        lifetime = (time_t)-1;
    }
    // Destroy handles
    if(0 != proxy_handle){
        globus_gsi_cred_handle_destroy(proxy_handle);
    }
    if(0 != handle_attrs){
        globus_gsi_cred_handle_attrs_destroy(handle_attrs);
    }
    return lifetime;
}
