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

#ifndef CRED_CREDSERVICE_H_
#define CRED_CREDSERVICE_H_

#include <iostream>
/**
 * CredService Interface.
 * Define the interface for retrieving a the User Credentials for a given user
 * DN
 */
class CredService
{
public:

    /**
     * Destructor
     */
    virtual ~CredService() {}

    /**
     * Get name of a file containing the credentials for the requested user
     * @param userDn [IN] The distinguished name of the user
     * @param id [IN] The credential id needed to retrieve the user's
     *        credentials (may be a passoword or an identifier, depending on the
     *        implementation)
     * @param filename [OUT] the name of the file where the credentials
     *        will be stored
     * @throws CredServiceException in case of errors in retrieving the
     *        user's credentials
     * @throws LogicError in case of the precodition are not satisfied
     */
    virtual void get(
        const std::string&  userDn,
        const std::string&  id,
        std::string&        filename);


    /**
     * Constructor
     */
    CredService();

    /**
     * Generate a name for the file that should contain the proxy certificate.
     * The length of this name shoud be (MAX_FILENAME - 7) maximum.
     * @param userDn [IN] the user DN passed to the get method
     * @param id [IN] the credential id passed to the get method
     * @return the generated file name
     */
    virtual std::string getFileName(
        const std::string& userDn,
        const std::string& id) /* throw(LogicError) */ = 0;

    /**
     * Get a new Certificate and store in into a file
     * @param userDn [IN] the user DN passed to the get method
     * @param id [IN] the credential id passed to the get method
     * @param fname [IN] the name of a temporary file where the new proxy
     * certificate should be stored
     */
    virtual void getNewCertificate(
        const std::string& userDn,
        const std::string& id,
        const std::string& fname) /* throw(LogicError, CredServiceException) */ = 0;

    /**
     * Returns the validity time that the cached copy of the certificate should
     * have.
     * @return the validity time that the cached copy of the certificate should
     * have
     */
    virtual unsigned long minValidityTime() /* throw() */
    {
        return 0;
    }

    /**
     * Returns true if the certificate in the given file name is still valid
     * @param filename [IN] trhe name of the file containing the proxy certificate
     * @return true if the certificate in the given file name is still valid
     */
    bool isValidProxy(const std::string& filename, std::string& message) /*throw()*/;


};


#endif //CRED_CREDSERVICE_H_