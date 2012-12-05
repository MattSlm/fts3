/*
 *	Copyright notice:
 *	Copyright © Members of the EMI Collaboration, 2010.
 *
 *	See www.eu-emi.eu for details on the copyright holders
 *
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use soap file except in compliance with the License.
 *	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implcfgied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */


#include "ws-ifce/gsoap/gsoap_stubs.h"

#include "GSoapDelegationHandler.h"
#include "ws/AuthorizationManager.h"

#include "common/logger.h"
#include <common/error.h>
#include "threadtraits.h"

using namespace std;
using namespace fts3::common;
using namespace fts3::ws;

//ThreadTraits::MUTEX _mutex;

int fts3::delegation__getProxyReq(struct soap* soap, std::string _delegationID, struct delegation__getProxyReqResponse &_param_4) {

//	FTS3_COMMON_LOGGER_NEWLOG (INFO) << "Handling 'delegation__getProxyReq' request" << commit;
//	ThreadTraits::LOCK lock(_mutex);
	try {
		AuthorizationManager::getInstance().authorize(
				soap,
				AuthorizationManager::DELEG,
				AuthorizationManager::dummy
			);

		GSoapDelegationHandler handler(soap);
		_param_4._getProxyReqReturn = handler.getProxyReq(_delegationID);

	} catch (Err& ex) {

		FTS3_COMMON_LOGGER_NEWLOG (ERR) << "An exception has been caught: " << ex.what() << commit;
		soap_receiver_fault(soap, ex.what(), "DelegationException");
		return SOAP_FAULT;
	}

	return SOAP_OK;
}

int fts3::delegation__getNewProxyReq(struct soap* soap, struct delegation__getNewProxyReqResponse &_param_5) {

//	ThreadTraits::LOCK lock(_mutex);
//	FTS3_COMMON_LOGGER_NEWLOG (INFO) << "Handling 'delegation__getNewProxyReq' request" << commit;

	try {
		AuthorizationManager::getInstance().authorize(
				soap,
				AuthorizationManager::DELEG,
				AuthorizationManager::dummy
			);

		GSoapDelegationHandler handler(soap);
		_param_5.getNewProxyReqReturn = handler.getNewProxyReq();

	} catch (Err& ex) {

		FTS3_COMMON_LOGGER_NEWLOG (ERR) << "An exception has been caught: " << ex.what() << commit;
		soap_receiver_fault(soap, ex.what(), "DelegationException");
		return SOAP_FAULT;
	}

	return SOAP_OK;
}

int fts3::delegation__renewProxyReq(struct soap* soap, std::string _delegationID, struct delegation__renewProxyReqResponse &_param_6) {

//	ThreadTraits::LOCK lock(_mutex);
//	FTS3_COMMON_LOGGER_NEWLOG (INFO) << "Handling 'delegation__renewProxyReq' request" << commit;

	try {
		AuthorizationManager::getInstance().authorize(
				soap,
				AuthorizationManager::DELEG,
				AuthorizationManager::dummy
			);

		GSoapDelegationHandler handler(soap);
		_param_6._renewProxyReqReturn = handler.renewProxyReq(_delegationID);

	} catch(Err& ex) {

		FTS3_COMMON_LOGGER_NEWLOG (ERR) << "An exception has been caught: " << ex.what() << commit;
		soap_receiver_fault(soap, ex.what(), "DelegationException");
		return SOAP_FAULT;
	}

	return SOAP_OK;
}

int fts3::delegation__putProxy(struct soap* soap, std::string _delegationID, std::string _proxy, struct delegation__putProxyResponse &_param_7) {
//	ThreadTraits::LOCK lock(_mutex);
//	FTS3_COMMON_LOGGER_NEWLOG (INFO) << "Handling 'delegation__putProxy' request" << commit;

	try {
		AuthorizationManager::getInstance().authorize(
				soap,
				AuthorizationManager::DELEG,
				AuthorizationManager::dummy
			);

		GSoapDelegationHandler handler(soap);
		handler.putProxy(_delegationID, _proxy);

	} catch (Err& ex) {

		FTS3_COMMON_LOGGER_NEWLOG (ERR) << "An exception has been caught: " << ex.what() << commit;
		soap_receiver_fault(soap, ex.what(), "DelegationException");
		return SOAP_FAULT;
	}

	return SOAP_OK;
}

int fts3::delegation__getTerminationTime(struct soap* soap, std::string _delegationID, struct delegation__getTerminationTimeResponse &_param_8) {
//	ThreadTraits::LOCK lock(_mutex);
//	FTS3_COMMON_LOGGER_NEWLOG (INFO) << "Handling 'delegation__getTerminationTime' request" << commit;

	try {
		AuthorizationManager::getInstance().authorize(
				soap,
				AuthorizationManager::DELEG,
				AuthorizationManager::dummy
			);

		GSoapDelegationHandler handler(soap);
		_param_8._getTerminationTimeReturn = handler.getTerminationTime(_delegationID);

	} catch (Err& ex) {

		FTS3_COMMON_LOGGER_NEWLOG (ERR) << "An exception has been caught: " << ex.what() << commit;
		soap_receiver_fault(soap, ex.what(), "DelegationException");
		return SOAP_FAULT;
	}

	return SOAP_OK;
}

int fts3::delegation__destroy(struct soap* soap, std::string _delegationID, struct delegation__destroyResponse &_param_9) {
//	ThreadTraits::LOCK lock(_mutex);
//	FTS3_COMMON_LOGGER_NEWLOG (INFO) << "Handling 'delegation__destroy' request" << commit;

	try {
		AuthorizationManager::getInstance().authorize(
				soap,
				AuthorizationManager::DELEG,
				AuthorizationManager::dummy
			);

		GSoapDelegationHandler handler(soap);
		handler.destroy(_delegationID);

	} catch(Err& ex) {

		FTS3_COMMON_LOGGER_NEWLOG (ERR) << "An exception has been caught: " << ex.what() << commit;
		soap_receiver_fault(soap, ex.what(), "DelegationException");
		return SOAP_FAULT;
	}

	return SOAP_OK;
}

int fts3::delegation__getVersion(struct soap* soap, struct delegation__getVersionResponse &_param_1) {

//	FTS3_COMMON_LOGGER_NEWLOG (INFO) << "Handling 'delegation__getVersion' request" << commit;
	_param_1.getVersionReturn = "3.7.6-1";

	return SOAP_OK;
}

int fts3::delegation__getInterfaceVersion(struct soap* soap, struct delegation__getInterfaceVersionResponse &_param_2) {

//	FTS3_COMMON_LOGGER_NEWLOG (INFO) << "Handling 'delegation__getInterfaceVersion' request" << commit;
	_param_2.getInterfaceVersionReturn = "3.7.0";

	return SOAP_OK;
}

int fts3::delegation__getServiceMetadata(struct soap* soap, std::string _key, struct delegation__getServiceMetadataResponse &_param_3) {

//	FTS3_COMMON_LOGGER_NEWLOG (INFO) << "Handling 'delegation__getServiceMetadata' request" << commit;
	_param_3._getServiceMetadataReturn = "glite-data-fts-service-3.7.6-1";

	return SOAP_OK;
}

