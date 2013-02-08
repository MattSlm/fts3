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
 
#include "gsoap_acceptor.h"
#include "gsoap_request_handler.h"
#include "ws-ifce/gsoap/fts3.nsmap"
#include "serverconfig.h"
#include "server_dev.h"
#include <cgsi_plugin.h>
#include <signal.h>
#include "StaticSslLocking.h"
#include <sys/socket.h>
#include <fstream>

extern bool  stopThreads;

using namespace FTS3_COMMON_NAMESPACE;
using namespace FTS3_CONFIG_NAMESPACE;
using namespace fts3::common;
FTS3_SERVER_NAMESPACE_START

GSoapAcceptor::GSoapAcceptor(const unsigned int port, const std::string& ip) {
	
	bool keepAlive = theServerConfig().get<std::string>("HttpKeepAlive")=="true" ? true : false;
	if (keepAlive) {
		ctx = soap_new2(SOAP_IO_KEEPALIVE, SOAP_IO_KEEPALIVE);

		ctx->bind_flags |= SO_REUSEADDR;
		ctx->max_keep_alive = 100; // at most 100 calls per keep-alive session		
		ctx->accept_timeout = 0; 
		ctx->socket_flags = MSG_NOSIGNAL; // use this, prevent sigpipe
		ctx->recv_timeout = 120; // Timeout after 1 minutes stall on recv
		ctx->send_timeout = 120; // Timeout after 1 minute stall on send

		soap_cgsi_init(ctx,  CGSI_OPT_KEEP_ALIVE  | CGSI_OPT_SERVER | CGSI_OPT_SSL_COMPATIBLE | CGSI_OPT_DISABLE_MAPPING);// | CGSI_OPT_DISABLE_NAME_CHECK);
		soap_set_namespaces(ctx, fts3_namespaces);

//		soap_set_imode(ctx, SOAP_ENC_MTOM | SOAP_IO_CHUNK);
//		soap_set_omode(ctx, SOAP_ENC_MTOM | SOAP_IO_CHUNK);
//
//		ctx->fmimereadopen = LogFileStreamer::readOpen;
//		ctx->fmimereadclose = LogFileStreamer::readClose;
//		ctx->fmimeread = LogFileStreamer::read;
//
//		ctx->fmimewriteopen = LogFileStreamer::writeOpen;
//		ctx->fmimewriteclose = LogFileStreamer::writeClose;
//		ctx->fmimewrite = LogFileStreamer::write;

		SOAP_SOCKET sock = soap_bind(ctx, ip.c_str(), static_cast<int>(port), 100);
		if (sock >= 0) {
			FTS3_COMMON_LOGGER_NEWLOG (INFO) << "Soap service " << sock << " IP:" << ip << " Port:" << port << commit;
		} else {
			FTS3_COMMON_EXCEPTION_THROW (Err_System ("Unable to bound to socket."));
			fclose (stderr);
			kill(getpid(), SIGINT);
		}

	} else {	
		ctx = soap_new();

		ctx->bind_flags |= SO_REUSEADDR;
		soap_cgsi_init(ctx,  CGSI_OPT_SERVER | CGSI_OPT_SSL_COMPATIBLE | CGSI_OPT_DISABLE_MAPPING);// | CGSI_OPT_DISABLE_NAME_CHECK);
		soap_set_namespaces(ctx, fts3_namespaces);

//		soap_set_imode(ctx, SOAP_ENC_MTOM | SOAP_IO_CHUNK);
//		soap_set_omode(ctx, SOAP_ENC_MTOM | SOAP_IO_CHUNK);
//
//		ctx->fmimereadopen = LogFileStreamer::readOpen;
//		ctx->fmimereadclose = LogFileStreamer::readClose;
//		ctx->fmimeread = LogFileStreamer::read;
//
//		ctx->fmimewriteopen = LogFileStreamer::writeOpen;
//		ctx->fmimewriteclose = LogFileStreamer::writeClose;
//		ctx->fmimewrite = LogFileStreamer::write;

		SOAP_SOCKET sock = soap_bind(ctx, ip.c_str(), static_cast<int>(port), 100);

	    if (sock >= 0) {
	        FTS3_COMMON_LOGGER_NEWLOG (INFO) << "Soap service " << sock << " IP:" << ip << " Port:" << port << commit;
	    } else {
	        FTS3_COMMON_EXCEPTION_THROW (Err_System ("Unable to bound to socket."));
	        fclose (stderr);   
	        kill(getpid(), SIGINT);
	    }
	}
}

GSoapAcceptor::~GSoapAcceptor() {
	soap* tmp=NULL;
	while (!recycle.empty()) {
		tmp = recycle.front();
                if(tmp){
		recycle.pop();	       
		soap_clr_omode(tmp, SOAP_IO_KEEPALIVE);
		shutdown(tmp->socket,2);               
		shutdown(tmp->master,2);
		soap_destroy(tmp);
		soap_end(tmp);
		soap_done(tmp);
		soap_free(tmp);		
               }
	}
      if(ctx){	
	soap_clr_omode(ctx, SOAP_IO_KEEPALIVE);
	shutdown(ctx->master,2);
	shutdown(ctx->socket,2);
	soap_destroy(ctx);
	soap_end(ctx);
	soap_done(ctx);
	soap_free(ctx);	
      }
}

boost::shared_ptr<GSoapRequestHandler> GSoapAcceptor::accept() {
    
    SOAP_SOCKET sock = soap_accept(ctx);
    boost::shared_ptr<GSoapRequestHandler> handler;

    if (sock >= 0) {
       
        handler.reset (
        		new GSoapRequestHandler(*this)
        	);
    } else {
        FTS3_COMMON_EXCEPTION_LOGERROR (Err_System ("Unable to accept connection request."));
    }

    return handler;
}

soap* GSoapAcceptor::getSoapContext() {	
        ThreadTraits::LOCK_R lock(_mutex);
	if (!recycle.empty()) {
		soap* ctx = recycle.front();
		recycle.pop();
		if(ctx){
			ctx->socket = this->ctx->socket;
			return ctx;
		}
	}
	

	return soap_copy(ctx);
}

void GSoapAcceptor::recycleSoapContext(soap* ctx) {     
	if(stopThreads) 
		return; 
		
	if(ctx){		
		soap_destroy(ctx);
		soap_end(ctx);
	        ThreadTraits::LOCK_R lock(_mutex);
		recycle.push(ctx);		
	}	
}

FTS3_SERVER_NAMESPACE_END
