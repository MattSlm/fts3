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

#pragma once

#include "server_dev.h"
#include "common/threadtraits.h"
#include "common/pointers.h"

FTS3_SERVER_NAMESPACE_START

using namespace FTS3_COMMON_NAMESPACE;

/* ---------------------------------------------------------------------- */

class ThreadFunction 
{
private:
    /* ------------------------------------------------------------------ */
	
    class ProgressCondition 
	{
	public:
		ProgressCondition() 
            : _stopped(false) 
        {};
		
        /* -------------------------------------------------------------- */
		
        bool wait(const boost::xtime t) 
        {
			ThreadTraits::LOCK_R lock(_mutex);
			bool ret = _stopped ? true : _cond.timed_wait(lock, t);
			return ret;
		};
		
        /* -------------------------------------------------------------- */
		
        void finished() 
        {
			ThreadTraits::LOCK_R lock(_mutex);
			_stopped = true;
			_cond.notify_all();
		}
		
	private:
        
        /* -------------------------------------------------------------- */
		
        bool _stopped;
        
        /* -------------------------------------------------------------- */
		
        mutable ThreadTraits::MUTEX_R _mutex;
        
        /* -------------------------------------------------------------- */

        ThreadTraits::CONDITION _cond;
	};

public:
	ThreadFunction() : _pcond(new ProgressCondition) {};
	ThreadFunction(const ThreadFunction& tf) : _pcond(tf._pcond) {};
	virtual ~ThreadFunction() {};
	
	bool wait(const boost::xtime t) {
		return _pcond->wait(t);
	};
	
protected:
	void _finished() {
		_pcond->finished();
	}
	
	virtual void operator () () = 0;
	
private:
	Pointer<ProgressCondition>::Shared _pcond;
};

FTS3_SERVER_NAMESPACE_END

