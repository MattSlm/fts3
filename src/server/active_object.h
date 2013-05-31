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
#include "common/monitorobject.h"
#include "common/ctl.h"
#include <boost/bind.hpp>
#include <boost/utility.hpp>
#include <assert.h>

FTS3_SERVER_NAMESPACE_START

using namespace FTS3_COMMON_NAMESPACE;

/* ---------------------------------------------------------------------- */

/** \brief Class implementing Active Object pattern.
 *
 * See the concept here: http://en.wikipedia.org/wiki/Active_object
 *
 */
template
<
class ExecutionPolicy, /**< Determines how the system executes the method.
        Queues it, executes it immediately, etc. */
      class TracingModel = ctl::EmptyType /**< Tracing of method execution. Tracing:
        log object creation, etc. Default: no tracing. */
      >
class ActiveObject :
    private boost::noncopyable,
    public MonitorObject
{
public:
    /* ------------------------------------------------------------------ */
    /** Constructor. */
    template<class T>
    ActiveObject
    (
        const T& t /**< A tracable object (can log the traced object state) */
    ) :
        _tracer(t),
        _runningMethods(0)
    {}

    /* ------------------------------------------------------------------ */

    template<class T, class U>
    ActiveObject
    (
        const T& t,
        const U& u
    ) :
        _tracer(t, u),
        _runningMethods(0)
    {}

    /* ------------------------------------------------------------------ */

    ActiveObject
    (
        const ctl::EmptyType&
    )
        : _runningMethods(0)
    {}
    /* ------------------------------------------------------------------ */

    ActiveObject()
        : _runningMethods(0)
    {}

    /* ------------------------------------------------------------------ */

    virtual ~ActiveObject()
    {}

    /* ------------------------------------------------------------------ */


protected:

    /* ------------------------------------------------------------------ */

    typedef ActiveObject<ExecutionPolicy, TracingModel> BaseType;

    /* ------------------------------------------------------------------ */

    typedef TracingModel TracingModelType;

    /* ------------------------------------------------------------------ */

    /** The method "body" itself, implementing the "Command" pattern */
    template<class OP>
    class Command
    {
    public:
        Command
        (
            ActiveObject* aobj,
            OP& op
        ) :
            _aobj(aobj),
            _op(op)
        {}

        /* -------------------------------------------------------------- */

        void operator () ()
        {
            _aobj->template _doWork(_op);
        }

    protected:

        /* -------------------------------------------------------------- */

        ActiveObject* _aobj;

        /* -------------------------------------------------------------- */

        OP _op;
    };

    /* ------------------------------------------------------------------ */

    template<class OP>
    void _enqueue(OP& op)
    {
        FTS3_COMMON_MONITOR_START_CRITICAL
        ++_runningMethods;
        FTS3_COMMON_MONITOR_END_CRITICAL
        Command<OP> packagedOp(this, op);
        ExecutionPolicy::instance().enqueue(packagedOp);
    }

    /* ------------------------------------------------------------------ */

    template<class OP>
    void _doWork(OP op)
    {
        try
            {
                op();
            }
        catch (...)
            {
                FTS3_COMMON_MONITOR_START_CRITICAL
                _decrease();
                throw;
                FTS3_COMMON_MONITOR_END_CRITICAL
            }

        FTS3_COMMON_MONITOR_START_CRITICAL
        _decrease();
        FTS3_COMMON_MONITOR_END_CRITICAL
    }

    /* ------------------------------------------------------------------ */

    TracingModel _tracer;

protected:

    /* ------------------------------------------------------------------ */

    void _decrease()
    {
        assert(_runningMethods > 0);
        --_runningMethods;
        _notRunning.notify_all();
    }

    /* ------------------------------------------------------------------ */

    template<class T>
    std::string _desc
    (
        const std::string& d,
        const T& t
    )
    {
        std::string ret = (t.id() + "(" + d + ")");
        return ret;
    }

    /* ------------------------------------------------------------------ */

    std::string _desc
    (
        const std::string& d,
        const ctl::EmptyType&
    )
    {
        std::string ret = "(" + d + ")";
        return ret;
    }

    /* ------------------------------------------------------------------ */

    std::string _description
    (
        const std::string& d
    )
    {
        return  _desc(d, _tracer);
    }

    /* ------------------------------------------------------------------ */

    unsigned long _runningMethods;

    /* ------------------------------------------------------------------ */

    typename synch_traits::CONDITION _notRunning;
};

FTS3_SERVER_NAMESPACE_END

