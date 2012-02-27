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

#include "web_service_handler.h"

#ifdef FTS3_COMPILE_WITH_UNITTEST
    #include "unittest/testsuite.h"
    #include <boost/assign/list_of.hpp>
#endif // FTS3_COMPILE_WITH_UNITTESTS

FTS3_SERVER_NAMESPACE_START

#ifdef FTS3_COMPILE_WITH_UNITTEST

/* -------------------------------------------------------------------------- */

struct Test_ActiveObject
{
    Test_ActiveObject 
    (
        const std::string& label,
        const std::string& desc
    ) :
        Label (label),
        Desc (desc),
        Enqueued (false)
    {}

    template <class OP>
    void _enqueue (OP&)
    {
        Enqueued = true;
    }

    std::string Label;
    std::string Desc;
    bool Enqueued;
};

/* -------------------------------------------------------------------------- */

struct Test_Handler
{
    Test_Handler()
        : Handled(false)
    {}


    void handle()
    {
        Handled = true;
    }

    bool Handled;
};

/* -------------------------------------------------------------------------- */

typedef boost::tuple<bool, bool> ScenarioElement;

/* -------------------------------------------------------------------------- */

struct Test_Acceptor
{
    Test_Acceptor 
    (
        const unsigned int port, 
        const std::string& ip
    ) 
    {
        Port = port;
        IP = ip;
        Accepted = false;
        New = false;
        Closed = false;
    }

    void accept ()
    {
        ScenarioType::iterator element = Scenario.begin();
        Accepted = true;

        if (element != Scenario.end())
        {   
            New = element->get<0>();
            Closed = element->get<1>();
            Scenario.erase (Scenario.begin());
        }
        else 
        {
            New = false;
            Closed = true;
        }
    }

    bool isConnectionClosed()
    {
        return Closed;
    }

    
    bool isNewConnection()
    {
        return New;
    }

    Pointer<Test_Handler>::Shared getHandler()
    {
        return Pointer<Test_Handler>::Shared();
    }

    bool Test_LoopOver()
    {
        return Scenario.empty();
    }

    static bool Accepted;
    static bool New;
    static bool Closed;
    typedef std::vector <ScenarioElement> ScenarioType;
    static ScenarioType Scenario;
    static unsigned int Port;
    static std::string IP;
};

Test_Acceptor::ScenarioType Test_Acceptor::Scenario;
bool Test_Acceptor::Accepted = false;
bool Test_Acceptor::New = false;
bool Test_Acceptor::Closed = false;
unsigned int Test_Acceptor::Port = 0;
std::string Test_Acceptor::IP;

/* -------------------------------------------------------------------------- */

struct Test_Traits
{   
    typedef Test_ActiveObject ActiveObjectType;
    typedef Test_Acceptor Acceptor;
    typedef Test_Handler Handler;
};

/* -------------------------------------------------------------------------- */

struct Test_WebServiceHandler :
    public WebServiceHandler <Test_Traits>
{
    Test_WebServiceHandler()
        : OwnType (Description()) 
    {
        _testHelper.loopOver = true ;
    }

    static const std::string& Description()
    {
        static std::string desc("desc");
        return desc;
    }

    void testListen()
    {
        _listen_a (Port(), IP());
    }

    static unsigned int Port()
    {
        return 8080;
    }
    
    static std::string IP()
    {
        return "localhost";
    }
};

/* -------------------------------------------------------------------------- */

/** Test if base constructors have been called */
BOOST_FIXTURE_TEST_CASE (Server_WebServiceHandler_Constructor, Test_WebServiceHandler)
{
    BOOST_CHECK_EQUAL (Label, "WebServiceHandler");
    BOOST_CHECK_EQUAL (Desc, Description());
}

/* -------------------------------------------------------------------------- */

/** Scenario:
 *
 * 1) New connection accepted
 * 2) Connection not closed
 */
BOOST_FIXTURE_TEST_CASE (Server_WebServiceHandler_listen_1, Test_WebServiceHandler)
{
    Test_Acceptor::Scenario  = boost::assign::tuple_list_of(true, false);
    testListen();
    BOOST_CHECK (Test_Acceptor::Accepted);
    BOOST_CHECK (!Test_Acceptor::Closed);
    BOOST_CHECK (Test_Acceptor::New);
    BOOST_CHECK (Enqueued);
}

/* -------------------------------------------------------------------------- */

/** Scenario:
 *
 * Connection closed
 */
BOOST_FIXTURE_TEST_CASE (Server_WebServiceHandler_listen_2, Test_WebServiceHandler)
{
    Test_Acceptor::Scenario  = boost::assign::tuple_list_of(true, true);
    _testHelper.loopOver = false;
    testListen();
    BOOST_CHECK (Test_Acceptor::Accepted);
    BOOST_CHECK (!Enqueued);
    BOOST_CHECK (Test_Acceptor::Closed);
}
 
/* -------------------------------------------------------------------------- */

/** Test is port and IP are passed to Acceptor */
BOOST_FIXTURE_TEST_CASE (Server_WebServiceHandler_port_ip, Test_WebServiceHandler)
{
    Test_Acceptor::Scenario  = boost::assign::tuple_list_of(true, true);
    _testHelper.loopOver = false;
    testListen();
    BOOST_CHECK_EQUAL (Test_Acceptor::Port, Port());
    BOOST_CHECK_EQUAL (Test_Acceptor::IP, IP());
}
#endif // FTS3_COMPILE_WITH_UNITTESTS

FTS3_SERVER_NAMESPACE_END

