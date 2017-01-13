/*
 * Copyright (c) CERN 2013-2015
 *
 * Copyright (c) Members of the EMI Collaboration. 2010-2013
 *  See  http://www.eu-emi.eu/partners for details on the copyright
 *  holders.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <memory>
#include "MsgProducer.h"
#include "common/Logger.h"

#include "config/ServerConfig.h"
#include "common/ConcurrentQueue.h"
#include "common/Exceptions.h"

#include <cajun/json/reader.h>
#include <decaf/lang/System.h>

using namespace fts3::config;


// Originally, this was a End-Of-Transmission character (0x04)
// which makes the messages invalid json.
// Starting with FTS 3.6, we replace this with a space, so code that explicitly
// strips the last byte regardless of its value (i.e. Rucio) still works, but
// making the message now a valid json.
// See https://its.cern.ch/jira/browse/FTS-827
// Of course, it would be nicer to get rid of this trailing byte altogether, but need to keep
// compatibility until people stops dropping the last byte.
static const char EOT = ' ';


bool stopThreads = false;

using namespace fts3::common; 


static std::string replaceMetadataString(std::string text)
{
    text = boost::replace_all_copy(text, "\\\"","\"");
    return text;
}


//utility routine private to this file
void find_and_replace(std::string &source, const std::string &find, std::string &replace)
{
    size_t j;
    for (; (j = source.find(find)) != std::string::npos;) {
        source.replace(j, find.length(), replace);
    }
}


MsgProducer::MsgProducer(const std::string &localBaseDir, const BrokerConfig& config):
    brokerConfig(config), localProducer(localBaseDir)
{
    connection = NULL;
    session = NULL;
    destination_transfer_started = NULL;
    destination_transfer_completed = NULL;
    producer_transfer_completed = NULL;
    producer_transfer_started = NULL;
    producer_transfer_state = NULL;
    destination_transfer_state = NULL;
    FTSEndpoint = fts3::config::ServerConfig::instance().get<std::string>("Alias");
    connected = false;
}

MsgProducer::~MsgProducer()
{
}


static std::string _getVoFromMessage(const std::string &msg, const char *key)
{
    std::stringstream input(msg);
    json::Object root;
    json::Reader::Read(root, input);

    auto node = root.Find(key);
    if (node == root.End()) {
        std::stringstream err;
        err << "Could not find " << key << " in the message";
        throw SystemError(err.str());
    }

    json::String value = node->element;
    return value.Value();
}


void MsgProducer::sendMessage(std::string &temp)
{
    std::string tempFTS("");

    if (temp.compare(0, 2, "ST") == 0) {
        temp = temp.substr(2, temp.length()); //remove message prefix
        tempFTS = "\"endpnt\":\"" + FTSEndpoint + "\"";
        find_and_replace(temp, "\"endpnt\":\"\"", tempFTS); //add FTS endpoint
        temp += EOT;
        cms::TextMessage *message = session->createTextMessage(temp);
        producer_transfer_started->send(message);
        FTS3_COMMON_LOGGER_LOG(DEBUG, temp);
        delete message;
    }
    else if (temp.compare(0, 2, "CO") == 0) {
        temp = temp.substr(2, temp.length()); //remove message prefix
        tempFTS = "\"endpnt\":\"" + FTSEndpoint + "\"";
        find_and_replace(temp, "\"endpnt\":\"\"", tempFTS); //add FTS endpoint

        temp = replaceMetadataString(temp);

        std::string vo;
        try {
            vo = _getVoFromMessage(temp, "vo");
        }
        catch (const std::exception &e) {
            std::ostringstream error_message;
            error_message << "(" << e.what() << ") " << temp;
            FTS3_COMMON_LOGGER_LOG(WARNING, error_message.str());
        }

        temp += EOT;
        cms::TextMessage *message = session->createTextMessage(temp);
        message->setStringProperty("vo", vo);
        producer_transfer_completed->send(message);
        FTS3_COMMON_LOGGER_LOG(DEBUG, temp);
        delete message;
    }
    else if (temp.compare(0, 2, "SS") == 0) {
        temp = temp.substr(2, temp.length()); //remove message prefix

        temp = replaceMetadataString(temp);

        std::string vo;
        try {
            vo = _getVoFromMessage(temp, "vo_name");
        }
        catch (const std::exception &e) {
            std::ostringstream error_message;
            error_message << "(" << e.what() << ") " << temp;
            FTS3_COMMON_LOGGER_LOG(WARNING, error_message.str());
        }

        temp += EOT;
        cms::TextMessage *message = session->createTextMessage(temp);
        message->setStringProperty("vo", vo);
        producer_transfer_state->send(message);
        FTS3_COMMON_LOGGER_LOG(DEBUG, temp);
        delete message;
    }
}


bool MsgProducer::getConnection()
{
    try {
        // Set properties for SSL, if enabled
        if (brokerConfig.UseSSL()) {
            FTS3_COMMON_LOGGER_LOG(INFO, "Using SSL");
            decaf::lang::System::setProperty("decaf.net.ssl.keyStore", brokerConfig.GetClientKeyStore());
            if (!brokerConfig.GetClientKeyStorePassword().empty()) {
                decaf::lang::System::setProperty("decaf.net.ssl.keyStorePassword",
                    brokerConfig.GetClientKeyStorePassword());
            }
            decaf::lang::System::setProperty("decaf.net.ssl.trustStore", brokerConfig.GetRootCA());
            if (brokerConfig.SslVerify()) {
                decaf::lang::System::setProperty("decaf.net.ssl.disablePeerVerification", "false");
            }
            else {
                FTS3_COMMON_LOGGER_LOG(INFO, "Disable peer verification");
                decaf::lang::System::setProperty("decaf.net.ssl.disablePeerVerification", "true");
            }
        }
        else {
            FTS3_COMMON_LOGGER_LOG(INFO, "Not using SSL");
        }
        FTS3_COMMON_LOGGER_LOG(DEBUG, brokerConfig.GetBrokerURI());

        // Create a ConnectionFactory
        std::unique_ptr<cms::ConnectionFactory> connectionFactory(
        cms::ConnectionFactory::createCMSConnectionFactory(brokerConfig.GetBrokerURI()));

        // Create a Connection
        if (brokerConfig.UseBrokerCredentials()) {
            connection = connectionFactory->createConnection(brokerConfig.GetUserName(), brokerConfig.GetPassword());
        }
        else {
            connection = connectionFactory->createConnection();
        }

        //connection->setExceptionListener(this);
        connection->start();

        session = connection->createSession(cms::Session::AUTO_ACKNOWLEDGE);

        // Create the destination (Topic or Queue)
        if (brokerConfig.UseTopics()) {
            destination_transfer_started = session->createTopic(brokerConfig.GetStartDestination());
            destination_transfer_completed = session->createTopic(brokerConfig.GetCompleteDestination());
            destination_transfer_state = session->createTopic(brokerConfig.GetStateDestination());
        }
        else {
            destination_transfer_started = session->createQueue(brokerConfig.GetStartDestination());
            destination_transfer_completed = session->createQueue(brokerConfig.GetCompleteDestination());
            destination_transfer_state = session->createQueue(brokerConfig.GetStateDestination());
        }

        // setTimeToLive expects milliseconds
        // GetTTL gives hours
        int ttlMs = brokerConfig.GetTTL() * 3600000;

        // Create a message producer
        producer_transfer_started = session->createProducer(destination_transfer_started);
        producer_transfer_started->setDeliveryMode(cms::DeliveryMode::PERSISTENT);
        producer_transfer_started->setTimeToLive(ttlMs);

        producer_transfer_completed = session->createProducer(destination_transfer_completed);
        producer_transfer_completed->setDeliveryMode(cms::DeliveryMode::PERSISTENT);
        producer_transfer_completed->setTimeToLive(ttlMs);

        producer_transfer_state = session->createProducer(destination_transfer_state);
        producer_transfer_state->setDeliveryMode(cms::DeliveryMode::PERSISTENT);
        producer_transfer_state->setTimeToLive(ttlMs);

        connected = true;

    }
    catch (cms::CMSException &e) {
        FTS3_COMMON_LOGGER_LOG(ERR, e.getMessage());
        connected = false;
    }
    catch (...) {
        FTS3_COMMON_LOGGER_LOG(ERR, "Unknown exception");
        connected = false;
    }

    return connected;
}

// If something bad happens you see it here as this class is also been
// registered as an ExceptionListener with the connection.
void MsgProducer::onException(const cms::CMSException &ex AMQCPP_UNUSED)
{
    FTS3_COMMON_LOGGER_LOG(ERR, ex.getMessage());
    stopThreads = true;
    std::queue<std::string> myQueue = ConcurrentQueue::getInstance()->theQueue;
    while (!myQueue.empty()) {
        std::string msg = myQueue.front();
        myQueue.pop();
        localProducer.runProducerMonitoring(msg);
    }
    connected = false;
    sleep(5);
}


void MsgProducer::run()
{
    std::string msg("");
    std::string msgBk("");
    while (stopThreads == false) {
        try {
            if (!connected) {
                cleanup();
                //https://issues.apache.org/jira/browse/AMQCPP-524
                activemq::library::ActiveMQCPP::shutdownLibrary();
                activemq::library::ActiveMQCPP::initializeLibrary();
                getConnection();

                if (!connected) {
                    sleep(10);
                    continue;
                }
            }

            //send messages
            msg = ConcurrentQueue::getInstance()->pop();
            msgBk = msg;
            sendMessage(msg);
            msg.clear();
            usleep(100);
        }
        catch (cms::CMSException &e) {
            localProducer.runProducerMonitoring(msgBk);
            FTS3_COMMON_LOGGER_LOG(ERR, e.getMessage());
            connected = false;
            sleep(5);
        }
        catch (std::exception &e) {
            localProducer.runProducerMonitoring(msgBk);
            FTS3_COMMON_LOGGER_LOG(ERR, e.what());
            connected = false;
            sleep(5);
        }
        catch (...) {
            localProducer.runProducerMonitoring(msgBk);
            FTS3_COMMON_LOGGER_LOG(CRIT, "Unexpected exception");
            connected = false;
            sleep(5);
        }
    }
}


void MsgProducer::cleanup()
{
    delete destination_transfer_started;
    destination_transfer_started = NULL;

    delete producer_transfer_started;
    producer_transfer_started = NULL;

    delete destination_transfer_completed;
    destination_transfer_completed = NULL;

    delete producer_transfer_completed;
    producer_transfer_completed = NULL;


    delete destination_transfer_state;
    destination_transfer_state = NULL;

    delete producer_transfer_state;
    producer_transfer_state = NULL;

    // Close open resources.
    try {
        if (session != NULL) {
            session->close();
        }
    }
    catch (cms::CMSException &e) {
        e.printStackTrace();
    }

    delete session;
    session = NULL;

    try {
        if (connection != NULL) {
            connection->close();
        }
    }
    catch (cms::CMSException &e) {
        e.printStackTrace();
    }

    delete connection;
    connection = NULL;
}
