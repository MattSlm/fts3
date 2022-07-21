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

#ifndef LINKCONFIG_H_
#define LINKCONFIG_H_

#include <string>

enum OptimizerMode {
    kOptimizerDisabled = 0,
    kOptimizerConservative = 1,
    kOptimizerNormal = 2,
    kOptimizerAggressive = 3,
    kOptimizerAggregated = 4
};

class LinkConfig
{
public:
    LinkConfig(): minActive(0), maxActive(0), optimizerMode(kOptimizerDisabled), tcpBufferSize(0), numberOfStreams(0) {}
    ~LinkConfig() {};

    std::string source;
    std::string destination;
    int minActive;
    int maxActive;
    OptimizerMode optimizerMode;
    int tcpBufferSize;
    int numberOfStreams;
};

#endif // LINKCONFIG_H_
