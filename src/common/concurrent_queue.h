/*
 *	Copyright notice:
 *	Copyright © Members of the EMI Collaboration, 2010.
 *
 *	See www.eu-emi.eu for details on the copyright holders
 *
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */

#ifndef _TQUEUE_H_
#define _TQUEUE_H_

#include <pthread.h>
#include <queue>
#include <iostream>


class concurrent_queue
{
private:
    static bool instanceFlag;
    static concurrent_queue *single;
    pthread_mutex_t lock; // The queue lock
    pthread_cond_t  cv;   // Lock conditional variable
    int             blck; // should pop() block by default

public:
    static concurrent_queue* getInstance();
    std::queue<std::string> the_queue;  // The queue
    void nonblock();
    void block();
    bool empty();
    unsigned int size();
    void push( std::string value );
    std::string pop(const int wait = -1);
    concurrent_queue()
    {
        blck = 1;
        pthread_mutex_init(&lock, NULL);
        pthread_cond_init (&cv, NULL);
    }
};


#endif
