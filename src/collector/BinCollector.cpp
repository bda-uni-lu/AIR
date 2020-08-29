/**
 * Copyright (c) 2020 University of Luxembourg. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are
 * permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list
 * of conditions and the following disclaimer in the documentation and/or other materials
 * provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors may be
 * used to endorse or promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY OF LUXEMBOURG AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE UNIVERSITY OF LUXEMBOURG OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

/*
 * BinCollector.cpp
 *
 *  Created on: Jan 31, 2017
 *      Author: martin.theobald
 */

#include "BinCollector.hpp"

//#include <pthread/pthread.h>
#include <unistd.h>
#include <iostream>
#include <list>
#include <vector>

#include "../communication/Message.hpp"
#include "../partitioning/Partition.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

BinCollector::BinCollector(int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {
	cout << "BINCOLLECTOR [" << tag << "] CREATED @ " << rank << endl;
}

BinCollector::~BinCollector() {
	cout << "BINCOLLECTOR [" << tag << "] DELETED @ " << rank << endl;
}

void BinCollector::batchProcess() {

	cout << "BINCOLLECTOR->BATCHPROCESS: TAG[" << tag << "] @ " << rank << endl;

	if (rank == 0) { // one rank to collect all data and partitions

		Serialization ser;

		for (int i = 0; i < previous.size() * worldSize; i++) {

			Partition<int> partition;
			ser.deserialize(rMessages[i], &partition);
			for (int j = 0; j < partition.size(); j++) {
				//cout << " ___ RESULT: " << partition.get(j) << endl;
			}
		}
	}
}

void BinCollector::streamProcess(int channel) {

	//cout << "BINCOLLECTOR->STREAMPROCESS [" << tag << "] @ " << rank
	//		<< " IN-CHANNEL " << channel << endl;

	Serialization ser;
	Message* inMessage;

	int c = 0;
	bool doProcess = true;
	while (doProcess) {

		// Synchronize on incoming message channel
		pthread_mutex_lock(&listenerMutexes[channel]);

		while (inMessages[channel].empty())
			pthread_cond_wait(&listenerCondVars[channel],
					&listenerMutexes[channel]);

		while (!inMessages[channel].empty()) {

			inMessage = inMessages[channel].front();
			inMessages[channel].pop_front();

			//sleep(((rank + 1) * 2) + channel);
			cout << "BINCOLLECTOR->POP MESSAGE [" << tag << "] #" << c << " @ "
					<< rank << " IN-CHANNEL " << channel << " SIZE "
					<< inMessage->size << " QUEUE "
					<< inMessages[channel].size() << endl;

			Partition<int> partition;
			ser.deserialize(inMessage, &partition);
			for (int j = 0; j < partition.size(); j++) {
				//if (j < 10)
				//	cout << " ___ RESULT: " << partition.get(j) << endl;
			}

			delete inMessage; // delete message from incoming queue and free memory

			c++;
		}

		pthread_mutex_unlock(&listenerMutexes[channel]);
		//sched_yield();
	}
}
