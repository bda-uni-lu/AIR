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
 * Map.cpp
 *
 *  Created on: Dec 20, 2017
 *      Author: martin.theobald, amal.tawakuli
 */

#include "Map.hpp"

//#include <pthread/pthread.h>
#include <unistd.h>
#include <iostream>
#include <list>
#include <vector>

#include "../communication/Message.hpp"
#include "../function/Function.hpp"
#include "../partitioning/Partition.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

Map::Map(Function* func, int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {
	this->func = func;
	cout << "MAP [" << tag << "] CREATED @ " << rank << endl;
}

Map::~Map() {
	cout << "MAP [" << tag << "] DELETED @ " << rank << endl;
}

void Map::batchProcess() {

	cout << "MAP->BATCHPROCESS [" << tag << "] @ " << rank << endl;

	Serialization ser;
	Partition<int> partitions[previous.size() * worldSize];

	// first process partitions
	for (int i = 0; i < previous.size() * worldSize; i++) {
		ser.deserialize(rMessages[i], &partitions[i]);
		for (int j = 0; j < partitions[i].size(); j++) {
			partitions[i].set(func->calculate(partitions[i].get(j)), j);
		}
	}

	// then redistribute the data, here includes resharding across channels!
	if (next.size() > 0)
		ser.serialize(partitions, previous.size() * worldSize, sMessages,
				next.size() * worldSize);
}

void Map::streamProcess(int channel) {

	//cout << "MAP->STREAMPROCESS [" << tag << "] @ " << rank << " IN-CHANNEL "
	//		<< channel << endl;

	Message* inMessage, *outMessage;
	list<Message*>* tmpMessages = new list<Message*>();

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

			//cout << "MAP->POP MESSAGE [" << tag << "] #" << c << " @ " << rank
			//		<< " IN-CHANNEL " << channel << " SIZE " << inMessage->size
			//		<< endl;

			tmpMessages->push_back(inMessage);
		}

		pthread_mutex_unlock(&listenerMutexes[channel]);

		while (!tmpMessages->empty()) {

			inMessage = tmpMessages->front();
			tmpMessages->pop_front();

			Partition<int> partition;
			Serialization ser;

			ser.deserialize(inMessage, &partition);
			for (int j = 0; j < partition.size(); j++) {
				partition.set(func->calculate(partition.get(j)), j);
			}

			// Replicate data to all subsequent vertices, do not actually reshard the data here
			int n = 0;
			for (vector<Vertex*>::iterator v = next.begin(); v != next.end();
					++v) {

				outMessage = new Message();
				ser.serialize(&partition, outMessage);

				int idx = n * worldSize + rank; // always keep workload on same rank for Map

				if (PIPELINE) {

					// Pipeline mode: immediately copy message into next operator's queue
					pthread_mutex_lock(&(*v)->listenerMutexes[idx]);
					(*v)->inMessages[idx].push_back(outMessage);

					cout << "MAP->PIPELINE MESSAGE [" << tag << "] #" << c
							<< " @ " << rank << " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessage->size << " CAP "
							<< outMessage->capacity << endl;

					pthread_cond_signal(&(*v)->listenerCondVars[idx]);
					pthread_mutex_unlock(&(*v)->listenerMutexes[idx]);

				} else {

					// Normal mode: synchronize on outgoing message channel & send message
					pthread_mutex_lock(&senderMutexes[idx]);
					outMessages[idx].push_back(outMessage);

					cout << "MAP->PUSHBACK MESSAGE [" << tag << "] #" << c
							<< " @ " << rank << " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessage->size << " CAP "
							<< outMessage->capacity << endl;

					pthread_cond_signal(&senderCondVars[idx]);
					pthread_mutex_unlock(&senderMutexes[idx]);
				}

				n++;
			}

			delete inMessage; // delete incoming message and free memory
			c++;
		}

		tmpMessages->clear();
	}

	delete tmpMessages; // delete temp message buffer
}
