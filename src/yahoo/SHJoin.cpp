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
 * SHJoin.cpp
 *
 *  Created on: 13, Dec, 2018
 *      Author: vinu.venugopal
 */

#include "SHJoin.hpp"

#include <iostream>
#include <vector>
#include <cstring>
#include <iterator>
#include <cstring>
#include <string>
#include <sstream>
#include <stdlib.h>
#include <unordered_map>
#include <fstream>

#include "../communication/Message.hpp"
#include "../dataflow/Vertex.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

SHJoin::SHJoin(int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {

	std::ifstream ifile("../data/YSB_data/mapping_10000.txt");
	S_CHECK(if (rank == 0) {
		datafile.open("Data/mappings.tsv");
	})

	for (std::string line; getline(ifile, line);) {
		map.insert(
				std::make_pair(line.substr(1, 36) + "\0", line.substr(40, 36)));

		S_CHECK(if (rank == 0) {
			datafile << line.substr(1, 36) << "\t"
					<< std::stol(line.substr(40, 36), nullptr, 16)
					<< endl;

		})
	}

	D(cout << "SHJOIN [" << tag << "] CREATED @ " << rank << endl;)
}

SHJoin::~SHJoin() {
	D(cout << "SHJOIN [" << tag << "] DELETED @ " << rank << endl;)
}

void SHJoin::batchProcess() {
	D(cout << "SHJOIN->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

void SHJoin::streamProcess(int channel) {

	D(cout << "SHJOIN->STREAMPROCESS [" << tag << "] @ " << rank << " IN-CHANNEL "
			<< channel << endl;)

	Message* inMessage, *outMessage;
	list<Message*>* tmpMessages = new list<Message*>();
	Serialization sede;

	adToCampaignHMap::iterator it;

	//WrapperUnit wrapper_unit;
	EventFT eventFT;
	EventJ eventJ;

	int c = 0;

	while (ALIVE) {

		pthread_mutex_lock(&listenerMutexes[channel]);

		while (inMessages[channel].empty())
			pthread_cond_wait(&listenerCondVars[channel],
					&listenerMutexes[channel]);

//		if(inMessages[channel].size()>1)
//				  cout<<tag<<" CHANNEL-"<<channel<<" BUFFER SIZE:"<<inMessages[channel].size()<<endl;


		while (!inMessages[channel].empty()) {
			inMessage = inMessages[channel].front();
			inMessages[channel].pop_front();
			tmpMessages->push_back(inMessage);
		}

		pthread_mutex_unlock(&listenerMutexes[channel]);

		while (!tmpMessages->empty()) {

			inMessage = tmpMessages->front();
			tmpMessages->pop_front();

			D(cout << "SHJOIN->POP MESSAGE: TAG [" << tag << "] @ " << rank
					<< " CHANNEL " << channel << " BUFFER " << inMessage->size
					<< endl;)

			sede.unwrap(inMessage);
			//if (inMessage->wrapper_length > 0) {
			//	sede.unwrapFirstWU(inMessage, &wrapper_unit);
			//	sede.printWrapper(&wrapper_unit);
			//}

			int offset = sizeof(int)
					+ (inMessage->wrapper_length * sizeof(WrapperUnit));

			outMessage = new Message(inMessage->size - offset,
					inMessage->wrapper_length); // create new message with max. required capacity
			memcpy(outMessage->buffer, inMessage->buffer, offset); // simply copy header from old message for now!
			outMessage->size += offset;

			int event_count = (inMessage->size - offset) / sizeof(EventFT);

			D(cout << "EVENT_COUNT: " << event_count << endl;)

			int i = 0, j = 0;
			while (i < event_count) {

				sede.YSBdeserializeFT(inMessage, &eventFT,
						offset + (i * sizeof(EventFT)));

				if ((it = map.find(eventFT.ad_id)) != map.end()) {

					//cout << "  " << i << "\tevent_time: " << eventFT.event_time
					//		<< "\tad_id: " << eventFT.ad_id << "\tc_id: "
					//		<< it->second << endl;

					eventJ.event_time = eventFT.event_time;
					memcpy(eventJ.c_id, it->second.c_str(), 37);
					sede.YSBserializeJ(&eventJ, outMessage); // store joined events directly in outgoing message!
					//sede.YSBdeserializeJ(outMessage, &eventJ,
					//		outMessage->size - sizeof(EventJ));
					//sede.YSBprintJ(&eventJ);
					j++;
				}

				i++;
			}

			D(cout << "W_ID: " << eventFT.event_time /AGG_WIND_SPAN
					<<" RANK: "<<rank<<" TAG: "<<tag
					<< "\n-----"<< endl;)

			// Replicate data to all subsequent vertices, do not actually reshard the data here
			int n = 0, listenerBuffer_offset = 0;
			for (vector<Vertex*>::iterator v = next.begin(); v != next.end();
					++v) {

				int idx = n * worldSize + rank; // always keep workload on same rank

				if (PIPELINE) {
					idx = rank; // calculating the index of the buffer at the listener thread
					// Pipeline mode: immediately copy message into next operator's queue
					pthread_mutex_lock(&(*v)->listenerMutexes[idx]);
					(*v)->inMessages[idx].push_back(outMessage);

					D(cout << "SHJOIN->PIPELINE MESSAGE [" << tag << "] #" << c
							<< " @ " << rank << " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessage->size << " CAP "
							<< outMessage->capacity << endl;)

					pthread_cond_signal(&(*v)->listenerCondVars[idx]);
					pthread_mutex_unlock(&(*v)->listenerMutexes[idx]);

				} else {

					// Normal mode: synchronize on outgoing message channel & send message
					pthread_mutex_lock(&senderMutexes[idx]);
					outMessages[idx].push_back(outMessage);

					D(cout << "SHJOIN->PUSHBACK MESSAGE [" << tag << "] #" << c
							<< " @ " << rank << " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessage->size << " CAP "
							<< outMessage->capacity << endl;)

					pthread_cond_signal(&senderCondVars[idx]);
					pthread_mutex_unlock(&senderMutexes[idx]);
				}

				n++;
				break; // only one successor node allowed!
			}

			delete inMessage;
			c++;
		}

		tmpMessages->clear();
	}

	delete tmpMessages;
}

