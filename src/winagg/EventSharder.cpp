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
 * EventSharder.cpp
 *
 *  Created on:
 *      Author: vinu.venugopal
 */

#include "EventSharder.hpp"

#include <iostream>
#include <vector>
#include <cstring>
#include <iterator>
#include <string>
#include <sstream>
#include <unordered_map>
#include <mpi.h>
#include <ctime>

#include "../communication/Message.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

EventSharder::EventSharder(int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {
	D(cout << "EVENTSHARDER [" << tag << "] CREATED @ " << rank << endl;)
	THROUGHPUT_LOG(datafile.open("Data/tp_log" + to_string(rank) + ".tsv")
	;
)
}

EventSharder::~EventSharder() {
D(cout << "EVENTSHARDER [" << tag << "] DELETED @ " << rank << endl;)
}

void EventSharder::batchProcess() {
D(cout << "EVENTSHARDER->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

void EventSharder::streamProcess(int channel) {

D(cout << "EVENTSHARDER->STREAMPROCESS [" << tag << "] @ " << rank
	<< " IN-CHANNEL " << channel << endl;)

Message* inMessage, *outMessage;
Message** outMessagesToWindowIDs = new Message*[worldSize]; //for sharding based on WID
list<Message*>* tmpMessages = new list<Message*>();
Serialization sede;

EventDG eventDG; //for incoming events
IdCount idcnt; // for outgoing events

int windowSpan = 10000; // local value for now!

int c = 0;

while (ALIVE) {

pthread_mutex_lock(&listenerMutexes[channel]);

while (inMessages[channel].empty())
	pthread_cond_wait(&listenerCondVars[channel], &listenerMutexes[channel]);

//if (inMessages[channel].size() > 1)
//	cout << tag << " CHANNEL-" << channel << " BUFFER SIZE:"
//			<< inMessages[channel].size() << endl;

while (!inMessages[channel].empty()) {
	inMessage = inMessages[channel].front();
	inMessages[channel].pop_front();
	tmpMessages->push_back(inMessage);
}

pthread_mutex_unlock(&listenerMutexes[channel]);

while (!tmpMessages->empty()) {

	inMessage = tmpMessages->front();
	tmpMessages->pop_front();

	D(cout << "EVENTSHARDER->POP MESSAGE: TAG [" << tag << "] @ " << rank
			<< " CHANNEL " << channel << " BUFFER " << inMessage->size
			<< endl;)

	sede.unwrap(inMessage);
	//if (inMessage->wrapper_length > 0) {
	//	sede.unwrapFirstWU(inMessage, &wrapper_unit);
	//	sede.printWrapper(&wrapper_unit);
	//}

	int offset = sizeof(int)
			+ (inMessage->wrapper_length * sizeof(WrapperUnit));

	// Caution: copying the header from the inMessage only works if there is only one wrapper and one window per message!
	for (int w = 0; w < worldSize; w++) {
		outMessagesToWindowIDs[w] = new Message(inMessage->size - offset,
				inMessage->wrapper_length); // create new message with max. required capacity
		memcpy(outMessagesToWindowIDs[w]->buffer, inMessage->buffer, offset); // simply copy header from old message for now!
		outMessagesToWindowIDs[w]->size += offset;
	}

	int event_count = (inMessage->size - offset) / sizeof(EventDG);

D(cout << "THROUGHPUT: " << event_count <<" @RANK-"<<rank<<" TIME: "<<(long int)MPI_Wtime()<< endl;)

									THROUGHPUT_LOG(
			datafile << event_count << "\t" << rank << "\t"
					<< (long int )MPI_Wtime() << endl
			;
	)

	int i = 0;
	idcnt.count = 0;
	idcnt.max_event_time = 0;

	while (i < event_count) {

		sede.YSBdeserializeDG(inMessage, &eventDG,
				offset + (i * sizeof(EventDG)));

//				cout << "  " << i << "\tevent_time: " << eventDG.event_time
//						<< "\tevent_type: " << eventDG.event_type << "\t"
//						<< "ad_id: " << eventDG.ad_id << endl;

		/*you can have a hashmap here to keep track of the count per window (where there is multiple window information in a given message),
		 however it is not the case here!*/
		idcnt.max_event_time = (
				eventDG.event_time > idcnt.max_event_time ?
						eventDG.event_time : idcnt.max_event_time);
		idcnt.count = idcnt.count + 1; // we could explicitly use the event_count value here -- but, for a fair comparison with other implementations we preferred to parse the entire message!

		i++;
	}

	long int WID = idcnt.max_event_time / windowSpan;

//	sede.YSBprintIdCnt(&idcnt);

	sede.YSBserializeIdCnt(&idcnt,
			outMessagesToWindowIDs[WID % worldSize]);

//	cout << "outMessagesToWindowIDs[WID % worldSize]->size: "
//			<< outMessagesToWindowIDs[WID % worldSize]->size << endl;

	int n = 0;
	for (vector<Vertex*>::iterator v = next.begin(); v != next.end(); ++v) {

		for (int w = 0; w < worldSize; w++) {

			int idx = n * worldSize + w; // iterate over all ranks

			if (outMessagesToWindowIDs[w]->size > 20) { // quick fix for now

				// Normal mode: synchronize on outgoing message channel & send message
				pthread_mutex_lock(&senderMutexes[idx]);
				outMessages[idx].push_back(outMessagesToWindowIDs[w]);

				D(cout << "EVENTSHARDER->PUSHBACK MESSAGE [" << tag
						<< "] #" << c << " @ " << rank << " IN-CHANNEL "
						<< channel << " OUT-CHANNEL " << idx << " SIZE "
						<< outMessagesToWindowIDs[w]->size << " CAP "
						<< outMessagesToWindowIDs[w]->capacity << endl);

				pthread_cond_signal(&senderCondVars[idx]);
				pthread_mutex_unlock(&senderMutexes[idx]);

			} else {

				delete outMessagesToWindowIDs[w];
			}
		}

		n++;
	}

	delete inMessage;

	c++;
}

tmpMessages->clear();
}

delete tmpMessages;
}
