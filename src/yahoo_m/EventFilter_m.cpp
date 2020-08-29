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
 * EventFilter.cpp
 *
 *  Created on:
 *      Author: vinu.venugopal
 */

#include "EventFilter_m.hpp"

#include <iostream>
#include <vector>
#include <cstring>
#include <iterator>
#include <cstring>
#include <string>
#include <sstream>
#include <unordered_map>

#include "../communication/Message.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

EventFilterM::EventFilterM(int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {
	D(cout << "EVENTFILTER [" << tag << "] CREATED @ " << rank << endl;);
}

EventFilterM::~EventFilterM() {
	D(cout << "EVENTFILTER [" << tag << "] DELETED @ " << rank << endl;);
}

void EventFilterM::batchProcess() {
	cout << "EVENTFILTER->BATCHPROCESS [" << tag << "] @ " << rank << endl;
}

void EventFilterM::streamProcess(int channel) {

	D(cout << "EVENTFILTER->STREAMPROCESS [" << tag << "] @ " << rank
			<< " IN-CHANNEL " << channel << endl;);

	Message* inMessage;//, *outMessageC, *outMessageV;
	Message** outMessagesClickandView = new Message*[2];
	list<Message*>* tmpMessages = new list<Message*>();
	Serialization sede;

	//WrapperUnit wrapper_unit;
	EventDG eventDG;
	EventFT eventFT;

	while (ALIVE) {

		pthread_mutex_lock(&listenerMutexes[channel]);

		while (inMessages[channel].empty())
			pthread_cond_wait(&listenerCondVars[channel],
					&listenerMutexes[channel]);

		while (!inMessages[channel].empty()) {
			inMessage = inMessages[channel].front();
			inMessages[channel].pop_front();
			tmpMessages->push_back(inMessage);
		}

		pthread_mutex_unlock(&listenerMutexes[channel]);

		while (!tmpMessages->empty()) {

			inMessage = tmpMessages->front();
			tmpMessages->pop_front();

			D(cout << "EVENTFILTER->POP MESSAGE: TAG [" << tag << "] @ " << rank
					<< " CHANNEL " << channel << " BUFFER " << inMessage->size
					<< endl;);

			sede.unwrap(inMessage);
			//if (inMessage->wrapper_length > 0) {
			//	sede.unwrapFirstWU(inMessage, &wrapper_unit);
			//	sede.printWrapper(&wrapper_unit);
			//}

			int offset = sizeof(int)
					+ (inMessage->wrapper_length * sizeof(WrapperUnit));

			outMessagesClickandView[0] = new Message(inMessage->size - offset,
					inMessage->wrapper_length); // create new message with max. required capacity

			outMessagesClickandView[1] = new Message(inMessage->size - offset,
					inMessage->wrapper_length);

			memcpy(outMessagesClickandView[0]->buffer, inMessage->buffer, offset); // simply copy header from old message for now!
			outMessagesClickandView[0]->size += offset;

			memcpy(outMessagesClickandView[1]->buffer, inMessage->buffer, offset); // simply copy header from old message for now!
			outMessagesClickandView[1]->size += offset;

			int event_count = (inMessage->size - offset) / sizeof(EventDG);
			D(cout << "EVENT_COUNT: " << event_count << endl;);

			int i = 0, c = 0, v = 0;
			while (i < event_count) {

				sede.YSBdeserializeDG(inMessage, &eventDG,
						offset + (i * sizeof(EventDG)));

				D(cout << "  " << i << "\tevent_time: " << eventDG.event_time
						<< "\tevent_type: " << eventDG.event_type << "\t"
						<< "ad_id: " << eventDG.ad_id << endl;);

				if (strcmp(eventDG.event_type, "click") == 0) { //FILTERING BASED ON EVENT_TYPE
					eventFT.event_time = eventDG.event_time;
					memcpy(eventFT.ad_id, eventDG.ad_id, 37);
					sede.YSBserializeFT(&eventFT, outMessagesClickandView[0]); // store filtered events directly in outgoing message!
//					sede.YSBdeserializeFT(outMessage, &eventFT,
//							outMessage->size - sizeof(EventFT));
//					sede.YSBprintFT(&eventFT);
					c++;
				} else if (strcmp(eventDG.event_type, "view") == 0) { //FILTERING BASED ON EVENT_TYPE
					eventFT.event_time = eventDG.event_time;
					memcpy(eventFT.ad_id, eventDG.ad_id, 37);
					sede.YSBserializeFT(&eventFT, outMessagesClickandView[1]); // store filtered events directly in outgoing message!
					//					sede.YSBdeserializeFT(outMessage, &eventFT,
					//							outMessage->size - sizeof(EventFT));
					//					sede.YSBprintFT(&eventFT);
					v++;
				}

				i++;
			}
			D(cout <<"@"<<rank<<" FILTERED_EVENT_COUNT: " << c <<" click-events, "<<
					v << " view-events" << endl;);

			// Replicate data to all subsequent vertices, do not actually reshard the data here
			// send click-outMessages to idx: 0 * worldSize + rank and
			// send view-outMessages to idx: 1 * worldSize + rank
			int n = 0, listenerBuffer_offset = 0;

			for (vector<Vertex*>::iterator v = next.begin(); v != next.end();
					++v) {

				int idx = n * worldSize + rank; // always keep workload on same rank

				if (PIPELINE) {
					idx = rank;// * worldSize + listenerBuffer_offset; // calculating the index of the buffer at the listener thread!
					// Pipeline mode: immediately copy message into next operator's queue
					pthread_mutex_lock(&(*v)->listenerMutexes[idx]);
					(*v)->inMessages[idx].push_back(outMessagesClickandView[n]);

					D(cout << "EVENTFILTER->PIPELINE MESSAGE [" << tag << "] #"
							<< c << " @ " << rank << " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessagesClickandView[n]->size << " CAP "
							<< outMessagesClickandView[n]->capacity << endl;);

					pthread_cond_signal(&(*v)->listenerCondVars[idx]);
					pthread_mutex_unlock(&(*v)->listenerMutexes[idx]);

				} else {

					// Normal mode: synchronize on outgoing message channel & send message
					pthread_mutex_lock(&senderMutexes[idx]);
					outMessages[idx].push_back(outMessagesClickandView[n]);

					D(cout << "EVENTFILTER->PUSHBACK MESSAGE [" << tag << "] #"
							<< c << " @ " << rank << " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessagesClickandView[n]->size << " CAP "
							<< outMessagesClickandView[n]->capacity << endl;);

					pthread_cond_signal(&senderCondVars[idx]);
					pthread_mutex_unlock(&senderMutexes[idx]);
				}

				n++;
				//break; // only one successor node allowed!
			}

			delete inMessage;
			c++;
		}

		tmpMessages->clear();
	}

	delete tmpMessages;
}
