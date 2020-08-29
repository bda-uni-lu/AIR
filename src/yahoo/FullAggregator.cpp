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
 * FullAggregator.cpp
 *
 *  Created on: 17, Dec, 2018
 *      Author: vinu.venugopal
 */

#include "FullAggregator.hpp"

#include <mpi.h>
#include <cstring>
#include <iostream>
#include <list>
#include <unistd.h>

#include "../communication/Message.hpp"
#include "../communication/Window.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

FullAggregator::FullAggregator(int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {
	D(cout << "FULLAGGREGATOR [" << tag << "] CREATED @ " << rank << endl;);
	pthread_mutex_init(&WIDtoIHM_mutex, NULL);
	pthread_mutex_init(&WIDtoWrapperUnit_mutex, NULL);
}

FullAggregator::~FullAggregator() {
	D(cout << "FULLAGGREGATOR [" << tag << "] DELETED @ " << rank << endl;);
}

void FullAggregator::batchProcess() {
	D(cout << "FULLAGGREGATOR->BATCHPROCESS [" << tag << "] @ " << rank << endl;);
}

void FullAggregator::streamProcess(int channel) {

	D(cout << "FULLAGGREGATOR->STREAMPROCESS [" << tag << "] @ " << rank
			<< " IN-CHANNEL " << channel << endl;);

	Message* inMessage, *outMessage;
	list<Message*>* tmpMessages = new list<Message*>();
	Serialization sede;

	WIDtoWrapperUnitHMap::iterator WIDtoWrapperUnit_it;
	OuterHMap::iterator WIDtoIHM_it;
	InnerHMap::iterator CIDtoCountAndMaxEventTime_it;

	WrapperUnit wrapper_unit;
	EventPA eventPA;
	EventPC eventPC;

	int c = 0;
	while (ALIVE) {
//		sleep(10);
		int flag = 0;
		pthread_mutex_lock(&listenerMutexes[channel]);

		while (inMessages[channel].empty())
			pthread_cond_wait(&listenerCondVars[channel],
					&listenerMutexes[channel]);
//		if (inMessages[channel].size() > 1){flag =1;
//			cout << tag << " CHANNEL-" << channel << " BUFFER SIZE:"
//					<< inMessages[channel].size() << endl;}

		while (!inMessages[channel].empty()) {
			inMessage = inMessages[channel].front();
			inMessages[channel].pop_front();
			tmpMessages->push_back(inMessage);
		}

		pthread_mutex_unlock(&listenerMutexes[channel]);

//		if(flag==1)
//		cout<<"CHANNEL-"<<channel<<" BUFFER SIZE:"<<tmpMessages->size()<<endl;

		while (!tmpMessages->empty()) {

			inMessage = tmpMessages->front();
			tmpMessages->pop_front();

			D(cout << "FULLAGGREGATOR->POP MESSAGE: TAG [" << tag << "] @ "
					<< rank << " CHANNEL " << channel << " BUFFER "
					<< inMessage->size << endl);

			long int WID;
			list<long int> completed_windows;

			sede.unwrap(inMessage);
			if (inMessage->wrapper_length > 0) {

				sede.unwrapFirstWU(inMessage, &wrapper_unit);
//				sede.printWrapper(&wrapper_unit);

				WID = wrapper_unit.window_start_time / AGG_WIND_SPAN;

				if (wrapper_unit.completeness_tag_denominator == 1) {
					completed_windows.push_back(WID);
				} else {
					pthread_mutex_lock(&WIDtoWrapperUnit_mutex); //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

					if ((WIDtoWrapperUnit_it = WIDtoWrapperUnit.find(WID))
							!= WIDtoWrapperUnit.end()) {

						WIDtoWrapperUnit_it->second.first =
								WIDtoWrapperUnit_it->second.first
										+ wrapper_unit.completeness_tag_numerator;

						//cout << "____AGGREGATE WRAPPER: " << WID << " NUM="
						//		<< WIDtoWrapperUnit_it->second.first << " DEN="
						//		<< WIDtoWrapperUnit_it->second.second << endl;

						if (WIDtoWrapperUnit_it->second.first
								/ WIDtoWrapperUnit_it->second.second) {
							completed_windows.push_back(WID);
							WIDtoWrapperUnit.erase(WID);
							//cout << "____WID COMPLETE: " << WID << endl;
						}

					} else {

						WIDtoWrapperUnit.emplace(WID,
								make_pair(
										wrapper_unit.completeness_tag_numerator,
										wrapper_unit.completeness_tag_denominator));

					}

					pthread_mutex_unlock(&WIDtoWrapperUnit_mutex);//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
				}
			}

			int offset = sizeof(int)
					+ (inMessage->wrapper_length * sizeof(WrapperUnit));

			outMessage = new Message(sizeof(EventPC) * 100); // create new message with max. required capacity

			int event_count = (inMessage->size - offset) / sizeof(EventPA);
			//cout << "EVENT_COUNT: " << event_count << endl;

			pthread_mutex_lock(&WIDtoIHM_mutex); //===========================================================================

			int i = 0, j = 0, k = 0;
			while (i < event_count) {

				sede.YSBdeserializePA(inMessage, &eventPA,
						offset + (i * sizeof(EventPA)));

				WID = eventPA.max_event_time / AGG_WIND_SPAN;

				if ((WIDtoIHM_it = WIDtoIHM.find(WID)) != WIDtoIHM.end()) {

					if ((CIDtoCountAndMaxEventTime_it =
							WIDtoIHM_it->second.find(eventPA.c_id))
							!= WIDtoIHM_it->second.end()) {

						CIDtoCountAndMaxEventTime_it->second.first =
								CIDtoCountAndMaxEventTime_it->second.first
										+ eventPA.count;

						if (CIDtoCountAndMaxEventTime_it->second.second
								< eventPA.max_event_time) { // new max. event time!
							CIDtoCountAndMaxEventTime_it->second.second =
									eventPA.max_event_time;
						}

					} else { // new entry in inner hashmap!

						WIDtoIHM_it->second.emplace(eventPA.c_id,
								std::make_pair(eventPA.count,
										eventPA.max_event_time));
						k++;
					}

				} else { // new entry in outer hashmap!

					InnerHMap new_CIDtoCountAndMaxEventTime(100);
					new_CIDtoCountAndMaxEventTime.emplace(eventPA.c_id,
							std::make_pair(eventPA.count,
									eventPA.max_event_time));
					WIDtoIHM.emplace(WID, new_CIDtoCountAndMaxEventTime);

					j++;
				}

				i++;
			}

			long int time_now = (long int) (MPI_Wtime() * 1000.0);
			//cout << "\nFULLAGGR TIME_NOW:  " << time_now << endl;
			//printf("FULLAGGR MPI_Wtime: %lf\n", (MPI_Wtime() * 1000.0));

			while (!completed_windows.empty()) {

				WID = completed_windows.front();
				completed_windows.pop_front();

				WIDtoIHM_it = WIDtoIHM.find(WID);
				if (WIDtoIHM_it != WIDtoIHM.end()) {

					j = 0;
					for (CIDtoCountAndMaxEventTime_it =
							WIDtoIHM_it->second.begin();
							CIDtoCountAndMaxEventTime_it
									!= WIDtoIHM_it->second.end();
							CIDtoCountAndMaxEventTime_it++) {

						eventPC.WID = WID;
						eventPC.c_id = CIDtoCountAndMaxEventTime_it->first;
						eventPC.count =
								CIDtoCountAndMaxEventTime_it->second.first;
						eventPC.latency = (time_now - eventPA.max_event_time);

						cout << "  " << j << "\tWID: " << eventPC.WID
								<< "\tc_id: " << eventPC.c_id << "\tcount: "
								<< eventPC.count << "\tlatency: "
								<< eventPC.latency << " SIZE "
								<< outMessage->size << " CAP "
								<< outMessage->capacity << endl;

						sede.YSBserializePC(&eventPC, outMessage);
						//sede.YSBdeserializePC(outMessage, &eventPC,
						//		outMessage->size - sizeof(EventPC));
						//sede.YSBprintPC(&eventPC);
						j++;
					}

					WIDtoIHM_it->second.clear(); // clear inner map
					WIDtoIHM.erase(WID); // remove from outer map
				}
			}

			pthread_mutex_unlock(&WIDtoIHM_mutex); //====================================================================

			// Finally send message to a single collector on rank 0
			int n = 0;
			for (vector<Vertex*>::iterator v = next.begin(); v != next.end();
					++v) {

				if (outMessage->size > 0) {

					int idx = n * worldSize + 0; // send to rank 0 only

					// Normal mode: synchronize on outgoing message channel & send message
					pthread_mutex_lock(&senderMutexes[idx]);
					outMessages[idx].push_back(outMessage);

					D(cout << "FULLAGGREGATOR->PUSHBACK MESSAGE [" << tag << "] #"
							<< c << " @ " << rank << " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessage->size << " CAP "
							<< outMessage->capacity << endl;);

					pthread_cond_signal(&senderCondVars[idx]);
					pthread_mutex_unlock(&senderMutexes[idx]);

				} else {

					delete outMessage;
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
