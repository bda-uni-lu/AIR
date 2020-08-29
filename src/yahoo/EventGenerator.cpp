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
 * EventGenerator.cpp
 *
 *  Created on: Dec 06, 2018
 *      Author: vinu.venugopal
 */

#include <mpi.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <iterator>
#include <list>
#include <string>
#include <vector>

#include "../communication/Window.hpp"
#include "EventGenerator.hpp"

using namespace std;

EventGenerator::EventGenerator(int tag, int rank, int worldSize,
		unsigned long tp) :
		Vertex(tag, rank, worldSize) {
	this->throughput = tp;
	std::ifstream ifile("../data/YSB_data/ad_ids.txt");
	for (std::string line; getline(ifile, line);) {
		ad_ids.push_back(line);
	}

	cout << "AIR INSTANCE AT RANK " << (rank + 1) << "/" << worldSize << " | TP: " << throughput << " | MSG/SEC/RANK: " << PER_SEC_MSG_COUNT << " | AGGR_WINDOW: " << AGG_WIND_SPAN << "ms" << endl;

	S_CHECK(
			datafile.open("Data/data" + to_string(rank) + ".tsv");
	)

	D(cout << "EVENTGENERATOR [" << tag << "] CREATED @ " << rank << endl;)
}

EventGenerator::~EventGenerator() {
	D(cout << "EVENTGENERATOR [" << tag << "] DELTETED @ " << rank << endl;)
}

void EventGenerator::batchProcess() {
	D(
			cout << "EVENTGENERATOR->BATCHPROCESS: TAG [" << tag << "] @ "
			<< rank << endl
			;)
}

void EventGenerator::streamProcess(int channel) {

	D(
			cout << "EVENTGENERATOR->STREAMPROCESS: TAG [" << tag << "] @ "
			<< rank << " CHANNEL " << channel << endl
			;)

	Message* message;
	Message** outMessagesPerSec = new Message*[PER_SEC_MSG_COUNT];

	WrapperUnit wrapper_unit;
	EventDG eventDG;

	int wrappers_per_msg = 1; // currently only one wrapper per message!
	int events_per_msg = this->throughput / PER_SEC_MSG_COUNT / worldSize;

	long int start_time = (long int) MPI_Wtime();
	long int t1, t2;

	int iteration_count = 0, c = 0;

	while (ALIVE) {

//		cout << "\n_________Iteration: " << iteration_count
//				<< ", Local-throughput: " << (THROUGHPUT / worldSize)
//				<< ", Start-time: " << (start_time * 1000)
//				<< ", Per-Sec-Msg-Count: " << PER_SEC_MSG_COUNT << endl;

		t1 = MPI_Wtime();

		int msg_count = 0;
		while (msg_count < PER_SEC_MSG_COUNT) {

			outMessagesPerSec[msg_count] = new Message(
					events_per_msg * sizeof(EventDG), wrappers_per_msg);

			// Message header
			long int time_now = (start_time + iteration_count) * 1000;
			wrapper_unit.window_start_time = time_now + 999; // this is the max-event-end-time
			wrapper_unit.completeness_tag_numerator = 1;
			wrapper_unit.completeness_tag_denominator = PER_SEC_MSG_COUNT
					* worldSize * AGG_WIND_SPAN / 1000;

			memcpy(outMessagesPerSec[msg_count]->buffer, &wrappers_per_msg,
					sizeof(int));
			memcpy(outMessagesPerSec[msg_count]->buffer + sizeof(int),
					&wrapper_unit, sizeof(WrapperUnit));
			outMessagesPerSec[msg_count]->size += sizeof(int)
					+ outMessagesPerSec[msg_count]->wrapper_length
							* sizeof(WrapperUnit);

			// Message body
			getNextMessage(&eventDG, &wrapper_unit,
					outMessagesPerSec[msg_count], events_per_msg, time_now);

			// Debug output ---
			Serialization sede;
//			WrapperUnit wu;
//			sede.unwrapFirstWU(message, &wu); //index starts from one
//			sede.printWrapper(&wu);
//

			for (int e = 0; e < events_per_msg; e++) {
				sede.YSBdeserializeDG(outMessagesPerSec[msg_count], &eventDG,
						sizeof(int)
								+ (outMessagesPerSec[msg_count]->wrapper_length
										* sizeof(WrapperUnit))
								+ (e * sizeof(EventDG)));
				//sede.YSBprintDG(&eventDG);
			}
		D(	cout << "message_size: " << outMessagesPerSec[msg_count]->size
					<< "\tmessage_capacity: "
					<< outMessagesPerSec[msg_count]->capacity << endl;)
			// ----

			msg_count++;
			c++;
		}

		t2 = MPI_Wtime();
		while ((t2 - t1) < 1) {
			usleep(100);
			t2 = MPI_Wtime();
		}

		msg_count = 0;
		while (msg_count < PER_SEC_MSG_COUNT) {
			// Replicate data to all subsequent vertices, do not actually reshard the data here
			int n = 0;
			for (vector<Vertex*>::iterator v = next.begin(); v != next.end();
					++v) {

				int idx = n * worldSize + rank; // always keep workload on same rank

				if (PIPELINE) {

					// Pipeline mode: immediately copy message into next operator's queue
					pthread_mutex_lock(&(*v)->listenerMutexes[idx]);
					(*v)->inMessages[idx].push_back(
							outMessagesPerSec[msg_count]);

					D(
							cout << "EVENTGENERATOR->PIPELINE MESSAGE [" << tag
							<< "] #" << c << " @ " << rank
							<< " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessagesPerSec[msg_count]->size << " CAP "
							<< outMessagesPerSec[msg_count]->capacity << endl
							;)

					pthread_cond_signal(&(*v)->listenerCondVars[idx]);
					pthread_mutex_unlock(&(*v)->listenerMutexes[idx]);

				} else {

					// Normal mode: synchronize on outgoing message channel & send message
					pthread_mutex_lock(&senderMutexes[idx]);
					outMessages[idx].push_back(outMessagesPerSec[msg_count]);

					D(
							cout << "EVENTGENERATOR->PUSHBACK MESSAGE [" << tag
							<< "] #" << c << " @ " << rank
							<< " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessagesPerSec[msg_count]->size << " CAP "
							<< outMessagesPerSec[msg_count]->capacity << endl
							;)

					pthread_cond_signal(&senderCondVars[idx]);
					pthread_mutex_unlock(&senderMutexes[idx]);
				}

				n++;
				break; // only one successor node allowed!
			}

			msg_count++;
		}

		iteration_count++;
	}
}

void EventGenerator::getNextMessage(EventDG* event, WrapperUnit* wrapper_unit,
		Message* message, int events_per_msg, long int time_now) {

	Serialization sede;

	memcpy(event->ad_id, "3192274f-32f1-442b-8fc0-d5491664a447\0", 37); //default ad_id that would be replaced later
	memcpy(event->userid_pageid_ipaddress,
			"7ad5154e-b296-4b07-9cb8-15bb6a395b2f,328df5ff-0e4a-4f8e-b3ea-5c35d6a3fb3b,1.2.3.4\0",
			82); //default values that would be used for all the events

	long int max_time = 0;

	// Serializing the events
	int i = 0;
	while (i < events_per_msg) {

		memcpy(event->ad_id, ad_ids[myrandom(0, 999)].c_str(), 36);
		event->event_time = time_now + (999 - i % 1000); // uniformly distribute event times among current message window, upper first
		//event->event_time = (long int) (MPI_Wtime() * 1000);

		int rand_val = myrandom(0, 2);

		//memcpy(event->event_type, eventtypes[rand_val].c_str(), strlen(eventtypes[rand_val].c_str()));
		strcpy(event->event_type, eventtypes[rand_val].c_str());

		S_CHECK(
			datafile << event->event_time << "\t"
					//divide this by the agg wid size
					<< event->event_time / AGG_WIND_SPAN << "\t"
					//divide this by the agg wid size
					<< rank << "\t" << i << "\t" << event->event_type << "\t"
					<< event->ad_id << endl;
		);

		sede.YSBserializeDG(event, message);

		if (max_time < event->event_time)
			max_time = event->event_time;

		i++;
	}

	wrapper_unit->window_start_time = max_time;
}

int EventGenerator::myrandom(int min, int max) { //range : [min, max)
	static bool first = true;
	if (first) {
		srand(time(NULL));
		first = false;
	}
	return min + rand() % ((max + 1) - min);
}
