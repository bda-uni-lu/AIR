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
 * Serialization.cpp
 *
 *  Created on: Nov 27, 2017
 *      Author: amal.tawakuli, martin.theobald, vinu.venugopal
 */

#include <cstring>
#include <iostream>
#include <string>

#include "Serialization.hpp"

using namespace std;

void Serialization::deserialize(Window* window, Partition<int>* partition) {
	int len = sizeof(int);
	for (int i = 0; i < window->size; i += len) {
		partition->add(decodeInt(&window->buffer[i]));
		//cout << "  DESERIALIZE VALUE: " << decodeInt(&window->buffer[i])
		//		<< endl;
	}
}

void Serialization::serialize(Partition<int>* partition, Message* message) {
	int k = 0, len = sizeof(int);
	for (int i = 0; i < partition->size(); i++) {
		if (k + len <= message->capacity) {
			encodeInt(&message->buffer[k], partition->get(i));
			k += len;
		}
		//cout << "  SERIALIZE VALUE: " << partition->get(i) << endl;
	}
	message->size = k;
}

void Serialization::serialize(Partition<int>* partitions, int numPartitions,
		Message** messages, int numMessages) {
	int k = 0, len = sizeof(int);
	for (int i = 0; i < numPartitions; i++) {
		for (int j = 0; j < partitions[i].size(); j++) {
			if (messages[k]->size + len <= messages[k]->capacity) {
				encodeInt(&messages[k]->buffer[messages[k]->size],
						partitions[i].get(j));
				messages[k]->size += len;
			}
			k++;
			if (k == numMessages)
				k = 0;
			//cout << "  SERIALIZE VALUE: " << partitions[i].getValue(j) << endl;
		}
	}
}

int Serialization::decodeInt(char* chars) {
	int value;
	memcpy(&value, chars, sizeof(int));
	return value;
}

void Serialization::encodeInt(char* chars, int val) {
	memcpy(chars, &val, sizeof(int));
}

float Serialization::decodeFloat(char* chars) {
	float value;
	memcpy(&value, chars, sizeof(float));
	return value;
}

void Serialization::encodeFloat(char* chars, float val) {
	memcpy(chars, &val, sizeof(float));
}

void Serialization::YSBserializeDG(EventDG* event, Message* message) {
	char* b = message->buffer + message->size;
	memcpy(b, &event->event_time, 8);
	b += 8;
	memcpy(b, &event->event_type, 9);
	b +=9;
	memcpy(b, event->ad_id, 37);
	b += 37;
	memcpy(b, event->userid_pageid_ipaddress, 82);
	message->size += sizeof(EventDG);
}

void Serialization::YSBdeserializeDG(Message* message, EventDG* event,
		int offset) {
	char* b = message->buffer + offset;
	memcpy(&event->event_time, b, 8);
	b += 8;
	memcpy(&event->event_type, b, 9);
	b += 9;
	memcpy(event->ad_id, b, 37);
	b += 37;
	memcpy(event->userid_pageid_ipaddress, b, 82);
}

void Serialization::YSBprintDG(EventDG* event) {
	cout << "event_time: " << event->event_time << "\tevent_type: "
			<< event->event_type << "\tad_id: " << event->ad_id
			<< "\tuser_id, page_id, ip_address: "
			<< event->userid_pageid_ipaddress << endl;
}

void Serialization::unwrapFirstWU(Message* message, WrapperUnit* wu) {
	memcpy(&wu->window_start_time, message->buffer + 4, 8);
	memcpy(&wu->completeness_tag_numerator, message->buffer + 12, 4);
	memcpy(&wu->completeness_tag_denominator, message->buffer + 16, 4);
}

void Serialization::unwrap(Message* message) {//re-pointing the message variables to their respective positions
	memcpy(&message->wrapper_length, message->buffer, 4);
}

void Serialization::YSBserializeFT(EventFT* event, Message* message) {
	char* b = message->buffer + message->size;
	memcpy(b, &event->event_time, 8);
	b += 8;
	memcpy(b, event->ad_id, 37);
	message->size += sizeof(EventFT);
}

void Serialization::printWrapper(WrapperUnit* wc) {
	cout << "window_start_time: " << wc->window_start_time
			<< "\tcompletness_tag_numerator: " << wc->completeness_tag_numerator
			<< "\tcompletness_tag_denominator: "
			<< wc->completeness_tag_denominator << endl;
}

void Serialization::YSBdeserializeFT(Message* message, EventFT* event,
		int offset) {
	char* b = message->buffer + offset;
	memcpy(&event->event_time, b, 8);
	b += 8;
	memcpy(event->ad_id, b, 37);
}

void Serialization::YSBprintFT(EventFT* event) {
	cout << "event_time: " << event->event_time << "\tad_id: " << event->ad_id
			<< endl;
}

void Serialization::YSBserializeJ(EventJ* event, Message* message) {
	char* b = message->buffer + message->size;
	memcpy(b, &event->event_time, 8);
	b += 8;
	memcpy(b, event->c_id, 37);
	message->size += sizeof(EventJ);
}

void Serialization::YSBdeserializeJ(Message* message, EventJ* event,
		int offset) {
	char* b = message->buffer + offset;
	memcpy(&event->event_time, b, 8);
	b += 8;
	memcpy(event->c_id, b, 37);
}

void Serialization::YSBprintJ(EventJ* event) {
	cout << "event_time: " << event->event_time << "\tc_id: " << event->c_id
			<< endl;
}

void Serialization::YSBserializePA(EventPA* event, Message* message) {
	char* b = message->buffer + message->size;
	memcpy(b, &event->max_event_time, 8);
	b += 8;
	memcpy(b, &event->c_id, 8);
	b += 8;
	memcpy(b, &event->count, 4);
	message->size += sizeof(EventPA);
}

void Serialization::YSBdeserializePA(Message* message, EventPA* event,
		int offset) {
	char* b = message->buffer + offset;
	memcpy(&event->max_event_time, b, 8);
	b += 8;
	memcpy(&event->c_id, b, 8);
	b += 8;
	memcpy(&event->count, b, 4);
}

void Serialization::YSBprintPA(EventPA* event) {
	cout << "__________________________________________" << endl;
	cout << "max_event_time: " << event->max_event_time << endl;
	cout << "c_id: " << event->c_id << endl;
	cout << "count: " << event->count << endl;
}

void Serialization::YSBserializePC(EventPC* event, Message* message) {
	char* b = message->buffer + message->size;
	memcpy(b, &event->WID, 8);
	b += 8;
	memcpy(b, &event->c_id, 8);
	b += 8;
	memcpy(b, &event->count, 4);
	b += 4;
	memcpy(b, &event->latency, 4);
	message->size += sizeof(EventPC);
}

void Serialization::YSBdeserializePC(Message* message, EventPC* event,
		int offset) {
	char* b = message->buffer + offset;
	memcpy(&event->WID, b, 8);
	b += 8;
	memcpy(&event->c_id, b, 8);
	b += 8;
	memcpy(&event->count, b, 4);
	b += 4;
	memcpy(&event->latency, b, 4);
}

void Serialization::YSBprintPC(EventPC* event) {
	cout << "__________________________________________" << endl;
	cout << "WID: " << event->WID << endl;
	cout << "c_id: " << event->c_id << endl;
	cout << "count: " << event->count << endl;
	cout << "latency: " << event->latency << endl;
}

void Serialization::YSBserializeIdCnt(IdCount* event, Message* message) {
	char* b = message->buffer + message->size;
	memcpy(b, &event->max_event_time, 8);
	b += 8;
	memcpy(b, &event->count, 8);
	message->size += sizeof(IdCount);
}

void Serialization::YSBdeserializeIdCnt(Message* message, IdCount* event,
		int offset) {
	char* b = message->buffer + offset;
	memcpy(&event->max_event_time, b, 8);
	b += 8;
	memcpy(&event->count, b, 8);
}

void Serialization::YSBprintIdCnt(IdCount* event) {
	cout << "Max event time: " << event->max_event_time << "\t count: " << event->count
			<< endl;
}

void Serialization::YSBserializePC_m(EventPC_m* event, Message* message) {
	char* b = message->buffer + message->size;
	memcpy(b, &event->WID, 8);
	b += 8;
	memcpy(b, &event->c_id, 8);
	b += 8;
	memcpy(b, &event->count, 4);
	b += 4;
	memcpy(b, &event->event_time, 8);
	b += 8;
	memcpy(b, &event->type, 4);
	message->size += sizeof(EventPC_m);
}

void Serialization::YSBdeserializePC_m(Message* message, EventPC_m* event,
		int offset) {
	char* b = message->buffer + offset;
	memcpy(&event->WID, b, 8);
	b += 8;
	memcpy(&event->c_id, b, 8);
	b += 8;
	memcpy(&event->count, b, 4);
	b += 4;
	memcpy(&event->event_time, b, 8);
	b += 8;
	memcpy(&event->type, b, 4);
}

void Serialization::YSBprintPC_m(EventPC_m* event) {
	cout << "__________________________________________" << endl;
	cout << "WID: " << event->WID << endl;
	cout << "c_id: " << event->c_id << endl;
	cout << "count: " << event->count << endl;
	cout << "event_time: " << event->event_time << endl;
	cout << "event_type: " << event->type << endl;
}
