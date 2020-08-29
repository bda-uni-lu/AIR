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
 * Serialization.hpp
 *
 *  Created on: Nov 27, 2017
 *      Author: amal.tawakuli, martin.theobald, vinu.venugopal
 */

#ifndef SERIALIZATION_SERIALIZATION_HPP_
#define SERIALIZATION_SERIALIZATION_HPP_

#include "../communication/Message.hpp"
#include "../communication/Window.hpp"
#include "../partitioning/Partition.hpp"

using namespace std;

typedef struct EventDG {
	long int event_time;
//	int event_type; // possible values:  { view = 1, click = 2, purchase = 3}
	char event_type[9];
	char ad_id[37];
	char userid_pageid_ipaddress[82]; // default value: "7ad5154e-b296-4b07-9cb8-15bb6a395b2f, 328df5ff-0e4a-4f8e-b3ea-5c35d6a3fb3b, 1.2.3.4\0"
} EventDG;

typedef struct EventFT {
	long int event_time;
	char ad_id[37];
} EventFT;

typedef struct EventJ {
	long int event_time;
	char c_id[37];
} EventJ;

typedef struct EventPA {
	long int max_event_time;
	long int c_id;
	int count;
} EventPA;

typedef struct EventPC {
	long int WID;
	long int c_id;
	int count;
	int latency;
} EventPC;

typedef struct IdCount {
	long int max_event_time;
	long int count;
} idcnt;

typedef struct EventPC_m {
	long int WID;
	long int c_id;
	int count;
	long int event_time;
	int type;
} EventPC_m;

class Serialization {

public:

	void deserialize(Window* window, Partition<int>* partition);

	void serialize(Partition<int>* partition, Message* message);

	void serialize(Partition<int>* partitions, int numPartitions,
			Message** messages, int numMessages);

	static int decodeInt(char* chars);

	static void encodeInt(char* chars, int val);

	static float decodeFloat(char* chars);

	static void encodeFloat(char* chars, float val);

	//----for FLOW WRAPPING----

	void unwrap(Message* message); //for re-pointing the message variables correctly

	void unwrapFirstWU(Message* message, WrapperUnit* wu); //de-serializing the first wrapper-unit

	void printWrapper(WrapperUnit* wc);

	//----for YSB----

	void YSBserializeDG(EventDG* event, Message* message);

	void YSBdeserializeDG(Message* message, EventDG* event, int offset);

	void YSBprintDG(EventDG* event);

	void YSBserializeFT(EventFT* event, Message* message);

	void YSBdeserializeFT(Message* message, EventFT* event, int offset);

	void YSBprintFT(EventFT* event);

	void YSBserializeJ(EventJ* event, Message* message);

	void YSBdeserializeJ(Message* message, EventJ* event, int offset);

	void YSBprintJ(EventJ* event);

	void YSBserializePA(EventPA* event, Message* message);

	void YSBdeserializePA(Message* message, EventPA* event, int offset);

	void YSBprintPA(EventPA* event);

	void YSBserializePC(EventPC* event, Message* message);

	void YSBdeserializePC(Message* message, EventPC* event, int offset);

	void YSBprintPC(EventPC* event);

	//-----for WIN_AGG use-case------

	void YSBserializeIdCnt(IdCount* event, Message* message);

	void YSBdeserializeIdCnt(Message* message, IdCount* event, int offset);

	void YSBprintIdCnt(IdCount* event);

	//----for YSB* use-case-------

	void YSBserializePC_m(EventPC_m* event, Message* message);

	void YSBdeserializePC_m(Message* message, EventPC_m* event, int offset);

	void YSBprintPC_m(EventPC_m* event);
};

#endif /* SERIALIZATION_SERIALIZATION_HPP_ */
