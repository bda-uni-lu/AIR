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
 * Vertex.hpp
 *
 *  Created on: Aug 8, 2018
 *  Author: martin.theobald, vinu.venugopal
 *
 *  Note: this class has been completely redesigned for stream processing!
 */

#ifndef DATAFLOW_VERTEX_HPP_
#define DATAFLOW_VERTEX_HPP_

//#define DEBUG

//#define SAN_CHECK

#define TP_LOG

#ifdef DEBUG
	#define D(x) 
#else
	#define D(x)  //x cout<<"No Debugging is enabled!"<<endl;
#endif

#ifdef SAN_CHECK
	#define S_CHECK(x) x
#else
	#define S_CHECK(x) //cout<<"NO SANITY-CHECK ENABLED"<<endl;
#endif

#ifdef TP_LOG
	#define THROUGHPUT_LOG(x) x
#else
	#define THROUGHPUT_LOG(x) //cout<<"NO SANITY-CHECK ENABLED"<<endl;
#endif



#include <list>
#include <vector>

#include "../communication/Message.hpp"
#include "../function/Function.hpp"
#include "../partitioning/Partition.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

static const bool PIPELINE = false; //true;

static const bool SANITY_CHECK = false;

class Vertex;

typedef struct pthread_p {

	Vertex* fromVertex; // dataflow vertex from where incoming message is expected
	Vertex* toVertex; // dataflow vertex to where outgoing message is expected

	int fromRank; // rank from where incoming message is expected
	int toRank; // rank to where outgoing message is expected

	int channel; // communication channel = index in message arrays

} pthread_p;

class Vertex {

public:

	int rank, worldSize, tag, listeningThreadsBatch; // basic MPI parameters

	long BYTES_SENT, BYTES_RECEIVED;

	bool ALIVE;

	pthread_t* listenerThreadsBatch; // listening threads for incoming messages

	pthread_t* listenerThreadsStream; // listening threads for incoming messages
	pthread_t* processorThreadsStream; // processor threads for incoming messages
	pthread_t* senderThreadsStream; // sender threads for outgoing messages

	pthread_p* listenerThreadsParams;
	pthread_p* senderThreadsParams;

	pthread_mutex_t* listenerMutexes;
	pthread_cond_t* listenerCondVars;

	pthread_mutex_t* senderMutexes;
	pthread_cond_t* senderCondVars;

	Message** rMessages; // incoming messages
	Message** sMessages; // outgoing messages

	list<Message*>* inMessages; // incoming message queues (one per channel)
	list<Message*>* outMessages; // outgoing messages queues (one per channel)

	vector<Vertex*> next, previous;

	Vertex(int tag, int rank, int worldSize);

	// Virtual functions - to be overwritten by subclasses

	virtual ~Vertex();

	virtual void batchProcess();

	virtual void streamProcess(int channel);

	// Non-virtual functions - only defined in superclass

	void initialize();

	void startThreadsBatch();

	void joinThreadsBatch();

	void startThreadsStream();

	void joinThreadsStream();

private:

	// Internal thread entry point (receiver, simple batching mode)
	static void* startListenerThreadBatch(void* vertex);

	// Internal thread entry point (receiver, streaming mode)
	static void* startListenerThreadStream(void* params);

	// Internal thread entry point (receiver, streaming mode)
	static void* startProcessorThreadStream(void* params);

	// Internal thread entry point (sender, streaming mode)
	static void* startSenderThreadStream(void* params);
};

#endif /* DATAFLOW_VERTEX_HPP_ */
