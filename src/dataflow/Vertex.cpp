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
 * Vertex.cpp
 *
 *  Created on: Aug 8, 2018
 *  Author: martin.theobald, vinu.venugopal
 *
 *  Note: this class has been completely redesigned for stream processing!
 */

#include "Vertex.hpp"

#include <mpi.h>
#include <time.h>
#include <iostream>
#include <iterator>
#include <stdio.h>
#include <cstring>

Vertex::Vertex(int tag, int rank, int worldSize) {

	// Basic MPI parameters
	this->rank = rank;
	this->worldSize = worldSize;
	this->tag = tag;

	this->rMessages = nullptr;
	this->sMessages = nullptr;

	this->inMessages = nullptr;
	this->outMessages = nullptr;

	this->listenerThreadsBatch = nullptr;

	this->listenerThreadsStream = nullptr;
	this->processorThreadsStream = nullptr;
	this->senderThreadsStream = nullptr;

	this->listenerThreadsParams = nullptr;
	this->senderThreadsParams = nullptr;

	this->listenerMutexes = nullptr;
	this->listenerCondVars = nullptr;

	this->senderMutexes = nullptr;
	this->senderCondVars = nullptr;

	this->listeningThreadsBatch = 0;

	this->ALIVE = false;
	this->BYTES_SENT = 0;
	this->BYTES_RECEIVED = 0;

	//cout << "VERTEX [" << tag << "] CREATED @ " << rank << endl;
}

Vertex::~Vertex() {

	for (int i = 0; i < previous.size() * worldSize; i++) {
		delete rMessages[i];
		for (list<Message*>::iterator m = inMessages[i].begin();
				m != inMessages[i].end(); ++m)
			delete (*m);
		inMessages[i].clear();
	}

	for (int i = 0; i < next.size() * worldSize; i++) {
		delete sMessages[i];
		for (list<Message*>::iterator m = outMessages[i].begin();
				m != outMessages[i].end(); ++m)
			delete (*m);
		outMessages[i].clear();
	}

	delete[] rMessages;
	delete[] sMessages;

	delete[] listenerThreadsBatch;

	delete[] inMessages;
	delete[] outMessages;

	delete[] listenerThreadsStream;
	delete[] processorThreadsStream;
	delete[] senderThreadsStream;

	delete[] listenerThreadsParams;
	delete[] senderThreadsParams;

	delete[] listenerMutexes;
	delete[] listenerCondVars;

	delete[] senderMutexes;
	delete[] senderCondVars;

	//cout << "VERTEX [" << tag << "] DELETED @ " << rank << endl;
}

void Vertex::initialize() {

	inMessages = new list<Message*> [previous.size() * worldSize];
	outMessages = new list<Message*> [next.size() * worldSize];

	listenerThreadsBatch = new pthread_t[next.size()]; // batch mode setting

	listenerThreadsStream = new pthread_t[previous.size() * worldSize];
	listenerThreadsParams = new pthread_p[previous.size() * worldSize];

	processorThreadsStream = new pthread_t[previous.size() * worldSize];

	listenerMutexes = new pthread_mutex_t[previous.size() * worldSize];
	listenerCondVars = new pthread_cond_t[previous.size() * worldSize];

	rMessages = new Message*[previous.size() * worldSize];
	for (int i = 0; i < previous.size() * worldSize; i++) {
		rMessages[i] = new Message();
		listenerMutexes[i] = PTHREAD_MUTEX_INITIALIZER;
		listenerCondVars[i] = PTHREAD_COND_INITIALIZER;
	}

	senderThreadsStream = new pthread_t[next.size() * worldSize];
	senderThreadsParams = new pthread_p[next.size() * worldSize];

	senderMutexes = new pthread_mutex_t[next.size() * worldSize];
	senderCondVars = new pthread_cond_t[next.size() * worldSize];

	sMessages = new Message*[next.size() * worldSize];
	for (int i = 0; i < next.size() * worldSize; i++) {
		sMessages[i] = new Message();
		senderMutexes[i] = PTHREAD_MUTEX_INITIALIZER;
		senderCondVars[i] = PTHREAD_COND_INITIALIZER;
	}
}

void Vertex::batchProcess() {
	cout << "WARNING: CALL TO GENERIC VERTEX.BATCHPROCESS!" << endl;
}

void Vertex::streamProcess(int channel) {
	cout << "WARNING: CALL TO GENERIC VERTEX.STREAMPROCESS!" << endl;
}

void Vertex::startThreadsBatch() {

	int i = 0;
	for (vector<Vertex*>::iterator v = next.begin(); v != next.end(); ++v) {
		pthread_create(&listenerThreadsBatch[i], NULL,
				&startListenerThreadBatch, (void*) (*v));
		i++;
	}
}

void Vertex::joinThreadsBatch() {

	MPI_Request reqs[next.size() * worldSize];
	MPI_Status stats[next.size() * worldSize];

	int p = 0;
	for (vector<Vertex*>::iterator v = next.begin(); v != next.end(); ++v) {

		for (int i = 0; i < worldSize; i++) {

			// create unique message tag!
			unsigned int mTag = this->tag;
			mTag = (mTag << 8) + (*v)->tag;
			mTag = (mTag << 8) + i;

			int idx = p * worldSize + i;

			MPI_Isend(sMessages[idx]->buffer, sMessages[idx]->size, MPI_CHAR, i,
					mTag, MPI_COMM_WORLD, &reqs[idx]);

			BYTES_SENT += sMessages[idx]->size;
		}

		p++;
	}

	// Wait for all messages to be sent before clearing them for the next batch
	MPI_Waitall(next.size() * worldSize, reqs, stats);

	for (int p = 0; p < next.size(); p++) {

		for (int i = 0; i < worldSize; i++) {
			int idx = p * worldSize + i;
			sMessages[idx]->clear();
		}
	}

	for (int i = 0; i < next.size(); i++) {
		pthread_join(listenerThreadsBatch[i], (void **) NULL);
	}

	listeningThreadsBatch = 0;
}

void* Vertex::startListenerThreadBatch(void* vertex) {

	Vertex* currentVertex = (Vertex*) vertex;

	if (currentVertex->listeningThreadsBatch++ == 0) {

		Message** rMessages = currentVertex->rMessages;

		int p = 0;
		for (vector<Vertex*>::iterator v = currentVertex->previous.begin();
				v != currentVertex->previous.end(); ++v) {

			MPI_Status status;

			for (int i = 0; i < currentVertex->worldSize; i++) {

				// receive message only with matching tag!
				unsigned int mTag = (*v)->tag;
				mTag = (mTag << 8) + currentVertex->tag;
				mTag = (mTag << 8) + currentVertex->rank;

				int idx = p * currentVertex->worldSize + i;

				MPI_Probe(i, mTag, MPI_COMM_WORLD, &status);
				MPI_Get_count(&status, MPI_CHAR, &rMessages[idx]->size);
				MPI_Recv(rMessages[idx]->buffer, rMessages[idx]->size, MPI_CHAR,
						status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD,
						&status);

				currentVertex->BYTES_RECEIVED += rMessages[idx]->size;
			}

			p++;
		}

		currentVertex->startThreadsBatch();
		currentVertex->batchProcess();
		currentVertex->joinThreadsBatch();
	}

	// Final exit from execution thread (batching mode)
	pthread_exit(NULL);
}

void Vertex::startThreadsStream() {
if(this->ALIVE == false){

	this->ALIVE = true;

	int p = 0;
	for (vector<Vertex*>::iterator v = previous.begin(); v != previous.end();
			++v) {

		for (int i = 0; i < worldSize; i++) {

			int idx = p * worldSize + i;

			listenerThreadsParams[idx].fromVertex = (*v);
			listenerThreadsParams[idx].toVertex = this;
			listenerThreadsParams[idx].fromRank = i;
			listenerThreadsParams[idx].toRank = rank;
			listenerThreadsParams[idx].channel = idx;

			pthread_create(&listenerThreadsStream[idx], NULL,
					&startListenerThreadStream,
					(void*) &listenerThreadsParams[idx]);

			pthread_create(&processorThreadsStream[idx], NULL,
					&startProcessorThreadStream,
					(void*) &listenerThreadsParams[idx]);
		}

		p++;
	}

	int n = 0;
	for (vector<Vertex*>::iterator v = next.begin(); v != next.end(); ++v) {

		for (int i = 0; i < worldSize; i++) {

			int idx = n * worldSize + i;

			senderThreadsParams[idx].fromVertex = this;
			senderThreadsParams[idx].toVertex = (*v);
			senderThreadsParams[idx].fromRank = rank;
			senderThreadsParams[idx].toRank = i;
			senderThreadsParams[idx].channel = idx;

			pthread_create(&senderThreadsStream[idx], NULL,
					&startSenderThreadStream,
					(void*) &senderThreadsParams[idx]);

		}

		(*v)->startThreadsStream();

		n++;
	}
}
}

void Vertex::joinThreadsStream() {

	int p = 0;
	for (vector<Vertex*>::iterator v = previous.begin(); v != previous.end();
			++v) {

		for (int i = 0; i < worldSize; i++) {

			int idx = p * worldSize + i;
			pthread_join(listenerThreadsStream[idx], (void **) NULL);
			//pthread_cancel(listenerThreadsStream[idx]);
			pthread_join(processorThreadsStream[idx], (void **) NULL);
			//pthread_cancel(processorThreadsStream[idx]);
		}

		p++;
	}

	int n = 0;
	for (vector<Vertex*>::iterator v = next.begin(); v != next.end(); ++v) {

		(*v)->joinThreadsStream();

		for (int i = 0; i < worldSize; i++) {

			int idx = n * worldSize + i;
			pthread_join(senderThreadsStream[idx], (void **) NULL);
			//pthread_cancel(senderThreadsStream[idx]);
		}

		n++;
	}

	this->ALIVE = false;
}

void* Vertex::startListenerThreadStream(void* params) {

	Vertex* fromVertex = ((pthread_p*) params)->fromVertex;
	Vertex* toVertex = ((pthread_p*) params)->toVertex;

	int fromRank = ((pthread_p*) params)->fromRank;
	int toRank = ((pthread_p*) params)->toRank;
	int channel = ((pthread_p*) params)->channel;

	// receive messages only with matching tag
	unsigned int mTag = fromVertex->tag;
	mTag = (mTag << 8) + toVertex->tag;
	mTag = (mTag << 8) + toRank;

	MPI_Status status;
	Message* inMessage;

	int c = 0;
	bool doListen = true;
	while (doListen) {

		inMessage = new Message();

//		cout << "WAIT MESSAGE [" << toVertex->tag << "] #" << c << " @ "
//				<< toVertex->rank << " IN-CHANNEL " << channel << " QUEUE "
//				<< toVertex->inMessages[channel].size() << endl;

		MPI_Probe(fromRank, mTag, MPI_COMM_WORLD, &status);
		MPI_Get_count(&status, MPI_CHAR, &inMessage->size);
		MPI_Recv(inMessage->buffer, inMessage->size, MPI_CHAR,
				status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);

		toVertex->BYTES_RECEIVED += inMessage->size;

		//cout << "RECV MESSAGE [" << toVertex->tag << "] #" << c << " @ "
		//		<< toVertex->rank << " IN-CHANNEL " << channel << " SIZE "
		//		<< inMessage->size << endl;

		pthread_mutex_lock(&toVertex->listenerMutexes[channel]);
		toVertex->inMessages[channel].push_back(inMessage);
		pthread_cond_signal(&toVertex->listenerCondVars[channel]);
		pthread_mutex_unlock(&toVertex->listenerMutexes[channel]);

		c++;
	}

	// Final exit from listener thread (streaming mode)
	pthread_exit(NULL);
}

void* Vertex::startProcessorThreadStream(void* params) {

	Vertex* currentVertex = ((pthread_p*) params)->toVertex;
	int channel = ((pthread_p*) params)->channel;

	// Main process function, actual logic is defined only in subclass!
	currentVertex->streamProcess(channel);

	// Final exit from processor thread (streaming mode)
	pthread_exit(NULL);
}

void* Vertex::startSenderThreadStream(void* params) {

	Vertex* fromVertex = ((pthread_p*) params)->fromVertex;
	Vertex* toVertex = ((pthread_p*) params)->toVertex;

	int toRank = ((pthread_p*) params)->toRank;
	int channel = ((pthread_p*) params)->channel;

	// generate message tag
	unsigned int mTag = fromVertex->tag;
	mTag = (mTag << 8) + toVertex->tag;
	mTag = (mTag << 8) + toRank;

	Message* outMessage;
	bool doSend = true;
	while (doSend) {

		pthread_mutex_lock(&fromVertex->senderMutexes[channel]);

		while (fromVertex->outMessages[channel].empty())
			pthread_cond_wait(&fromVertex->senderCondVars[channel],
					&fromVertex->senderMutexes[channel]);

		while (!fromVertex->outMessages[channel].empty()) {
			outMessage = fromVertex->outMessages[channel].front();
			fromVertex->outMessages[channel].pop_front();

			MPI_Send(outMessage->buffer, outMessage->size, MPI_CHAR, toRank,
					mTag,
					MPI_COMM_WORLD);

			fromVertex->BYTES_SENT += outMessage->size;

//					cout << "SENT MESSAGE [" << fromVertex->tag << "] @ "
//							<< fromVertex->rank << " OUT-CHANNEL " << channel
//							<< " SIZE " << outMessage->size << endl;

			delete outMessage;
		}

		pthread_mutex_unlock(&fromVertex->senderMutexes[channel]);
	}

	// Final exit from sender thread (streaming mode)
	pthread_exit(NULL);
}
