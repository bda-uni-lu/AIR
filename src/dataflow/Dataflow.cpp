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
 * Dataflow.cpp
 *
 *  Created on: Nov 27, 2018
 *  Author: martin.theobald, amal.tawakuli
 *
 *  Note: this class has been completely redesigned for stream processing!
 */

#include <algorithm>
#include <iostream>
#include <iterator>
#include <mpi.h>
#include <unistd.h>
#include <vector>

#include "Dataflow.hpp"

using namespace std;

Dataflow::Dataflow() {

	//MPI initialization
	MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &this->level);
	MPI_Comm_size(MPI_COMM_WORLD, &this->worldSize);
	MPI_Comm_rank(MPI_COMM_WORLD, &this->rank);

	this->ALIVE = false;

	if (rank == 0)
		cout << "\n******************AIR (c) 2020 Uni.lu******************\n"
				<< endl;

	D(cout << "DATAFLOW CREATED @ " << rank << " WORLDSIZE " << worldSize << endl;);
}

Dataflow::~Dataflow() {

	// Shutdown MPI
	MPI_Finalize();

	vertices.clear();

	D(cout << "DATAFLOW DELETED @ " << rank << " WORLDSIZE " << worldSize << endl;);
}

void Dataflow::add(Vertex* vertex) {
	if (find(vertices.begin(), vertices.end(), vertex) == vertices.end())
		vertices.push_back(vertex);
}

void Dataflow::addLink(Vertex* parent, Vertex* child) {
	add(parent);
	add(child);
	if (find(parent->next.begin(), parent->next.end(), child)
			== parent->next.end()) {
		parent->next.push_back(child);
	}
	if (find(child->previous.begin(), child->previous.end(), parent)
			== child->previous.end()) {
		child->previous.push_back(parent);
	}
}

void Dataflow::batchProcess() {

	this->ALIVE = true;

	pthread_t threads[vertices.size()];

	// Start a new thread for each root vertex in the DAG
	int i = 0;
	for (vector<Vertex*>::iterator vertex = vertices.begin();
			vertex != vertices.end(); ++vertex) {

		if ((*vertex)->previous.empty()) { // root vertex has no parent
			pthread_create(&threads[i], NULL, &startRootThreadBatch, (*vertex));
			i++;
		}
	}

	for (int j = 0; j < i; j++) {
		pthread_join(threads[j], (void **) NULL);
	}

	this->ALIVE = false;
}

void Dataflow::streamProcess() {

	this->ALIVE = true;

	pthread_t threads[vertices.size()];

	// Start a new thread for each root vertex in the DAG
	int i = 0;
	for (vector<Vertex*>::iterator vertex = vertices.begin();
			vertex != vertices.end(); ++vertex) {

		if ((*vertex)->previous.empty()) { // a root vertex has no parent

			pthread_create(&threads[i], NULL, &startRootThreadStream,
					(*vertex));

			i++;
		}
	}

	for (int j = 0; j < i; j++) {
		pthread_join(threads[j], (void **) NULL);
	}

	this->ALIVE = false;
}

void* Dataflow::startRootThreadBatch(void* vertex) {

	Vertex* currentVertex = (Vertex*) vertex;
	currentVertex->startThreadsBatch();
	currentVertex->batchProcess(); // actual logic is defined only in subclass!
	currentVertex->joinThreadsBatch();

	pthread_exit(NULL);
}

void* Dataflow::startRootThreadStream(void* vertex) {

	Vertex* currentVertex = (Vertex*) vertex;
	currentVertex->startThreadsStream();
	currentVertex->streamProcess(-1); // actual logic is defined only in subclass!
	currentVertex->joinThreadsStream();

	pthread_exit(NULL);
}
