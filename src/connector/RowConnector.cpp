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
 * RowConnector.cpp
 *
 *  Created on: Jan 3, 2018
 *      Author: martin.theobald
 */

#include "RowConnector.hpp"

#include <cstring>
#include <iostream>
#include <iterator>
#include <vector>

#include "../communication/Message.hpp"
#include "../dataflow/Vertex.hpp"

using namespace std;

RowConnector::RowConnector(Schema* schema, string fileName, int* shardAttr,
		int numShardAttr, int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {
	this->schema = schema;
	this->shardAttr = shardAttr;
	this->numShardAttr = numShardAttr;
	this->window = new Window(DEFAULT_WINDOW_SIZE);
	this->input = new FileInput(fileName, window);
	this->input->open();
	//cout << "ROWCONNECTOR [" << fileName << "] CREATED @ " << rank << endl;
}

RowConnector::~RowConnector() {
	input->close();
	delete input;
	delete window;
	//cout << "ROWCONNECTOR DELETED @ " << rank << endl;
}

void RowConnector::batchProcess() {

	cout << "ROWCONNECTOR->BATCHPROCESS [" << tag << "] " << endl;

	if (rank == 0) { // one rank to connect to data and partition

		Window* window = input->nextWindow();

		for (vector<Vertex*>::iterator nextVertex = next.begin();
				nextVertex != next.end(); ++nextVertex) {

			int pos = 0, hash, i;
			while (pos < window->size) {

				hash = 0;
				for (i = 0; i < numShardAttr; i++)
					hash += (*(int*) (&window->buffer[pos]
							+ schema->offsets[shardAttr[i]]));

				memcpy(
						&sMessages[hash % worldSize]->buffer[sMessages[hash
								% worldSize]->size], &window->buffer[pos],
						schema->size);

				sMessages[hash % worldSize]->size += schema->size;

				pos += schema->size;
			}
		}
	}
}
