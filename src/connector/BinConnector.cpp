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
 * BinConnector.cpp
 *
 *  Created on: Dec 20, 2017
 *      Author: martin.theobald, amal.tawakuli
 */

#include "BinConnector.hpp"

//#include <pthread/pthread.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <iterator>
#include <list>
#include <vector>

#include "../communication/Message.hpp"
#include "../communication/Window.hpp"
#include "../input/FileInput.hpp"

using namespace std;

BinConnector::BinConnector(string fileName, int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {
	this->window = new Window(DEFAULT_WINDOW_SIZE);
	this->input = new FileInput(fileName, window); // fixed to files for now
	this->input->open();
	cout << "BINCONNECTOR [" << fileName << "] [" << tag << "] CREATED @ "
			<< rank << endl;
}

BinConnector::~BinConnector() {
	this->input->close();
	delete input;
	delete window;
	cout << "BINCONNECTOR [" << tag << "] DELETED @ " << rank << endl;
}

void BinConnector::batchProcess() {

	cout << "BINCONNECTOR->BATCHPROCESS [" << tag << "] @ " << rank << endl;

	if (rank == 0) { // one rank to connect to data and partition

		if (input->isOpen()) {

			input->nextWindow();

			//  replicate data to all subsequent vertices
			int p = 0;
			for (vector<Vertex*>::iterator v = next.begin(); v != next.end();
					++v) {

				int start = 0, step = (int) window->size / worldSize,
						messageSize = 0;

				for (int i = 0; i < worldSize; i++) {

					// just use very simple sharding for now
					if (start + step < window->size)
						messageSize = step;
					else
						messageSize = window->size - start;

					memcpy(sMessages[p * worldSize + i]->buffer,
							&window->buffer[start], messageSize);
					sMessages[p * worldSize + i]->size = messageSize;

					start += step;
				}

				p++;
			}
		}
	}
}

void BinConnector::streamProcess(int channel) {

	//cout << "BINCONNECTOR->STREAMPROCESS [" << tag << "] @ " << rank
	//		<< " IN-CHANNEL " << channel << endl;

	Message* outMessage;

	if (rank == 0) { // one rank to connect to data and partition

		int c = 0;
		while (input->isOpen()) {

			input->nextWindow();

			// Replicate data to all subsequent vertices
			int n = 0;
			for (vector<Vertex*>::iterator v = next.begin(); v != next.end();
					++v) {

				int start = 0, step = (int) window->size / worldSize,
						messageSize = 0;

				for (int i = 0; i < worldSize; i++) {

					int idx = n * worldSize + i;

					// Use very simple sharding for now (split window into consecutive blocks)
					if (start + step < window->size)
						messageSize = step;
					else
						messageSize = window->size - start;

					outMessage = new Message(messageSize);
					memcpy(outMessage->buffer, &window->buffer[start],
							messageSize);
					outMessage->size = messageSize;
					start += step;

					// Synchronize on outgoing message channel
					pthread_mutex_lock(&senderMutexes[idx]);
					outMessages[idx].push_back(outMessage);

					cout << "BINCONNECTOR->PUSHBACK MESSAGE [" << tag << "] #"
							<< c << " @ " << rank << " OUT-CHANNEL " << idx
							<< " SIZE " << outMessage->size << endl;

					pthread_cond_signal(&senderCondVars[idx]);
					pthread_mutex_unlock(&senderMutexes[idx]);
				}

				n++;

				//sleep(1); // enable this for slow-motion view
			}

			c++;

			//if (c > 20)
			//	return;
		}
	}
}
