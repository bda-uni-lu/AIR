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
 * Dataflow.hpp
 *
 *  Created on: Nov 27, 2018
 *      Author: martin.theobald, amal.tawakuli
 *
 *  Note: this class has been completely redesigned for stream processing!
 */

#include "Vertex.hpp"

using namespace std;

#ifndef DATAFLOW_DATAFLOW_HPP_
#define DATAFLOW_DATAFLOW_HPP_

class Dataflow {

public:

	bool ALIVE;

	int level, rank, worldSize;

	vector<Vertex*> vertices;

	Dataflow();

	virtual ~Dataflow();

	void add(Vertex* vertex);

	void addLink(Vertex* parent, Vertex* child);

	virtual void batchProcess();

	virtual void streamProcess();

private:

	// Internal root thread entry point
	static void* startRootThreadBatch(void* vertex);

	// Internal root thread entry point
	static void* startRootThreadStream(void* vertex);

};

#endif /* DATAFLOW_DATAFLOW_HPP_ */
