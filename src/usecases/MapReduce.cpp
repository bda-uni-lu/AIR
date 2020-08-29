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
 * MapReduce.cpp
 *
 *  Created on: Nov 27, 2017
 *      Author: martin.theobald, amal.tawakuli
 */

#include "MapReduce.hpp"

#include "../batching/Map.hpp"
#include "../batching/Reduce.hpp"
#include "../connector/BinConnector.hpp"
#include "../dataflow/Vertex.hpp"
#include "../function/SquareFunction.hpp"
#include "../function/SumFunction.hpp"

using namespace std;

MapReduce::MapReduce() :
		Dataflow() {

	connector = new BinConnector("../data/INT_64MB.bin", 0, rank, worldSize);
	mapper = new Map(new SquareFunction(), 1, rank, worldSize);
	reducer = new Reduce(new SumFunction(), 2, rank, worldSize);

	// Simple chain
	addLink(connector, mapper);
	addLink(mapper, reducer);

	connector->initialize();
	mapper->initialize();
	reducer->initialize();
}

MapReduce::~MapReduce() {
	delete connector;
	delete mapper;
	delete reducer;
}
