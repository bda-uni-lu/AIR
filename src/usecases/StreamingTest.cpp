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

#include "StreamingTest.hpp"

#include "../batching/Map.hpp"
#include "../collector/BinCollector.hpp"
#include "../connector/BinConnector.hpp"
#include "../function/SquareFunction.hpp"

using namespace std;

StreamingTest::StreamingTest() :
		Dataflow() {

	connector = new BinConnector("../data/INT_64MB.bin", 0, rank, worldSize);
	map1 = new Map(new SquareFunction(), 1, rank, worldSize);
	map2 = new Map(new SquareFunction(), 2, rank, worldSize);
	collector = new BinCollector(3, rank, worldSize);

	// Simple chain
	addLink(connector, map1);
	addLink(map1, map2);
	addLink(map2, collector);

	connector->initialize();
	map1->initialize();
	map2->initialize();
	collector->initialize();
}

StreamingTest::~StreamingTest() {
	delete connector;
	delete map1;
	delete map2;
	delete collector;
}
