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
 * YSB_m.cpp  MODIFIED-YSB
 *
 *  Created on: Feb 11, 2019
 *      Author: vinu.venugopal
 */

#include "YSB_m.hpp"

#include "../yahoo/EventCollector.hpp"
#include "../yahoo_m/EventFilter_m.hpp"
#include "../yahoo/EventGenerator.hpp"
#include "../yahoo_m/FullAggregator_m.hpp"
#include "../yahoo_m/WinJoinYSB_m.hpp"
#include "../yahoo/PartialAggregator.hpp"
#include "../yahoo/SHJoin.hpp"

using namespace std;

YSB_m::YSB_m(unsigned long throughput) :
		Dataflow() {

	generator = new EventGenerator(1, rank, worldSize, throughput);
	filter = new EventFilterM(2, rank, worldSize);
	joinClick = new SHJoin(3, rank, worldSize);
	joinView = new SHJoin(4, rank, worldSize);
	par_aggregateClick = new PartialAggregator(5, rank, worldSize);
	par_aggregateView = new PartialAggregator(6, rank, worldSize);
	full_aggregateClick = new FullAggregatorM(7, rank, worldSize, 1); //last argument denotes event_type click=1 and view=2
	full_aggregateView = new FullAggregatorM(8, rank, worldSize, 2);
	ratioFinder = new WinJoinYSBM(9, rank, worldSize);
//	collector = new EventCollector(6, rank, worldSize);

	addLink(generator, filter);
	addLink(filter, joinClick);
	addLink(filter, joinView);
	addLink(joinClick, par_aggregateClick);
	addLink(joinView, par_aggregateView);
	addLink(par_aggregateClick, full_aggregateClick);
	addLink(par_aggregateView, full_aggregateView);
	addLink(full_aggregateView, ratioFinder);
	addLink(full_aggregateClick, ratioFinder);
//	addLink(full_aggregate, collector);

	generator->initialize();
	filter->initialize();
	joinClick->initialize();
	joinView->initialize();
	par_aggregateClick->initialize();
	par_aggregateView->initialize();
	full_aggregateClick->initialize();
	full_aggregateView->initialize();
	ratioFinder->initialize();
//	collector->initialize();

}

YSB_m::~YSB_m() {

	delete generator;
	delete filter;
	delete joinClick;
	delete joinView;
	delete full_aggregateClick;
	delete full_aggregateView;
	delete ratioFinder;
//	delete full_aggregate;
//	delete collector;

}

