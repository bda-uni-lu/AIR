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
 * SortAggregation.hpp
 *
 *  Created on: Dec 26, 2017
 *      Author: martin.theobald
 */

#ifndef RELATIONAL_SORTAGGREGATION_HPP_
#define RELATIONAL_SORTAGGREGATION_HPP_

#include "Aggr.hpp"
#include "Attribute.hpp"
#include "Cond.hpp"
#include "Relation.hpp"
#include "Schema.hpp"

#include "../dataflow/Vertex.hpp"

using namespace std;

class SortAggregation: public Vertex {

public:

	Schema* schema;

	Aggr** aggr;
	Cond** cond;

	int* groupByAttr;
	int* projAttr;
	int* shardAttr;

	int numAggr;
	int numCond;
	int numGroupByAttr;
	int numProjAttr;
	int numShardAttr;

	bool distinct;

	SortAggregation();

	SortAggregation(Schema* schema, Aggr** aggr, Cond** cond, int* groupByAttr,
			int* projAttr, int* shardAttr, int numAggr, int numCond,
			int numGroupByAttr, int numProjAttr, int numShardAttr,
			bool distinct, int tag, int rank, int worldSize);

	~SortAggregation();

	void batchProcess();

	Relation* select(Relation* rel, Cond** cond, int numCond);

	Relation* selectDistinct(Relation* rel, Cond** cond, int numCond);

	Relation* selectProject(Relation* rel, Cond** cond, int* projAttr,
			int numCond, int numProjAttr);

	Relation* selectProjectDistinct(Relation* rel, Cond** cond, int* projAttr,
			int numCond, int numProjAttr);

	Relation* groupBy(Relation* rel, Aggr** aggr, Cond** cond, int* groupByAttr,
			int* projAttr, int numAggr, int numCond, int numGroupByAttr,
			int numProjAttr);

	Relation* groupByAll(Relation* rel, Aggr** aggr, Cond** cond, int numAggr,
			int numCond);

private:

	Relation* processLocal(Relation* rel, Aggr** aggr, Cond** cond,
			int* groupByAttr, int* projAttr, int numAggr, int numCond,
			int numGroupByAttr, int numProjAttr, bool distinct);

	void processDistr(Relation* rel, Aggr** aggr, Cond** cond, int* groupByAttr,
			int* projAttr, int* shardAttr, int numAggr, int numCond,
			int numGroupByAttr, int numProjAttr, int numShardAttr,
			bool distinct);

	bool equals(int* offsets, char* tuple1, char* tuple2, int* groupByAttr,
			int numGroupByAttr);

};

#endif /* RELATIONAL_SORTAGGREGATION_HPP_ */
