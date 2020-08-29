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
 * SortMergeJoin.hpp
 *
 *  Created on: Dec 25, 2017
 *      Author: martin.theobald
 */

#ifndef RELATIONAL_SORTMERGEJOIN_HPP_
#define RELATIONAL_SORTMERGEJOIN_HPP_

#include "Aggr.hpp"
#include "Attribute.hpp"
#include "Cond.hpp"
#include "Relation.hpp"
#include "Schema.hpp"

#include "../dataflow/Vertex.hpp"

using namespace std;

class SortMergeJoin: public Vertex {

public:

	Schema* leftSchema;
	Schema* rightSchema;

	Cond** leftCond;
	Cond** rightCond;

	int* leftJoinAttr;
	int* rightJoinAttr;
	int* leftProjAttr;
	int* rightProjAttr;
	int* leftShardAttr;
	int* rightShardAttr;

	int numLeftCond;
	int numRightCond;
	int numJoinAttr;
	int numLeftProjAttr;
	int numRightProjAttr;
	int numLeftShardAttr;
	int numRightShardAttr;

	SortMergeJoin();

	SortMergeJoin(Schema* leftSchema, Schema* rightSchema, Cond** leftCond,
			Cond** rightCond, int* leftJoinAttr, int* rightJoinAttr,
			int* leftProjAttr, int* rightProjAttr, int* leftShardAttr,
			int* rightShardAttr, int numLeftCond, int numRightCond,
			int numJoinAttr, int numLeftProjAttr, int numRightProjAttr,
			int numLeftShardAttr, int numRightShardAttr, int tag, int rank,
			int worldSize);

	~SortMergeJoin();

	void batchProcess();

	Relation* join(Relation* left, Relation* right, Cond** leftCond,
			Cond** rightCond, int* leftJoinAttr, int* rightJoinAttr,
			int* leftProjAttr, int* rightProjAttr, int numLeftCond,
			int numRightCond, int numJoinAttr, int numLeftProjAttr,
			int numRightProjAttr);

private:

	Relation* processLocal(Relation* left, Relation* right, Cond** leftCond,
			Cond** rightCond, int* leftJoinAttr, int* rightJoinAttr,
			int* leftProjAttr, int* rightProjAttr, int numLeftCond,
			int numRightCond, int numJoinAttr, int numLeftProjAttr,
			int numRightProjAttr);

	void processDistr(Relation* left, Relation* right, Cond** leftCond,
			Cond** rightCond, int* leftJoinAttr, int* rightJoinAttr,
			int* leftProjAttr, int* rightProjAttr, int* leftShardAttr,
			int* rightShardAttr, int numLeftCond, int numRightCond,
			int numJoinAttr, int numLeftProjAttr, int numRightProjAttr,
			int numLeftShardAttr, int numRightShardAttr);

	bool equals(int* leftOffsets, int* rightOffsets, char* leftTuple,
			char* rightTuple, int* leftJoinAttr, int* rightJoinAttr,
			int numJoinAttr);

	bool less(int* leftOffsets, int* rightOffsets, char* leftTuple,
			char* rightTuple, int* leftJoinAttr, int* rightJoinAttr,
			int numJoinAttr);

	bool greater(int* leftOffsets, int* rightOffsets, char* leftTuple,
			char* rightTuple, int* leftJoinAttr, int* rightJoinAttr,
			int numJoinAttr);

};

#endif /* RELATIONAL_SORTMERGEJOIN_HPP_ */
