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
 * SortAggregation.cpp
 *
 *  Created on: Dec 26, 2017
 *      Author: martin.theobald
 */

#include "SortAggregation.hpp"

#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "../communication/Message.hpp"
#include "../dataflow/Vertex.hpp"
#include "../serialization/Serialization.hpp"

SortAggregation::SortAggregation() :
		Vertex(0, 0, 1) {

	this->schema = nullptr;
	this->aggr = nullptr;
	this->cond = nullptr;
	this->groupByAttr = nullptr;
	this->projAttr = nullptr;
	this->shardAttr = nullptr;
	this->numAggr = 0;
	this->numCond = 0;
	this->numGroupByAttr = 0;
	this->numProjAttr = 0;
	this->numShardAttr = 0;
	this->distinct = false;
	//cout << "SORTAGGREGATION CREATED @ " << rank << endl;
}

SortAggregation::SortAggregation(Schema* schema, Aggr** aggr, Cond** cond,
		int* groupByAttr, int* projAttr, int* shardAttr, int numAggr,
		int numCond, int numGroupByAttr, int numProjAttr, int numShardAttr,
		bool distinct, int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {

	this->schema = schema;
	this->aggr = aggr;
	this->cond = cond;
	this->groupByAttr = groupByAttr;
	this->projAttr = projAttr;
	this->shardAttr = shardAttr;
	this->numAggr = numAggr;
	this->numCond = numCond;
	this->numGroupByAttr = numGroupByAttr;
	this->numProjAttr = numProjAttr;
	this->numShardAttr = numShardAttr;
	this->distinct = distinct;
	//cout << "SORTAGGREGATION CREATED @ " << rank << endl;
}

SortAggregation::~SortAggregation() {
	if (this->aggr != nullptr) {
		for (int i = 0; i < numAggr; i++)
			delete this->aggr[i];
		delete[] this->aggr;
	}
	if (this->cond != nullptr) {
		for (int i = 0; i < numCond; i++)
			delete this->cond[i];
		delete[] this->cond;
	}
	if (this->projAttr != nullptr)
		delete[] this->projAttr;
	if (this->groupByAttr != nullptr)
		delete[] this->groupByAttr;
	if (this->shardAttr != nullptr)
		delete[] this->shardAttr;
	//cout << "SORTAGGREGATION DELETED @ " << rank << endl;
}

Relation* SortAggregation::select(Relation* rel, Cond** cond, int numCond) {
	int attr[rel->schema->numAttributes];
	for (int i = 0; i < rel->schema->numAttributes; i++) {
		attr[i] = i;
	}
	return processLocal(rel, nullptr, cond, attr, attr, 0, numCond,
			rel->schema->numAttributes, rel->schema->numAttributes, false);
}

Relation* SortAggregation::selectDistinct(Relation* rel, Cond** cond,
		int numCond) {
	int attr[rel->schema->numAttributes];
	for (int i = 0; i < rel->schema->numAttributes; i++) {
		attr[i] = i;
	}
	return processLocal(rel, nullptr, cond, attr, attr, 0, numCond,
			rel->schema->numAttributes, rel->schema->numAttributes, true);
}

Relation* SortAggregation::selectProject(Relation* rel, Cond** cond,
		int* projAttr, int numCond, int numProjAttr) {
	int attr[rel->schema->numAttributes];
	for (int i = 0; i < rel->schema->numAttributes; i++) {
		attr[i] = i;
	}
	return processLocal(rel, nullptr, cond, attr, projAttr, 0, numCond,
			rel->schema->numAttributes, numProjAttr, false);
}

Relation* SortAggregation::selectProjectDistinct(Relation* rel, Cond** cond,
		int* projAttr, int numCond, int numProjAttr) {
	int attr[rel->schema->numAttributes];
	for (int i = 0; i < rel->schema->numAttributes; i++) {
		attr[i] = i;
	}
	return processLocal(rel, nullptr, cond, attr, projAttr, 0, numCond,
			rel->schema->numAttributes, numProjAttr, true);
}

Relation* SortAggregation::groupBy(Relation* rel, Aggr** aggr, Cond** cond,
		int* groupByAttr, int* projAttr, int numAggr, int numCond,
		int numGroupByAttr, int numProjAttr) {
	return processLocal(rel, aggr, cond, groupByAttr, projAttr, numAggr,
			numCond, numGroupByAttr, numProjAttr, true);
}

Relation* SortAggregation::groupByAll(Relation* rel, Aggr** aggr, Cond** cond,
		int numAggr, int numCond) {
	return processLocal(rel, aggr, cond, nullptr, nullptr, numAggr, numCond, 0,
			0, true);
}

void SortAggregation::batchProcess() {

	cout << "SORTAGGREGATION->BATCHPROCESS [" << tag << "] @ " << rank << endl;

	Relation relation(schema, rMessages, worldSize, 0);

	bool sorted = true;
	for (int i = 0; i < numGroupByAttr; i++)
		sorted = sorted && schema->attributes[groupByAttr[i]].sorted;
	if (!sorted)
		relation.sort(groupByAttr, numGroupByAttr);

	//relation.print(25);

	processDistr(&relation, aggr, cond, groupByAttr, projAttr, shardAttr,
			numAggr, numCond, numGroupByAttr, numProjAttr, numShardAttr,
			distinct);
}

Relation* SortAggregation::processLocal(Relation* rel, Aggr** aggr, Cond** cond,
		int* groupByAttr, int* projAttr, int numAggr, int numCond,
		int numGroupByAttr, int numProjAttr, bool distinct) {

	Window* window = new Window(12000000); // fixed for now in local processing!

	Serialization ser;
	char *currTuple, *lastTuple = nullptr;

	int i = 0, j, m = 0, pos = 0;
	while (i <= rel->size) {

		if (i < rel->size) {
			currTuple = rel->getTuple(i);

			for (int l = 0; l < numCond; l++) {
				if (!cond[l]->check(
						currTuple + rel->schema->offsets[cond[l]->condAttr])) {
					goto cont;
				}
			}
		}

		if (lastTuple != nullptr
				&& (!distinct || i == rel->size
						|| !equals(rel->schema->offsets, lastTuple, currTuple,
								groupByAttr, numGroupByAttr))) {

			for (j = 0; j < numProjAttr; j++) {
				memcpy(&window->buffer[pos],
						&lastTuple[rel->schema->offsets[projAttr[j]]],
						rel->schema->attributes[projAttr[j]].size);
				pos += rel->schema->attributes[projAttr[j]].size;
			}

			for (j = 0; j < numAggr; j++) {
				if (aggr[j]->type == INT_TYPE) {
					int val = (int) aggr[j]->getAggr();
					memcpy(&window->buffer[pos], &val, 4);
					pos += 4;
				} else if (aggr[j]->type == FLOAT_TYPE) {
					float val = aggr[j]->getAggr();
					memcpy(&window->buffer[pos], &val, 4);
					pos += 4;
				}
			}

			for (j = 0; j < numAggr; j++) {
				aggr[j]->reset();
			}

			m++;
		}

		for (j = 0; j < numAggr; j++) {
			aggr[j]->addValue(
					ser.decodeFloat(
							currTuple
									+ rel->schema->offsets[aggr[j]->aggrAttr]));
		}

		lastTuple = currTuple;

		cont: ;
		i++;
	}

	window->size = pos;

	Attribute* attr = new Attribute[numProjAttr + numAggr];

	for (i = 0; i < numProjAttr; i++) {
		attr[i].setValues(rel->schema->attributes[projAttr[i]].name,
				rel->schema->attributes[projAttr[i]].type,
				rel->schema->attributes[projAttr[i]].size, true);
	}

	for (j = 0; j < numAggr; j++) {
		attr[i + j].setValues(aggr[j]->name, aggr[j]->type, aggr[j]->size,
				false);
	}

	Schema* schema = new Schema(attr, numProjAttr + numAggr);
	Relation* result = new Relation(schema, window);

	//cout << m << " TUPLES, " << result->size << " BYTES." << endl;

	return result;
}

void SortAggregation::processDistr(Relation* rel, Aggr** aggr, Cond** cond,
		int* groupByAttr, int* projAttr, int* shardAttr, int numAggr,
		int numCond, int numGroupByAttr, int numProjAttr, int numShardAttr,
		bool distinct) {

	Serialization ser;
	char *currTuple = nullptr, *lastTuple = nullptr;

	int i = 0, j, m = 0, pos = 0, hash;
	while (i <= rel->size) {

		if (i < rel->size) {
			currTuple = rel->getTuple(i);

			for (int l = 0; l < numCond; l++) {
				if (!cond[l]->check(
						currTuple + rel->schema->offsets[cond[l]->condAttr])) {
					goto cont;
				}
			}
		}

		if (lastTuple != nullptr
				&& (!distinct || i == rel->size
						|| !equals(rel->schema->offsets, lastTuple, currTuple,
								groupByAttr, numGroupByAttr))) {

			hash = 0;
			for (j = 0; j < numShardAttr; j++)
				hash +=
						(*(int*) (&lastTuple[rel->schema->offsets[shardAttr[j]]]));

			for (j = 0; j < numProjAttr; j++) {
				memcpy(
						&sMessages[hash % worldSize]->buffer[sMessages[hash
								% worldSize]->size],
						&lastTuple[rel->schema->offsets[projAttr[j]]],
						rel->schema->attributes[projAttr[j]].size);
				sMessages[hash % worldSize]->size +=
						rel->schema->attributes[projAttr[j]].size;
				pos += rel->schema->attributes[projAttr[j]].size;
			}

			for (j = 0; j < numAggr; j++) {
				if (aggr[j]->type == INT_TYPE) {
					int val = (int) aggr[j]->getAggr();
					memcpy(
							&sMessages[hash % worldSize]->buffer[sMessages[hash
									% worldSize]->size], &val, 4);
					sMessages[hash % worldSize]->size += 4;
					pos += 4;
				} else if (aggr[j]->type == FLOAT_TYPE) {
					float val = aggr[j]->getAggr();
					memcpy(
							&sMessages[hash % worldSize]->buffer[sMessages[hash
									% worldSize]->size], &val, 4);
					sMessages[hash % worldSize]->size += 4;
					pos += 4;
				}
			}

			for (j = 0; j < numAggr; j++) {
				aggr[j]->reset();
			}

			m++;
		}

		if (currTuple != nullptr) {
			for (j = 0; j < numAggr; j++) {
				aggr[j]->addValue(
						ser.decodeFloat(
								currTuple
										+ rel->schema->offsets[aggr[j]->aggrAttr]));
			}
		}

		lastTuple = currTuple;

		cont: ;
		i++;
	}

	//cout << m << " TUPLES, " << pos << " BYTES." << endl;
}

bool SortAggregation::equals(int* offsets, char* tuple1, char* tuple2,
		int* groupByAttr, int numGroupByAttr) {

	for (int i = 0; i < numGroupByAttr; i++) {

		if ((*(int*) (tuple1 + offsets[groupByAttr[i]]))
				!= (*(int*) (tuple2 + offsets[groupByAttr[i]])))
			return false;
	}

	return true;
}
