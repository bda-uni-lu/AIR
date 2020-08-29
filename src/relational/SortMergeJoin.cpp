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
 * SortMergeJoin.cpp
 *
 *  Created on: Dec 25, 2017
 *      Author: martin.theobald
 */

#include "SortMergeJoin.hpp"

#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "../communication/Message.hpp"
#include "../dataflow/Vertex.hpp"

SortMergeJoin::SortMergeJoin() :
		Vertex(0, 0, 1) {

	this->leftSchema = nullptr;
	this->rightSchema = nullptr;
	this->leftCond = nullptr;
	this->rightCond = nullptr;
	this->leftJoinAttr = nullptr;
	this->rightJoinAttr = nullptr;
	this->leftProjAttr = nullptr;
	this->rightProjAttr = nullptr;
	this->leftShardAttr = nullptr;
	this->rightShardAttr = nullptr;
	this->numLeftCond = 0;
	this->numRightCond = 0;
	this->numJoinAttr = 0;
	this->numLeftProjAttr = 0;
	this->numRightProjAttr = 0;
	this->numLeftShardAttr = 0;
	this->numRightShardAttr = 0;
	cout << "SORTMERGEJOIN CREATED @ " << rank << endl;
}

SortMergeJoin::SortMergeJoin(Schema* leftSchema, Schema* rightSchema,
		Cond** leftCond, Cond** rightCond, int* leftJoinAttr,
		int* rightJoinAttr, int* leftProjAttr, int* rightProjAttr,
		int* leftShardAttr, int* rightShardAttr, int numLeftCond,
		int numRightCond, int numJoinAttr, int numLeftProjAttr,
		int numRightProjAttr, int numLeftShardAttr, int numRightShardAttr,
		int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {

	this->leftSchema = leftSchema;
	this->rightSchema = rightSchema;
	this->leftCond = leftCond;
	this->rightCond = rightCond;
	this->leftJoinAttr = leftJoinAttr;
	this->rightJoinAttr = rightJoinAttr;
	this->leftProjAttr = leftProjAttr;
	this->rightProjAttr = rightProjAttr;
	this->leftShardAttr = leftShardAttr;
	this->rightShardAttr = rightShardAttr;
	this->numLeftCond = numLeftCond;
	this->numRightCond = numRightCond;
	this->numJoinAttr = numJoinAttr;
	this->numLeftProjAttr = numLeftProjAttr;
	this->numRightProjAttr = numRightProjAttr;
	this->numLeftShardAttr = numLeftShardAttr;
	this->numRightShardAttr = numRightShardAttr;
	//cout << "SORTMERGEJOIN CREATED @ " << rank << endl;
}

SortMergeJoin::~SortMergeJoin() {
	if (this->leftCond != nullptr) {
		for (int i = 0; i < numLeftCond; i++)
			delete this->leftCond[i];
		delete[] this->leftCond;
	}
	if (this->rightCond != nullptr) {
		for (int i = 0; i < numRightCond; i++)
			delete this->rightCond[i];
		delete[] this->rightCond;
	}
	if (this->leftJoinAttr != nullptr)
		delete[] this->leftJoinAttr;
	if (this->rightJoinAttr != nullptr)
		delete[] this->rightJoinAttr;
	if (this->leftProjAttr != nullptr)
		delete[] this->leftProjAttr;
	if (this->rightProjAttr != nullptr)
		delete[] this->rightProjAttr;
	if (this->leftShardAttr != nullptr)
		delete[] this->leftShardAttr;
	if (this->rightShardAttr != nullptr)
		delete[] this->rightShardAttr;
	//cout << "SORTMERGEJOIN DELETED @ " << rank << endl;
}

void SortMergeJoin::batchProcess() {

	cout << "SORTMERGEJOIN->BATCHPROCESS " << tag << " @ " << rank << endl;

	Relation leftRelation(leftSchema, rMessages, worldSize, 0);
	Relation rightRelation(rightSchema, rMessages, worldSize, 1);

	bool lSorted = true, rSorted = true;
	for (int i = 0; i < numJoinAttr; i++) {
		lSorted = lSorted && leftSchema->attributes[leftJoinAttr[i]].sorted;
		rSorted = rSorted && rightSchema->attributes[rightJoinAttr[i]].sorted;
	}
	if (!lSorted)
		leftRelation.sort(leftJoinAttr, numJoinAttr);
	if (!rSorted)
		rightRelation.sort(rightJoinAttr, numJoinAttr);

	//leftRelation.print(25);
	//rightRelation.print(25);

	processDistr(&leftRelation, &rightRelation, leftCond, rightCond,
			leftJoinAttr, rightJoinAttr, leftProjAttr, rightProjAttr,
			leftShardAttr, rightShardAttr, numLeftCond, numRightCond,
			numJoinAttr, numLeftProjAttr, numRightProjAttr, numLeftShardAttr,
			numRightShardAttr);

}

Relation* SortMergeJoin::join(Relation* left, Relation* right, Cond** leftCond,
		Cond** rightCond, int* leftJoinAttr, int* rightJoinAttr,
		int* leftProjAttr, int* rightProjAttr, int numLeftCond,
		int numRightCond, int numJoinAttr, int numLeftProjAttr,
		int numRightProjAttr) {

	return processLocal(left, right, leftCond, rightCond, leftJoinAttr,
			rightJoinAttr, leftProjAttr, rightProjAttr, numLeftCond,
			numRightCond, numJoinAttr, numLeftProjAttr, numRightProjAttr);
}

Relation* SortMergeJoin::processLocal(Relation* left, Relation* right,
		Cond** leftCond, Cond** rightCond, int* leftJoinAttr,
		int* rightJoinAttr, int* leftProjAttr, int* rightProjAttr,
		int numLeftCond, int numRightCond, int numJoinAttr, int numLeftProjAttr,
		int numRightProjAttr) {

	Window* window = new Window(12000000); // fixed for now in local mode!

	char *leftTuple, *rightTuple;
	int i = 0, j = 0, jj, r, l, m = 0, pos = 0;

	while (i < left->size && j < right->size) {

		rightTuple = right->getTuple(j);

		while (i < left->size
				&& less(left->schema->offsets, right->schema->offsets,
						leftTuple = left->getTuple(i), rightTuple, leftJoinAttr,
						rightJoinAttr, numJoinAttr))
			i++;

		while (j < right->size
				&& greater(left->schema->offsets, right->schema->offsets,
						leftTuple, rightTuple = right->getTuple(j),
						leftJoinAttr, rightJoinAttr, numJoinAttr))
			j++;

		for (int l = 0; l < numLeftCond; l++) {
			if (!leftCond[l]->check(
					leftTuple + left->schema->offsets[leftCond[l]->condAttr])) {
				goto cont1;
			}
		}

		for (jj = j;
				jj < right->size
						&& equals(left->schema->offsets, right->schema->offsets,
								leftTuple, rightTuple = right->getTuple(jj),
								leftJoinAttr, rightJoinAttr, numJoinAttr);
				jj++) {

			for (r = 0; r < numRightCond; r++) {
				if (!rightCond[r]->check(
						rightTuple
								+ right->schema->offsets[rightCond[r]->condAttr])) {
					goto cont2;
				}
			}

			//cout << "  MATCH LEFT " << i << " " << jj << " " << numLeftProjAttr << endl;

			for (l = 0; l < numLeftProjAttr; l++) {
				memcpy(&window->buffer[pos],
						&leftTuple[left->schema->offsets[leftProjAttr[l]]],
						left->schema->attributes[leftProjAttr[l]].size);
				pos += left->schema->attributes[leftProjAttr[l]].size;
			}

			//cout << "  MATCH RIGHT  " << i << " " << jj << " " << numRightProjAttr << endl;

			for (r = 0; r < numRightProjAttr; r++) {
				memcpy(&window->buffer[pos],
						&rightTuple[right->schema->offsets[rightProjAttr[r]]],
						right->schema->attributes[rightProjAttr[r]].size);
				pos += right->schema->attributes[rightProjAttr[r]].size;
			}

			m++;
			cont2: ;
		}

		cont1: ;
		i++;
	}

	window->size = pos;

	Attribute* attr = new Attribute[numLeftProjAttr + numRightProjAttr];

	for (l = 0; l < numLeftProjAttr; l++) {
		attr[l].setValues(left->schema->attributes[leftProjAttr[l]].name,
				left->schema->attributes[leftProjAttr[l]].type,
				left->schema->attributes[leftProjAttr[l]].size, false);
	}

	for (r = 0; r < numRightProjAttr; r++) {
		attr[l + r].setValues(right->schema->attributes[rightProjAttr[r]].name,
				right->schema->attributes[rightProjAttr[r]].type,
				right->schema->attributes[rightProjAttr[r]].size, false);
	}

	Schema* schema = new Schema(attr, numLeftProjAttr + numRightProjAttr);
	Relation* result = new Relation(schema, window);

	//cout << m << " TUPLES, " << result->size << " BYTES." << endl;

	return result;
}

void SortMergeJoin::processDistr(Relation* left, Relation* right,
		Cond** leftCond, Cond** rightCond, int* leftJoinAttr,
		int* rightJoinAttr, int* leftProjAttr, int* rightProjAttr,
		int* leftShardAttr, int* rightShardAttr, int numLeftCond,
		int numRightCond, int numJoinAttr, int numLeftProjAttr,
		int numRightProjAttr, int numLeftShardAttr, int numRightShardAttr) {

	char *leftTuple, *rightTuple;
	int i = 0, j = 0, jj, r, l, m = 0, pos = 0, hash;
	while (i < left->size && j < right->size) {

		rightTuple = right->getTuple(j);

		while (i < left->size
				&& less(left->schema->offsets, right->schema->offsets,
						leftTuple = left->getTuple(i), rightTuple, leftJoinAttr,
						rightJoinAttr, numJoinAttr))
			i++;

		while (j < right->size
				&& greater(left->schema->offsets, right->schema->offsets,
						leftTuple, rightTuple = right->getTuple(j),
						leftJoinAttr, rightJoinAttr, numJoinAttr))
			j++;

		for (int l = 0; l < numLeftCond; l++) {
			if (!leftCond[l]->check(
					leftTuple + left->schema->offsets[leftCond[l]->condAttr])) {
				goto cont1;
			}
		}

		for (jj = j;
				jj < right->size
						&& equals(left->schema->offsets, right->schema->offsets,
								leftTuple, rightTuple = right->getTuple(jj),
								leftJoinAttr, rightJoinAttr, numJoinAttr);
				jj++) {

			for (r = 0; r < numRightCond; r++) {
				if (!rightCond[r]->check(
						rightTuple
								+ right->schema->offsets[rightCond[r]->condAttr])) {
					goto cont2;
				}
			}

			hash = 0;
			for (l = 0; l < numLeftShardAttr; l++)
				hash +=
						(*(int*) (&leftTuple[left->schema->offsets[leftShardAttr[l]]]));
			for (r = 0; r < numRightShardAttr; r++)
				hash +=
						(*(int*) (&rightTuple[right->schema->offsets[rightShardAttr[r]]]));

			//cout << "  MATCH LEFT " << i << " " << jj << " " << numLeftProjAttr << endl;

			for (l = 0; l < numLeftProjAttr; l++) {
				memcpy(
						&sMessages[hash % worldSize]->buffer[sMessages[hash
								% worldSize]->size],
						&leftTuple[left->schema->offsets[leftProjAttr[l]]],
						left->schema->attributes[leftProjAttr[l]].size);
				sMessages[hash % worldSize]->size +=
						left->schema->attributes[leftProjAttr[l]].size;
				pos += left->schema->attributes[leftProjAttr[l]].size;
			}

			//cout << "  MATCH RIGHT  " << i << " " << jj << " " << numRightProjAttr << endl;

			for (r = 0; r < numRightProjAttr; r++) {
				memcpy(
						&sMessages[hash % worldSize]->buffer[sMessages[hash
								% worldSize]->size],
						&rightTuple[right->schema->offsets[rightProjAttr[r]]],
						right->schema->attributes[rightProjAttr[r]].size);
				sMessages[hash % worldSize]->size +=
						right->schema->attributes[rightProjAttr[r]].size;
				pos += right->schema->attributes[rightProjAttr[r]].size;
			}

			m++;
			cont2: ;
		}

		cont1: ;
		i++;
	}

//cout << m << " TUPLES, " << pos << " BYTES." << endl;
}

bool SortMergeJoin::less(int* leftOffsets, int* rightOffsets, char* leftTuple,
		char* rightTuple, int* leftJoinAttr, int* rightJoinAttr,
		int numJoinAttr) {

	for (int i = 0; i < numJoinAttr; i++) {

		if ((*(int*) (leftTuple + leftOffsets[leftJoinAttr[i]]))
				< (*(int*) (rightTuple + rightOffsets[rightJoinAttr[i]])))
			return true;

		else if ((*(int*) (leftTuple + leftOffsets[leftJoinAttr[i]]))
				> (*(int*) (rightTuple + rightOffsets[rightJoinAttr[i]])))
			return false;
	}

	return false;
}

bool SortMergeJoin::greater(int* leftOffsets, int* rightOffsets,
		char* leftTuple, char* rightTuple, int* leftJoinAttr,
		int* rightJoinAttr, int numJoinAttr) {

	for (int i = 0; i < numJoinAttr; i++) {

		if ((*(int*) (leftTuple + leftOffsets[leftJoinAttr[i]]))
				> (*(int*) (rightTuple + rightOffsets[rightJoinAttr[i]])))
			return true;

		else if ((*(int*) (leftTuple + leftOffsets[leftJoinAttr[i]]))
				< (*(int*) (rightTuple + rightOffsets[rightJoinAttr[i]])))
			return false;
	}

	return false;
}

bool SortMergeJoin::equals(int* leftOffsets, int* rightOffsets, char* leftTuple,
		char* rightTuple, int* leftJoinAttr, int* rightJoinAttr,
		int numJoinAttr) {

	for (int i = 0; i < numJoinAttr; i++) {

		if ((*(int*) (leftTuple + leftOffsets[leftJoinAttr[i]]))
				!= (*(int*) (rightTuple + rightOffsets[rightJoinAttr[i]])))
			return false;
	}

	return true;
}
