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
 * Relation.hpp
 *
 *  Created on: Dec 23, 2017
 *      Author: martin.theobald
 */

#ifndef RELATIONAL_RELATION_HPP_
#define RELATIONAL_RELATION_HPP_

#include "Schema.hpp"

#include "../communication/Window.hpp"
#include "../serialization/Serialization.hpp"

#include <cstring>
#include <iostream>
#include <string>
#include <algorithm>

using namespace std;

struct IntComparator {

	int* offsets;

	int* sortAttr;

	int numSortAttr;

	IntComparator(int* offsets, int* sortAttr, int numSortAttr) {
		this->offsets = offsets;
		this->sortAttr = sortAttr;
		this->numSortAttr = numSortAttr;
	}

	bool operator ()(char* left, char* right) { // for int only, pretty fast!
		for (int i = 0; i < numSortAttr; i++) {
			if ((*(int*) (left + offsets[sortAttr[i]]))
					< (*(int*) (right + offsets[sortAttr[i]]))) {
				return true;
			} else if ((*(int*) (left + offsets[sortAttr[i]]))
					> (*(int*) (right + offsets[sortAttr[i]]))) {
				return false;
			}
		}
		return false;
	}

};

struct StringComparator {

	Schema* schema;

	int* sortAttr;

	int numSortAttr;

	StringComparator(Schema* schema, int* sortAttr, int numSortAttr) {
		this->schema = schema;
		this->sortAttr = sortAttr;
		this->numSortAttr = numSortAttr;
	}

	bool operator ()(char* left, char* right) { // caution: expensive string comparator!
		for (int i = 0; i < numSortAttr; i++) {
			string l(left + schema->offsets[sortAttr[i]],
					schema->attributes[sortAttr[i]].size);
			string r(right + schema->offsets[sortAttr[i]],
					schema->attributes[sortAttr[i]].size);
			int c = l.compare(r);
			if (c < 0) {
				return true;
			} else if (c > 0) {
				return false;
			}
		}
		return false;
	}

};

class Relation {

public:

	Window* window;

	Schema* schema;

	Relation(Schema* schema, Window* window);

	Relation(Schema* schema, Message** messages, int numMessages, int opIdx);

	~Relation();

	char* getTuple(int i);

	char* getValue(int i, int j);

	void print(int maxTuples);

	int size;

	void sort(int* sortAttr, int numSortAttr);

	void sortStrings(int* sortAttr, int numSortAttr);

private:

	char** tuples;

	void quicksort(int low, int high) {
		if (low < high) {
			int pi = partition(low, high);
			quicksort(low, pi - 1);
			quicksort(pi + 1, high);
		}
	}

	int partition(int low, int high) {
		char* pivot = getTuple(high);
		int i = (low - 1);
		for (int j = low; j <= high - 1; j++) {
			if (less(getTuple(j), pivot)) {
				i++;
				swap(i, j);
			}
		}
		swap(i + 1, high);
		return i + 1;
	}

	void swap(int i, int j) {
		char* temp = tuples[i];
		tuples[i] = tuples[j];
		tuples[j] = temp;
	}

	static bool less(char* left, char* right) {
		return Serialization::decodeInt(left) < Serialization::decodeInt(right);
	}

};

#endif /* RELATIONAL_RELATION_HPP_ */
