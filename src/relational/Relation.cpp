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
 * Relation.cpp
 *
 *  Created on: Dec 23, 2017
 *      Author: martin.theobald
 */

#include "Attribute.hpp"
#include "Relation.hpp"

#include <string>
#include <iostream>

using namespace std;

Relation::Relation(Schema* schema, Window* window) {

	this->schema = schema;
	this->window = window;
	this->size = window->size / schema->size;
	this->tuples = new char*[size];

	int t = 0;
	for (int i = 0; i < size; i++) {
		tuples[i] = &window->buffer[t];
		t += schema->size;
	}
}

Relation::Relation(Schema* schema, Message** messages, int numMessages,
		int opIdx) {

	this->schema = schema;
	this->window = nullptr;

	int s = 0;
	for (int i = 0; i < numMessages; i++) {
		s += messages[opIdx * numMessages + i]->size;
	}

	this->size = s / schema->size;
	this->tuples = new char*[size];

	int t = 0;
	for (int i = 0; i < numMessages; i++) {
		int b = 0, mSize = messages[opIdx * numMessages + i]->size
				/ schema->size;
		for (int j = 0; j < mSize; j++) {
			tuples[t] = &messages[opIdx * numMessages + i]->buffer[b];
			b += schema->size;
			t++;
		}
	}
}

Relation::~Relation() {
	delete[] tuples;
	//cout << "RELATION DELETED." << endl;
}

char* Relation::getTuple(int i) {
	return tuples[i];
}

char* Relation::getValue(int i, int j) {
	return tuples[i] + schema->offsets[j];
}

void Relation::sort(int* sortAttr, int numSortAttr) {
	std::sort(tuples, tuples + size,
			IntComparator(schema->offsets, sortAttr, numSortAttr));
	//quicksort(0, size() - 1);
}

void Relation::sortStrings(int* sortAttr, int numSortAttr) {
	std::sort(tuples, tuples + size,
			StringComparator(schema, sortAttr, numSortAttr));
	//quicksort(0, size() - 1);
}

void Relation::print(int maxTuples) {
	cout << "\n\t";
	for (int i = 0; i < schema->numAttributes; i++) {
		cout << schema->attributes[i].name
				<< (schema->attributes[i].sorted ? "*" : "") << "["
				<< schema->attributes[i].size << "]" << "\t";
	}
	cout << endl;
	cout << fixed;
	cout.precision(2);
	for (int i = 0; i < size && i < maxTuples; i++) {
		cout << i << ":";
		for (int j = 0; j < schema->numAttributes && j < 5; j++) {
			if (schema->attributes[j].type == INT_TYPE) {
				cout << "\t" << Serialization::decodeInt(getValue(i, j));
			} else if (schema->attributes[j].type == FLOAT_TYPE) {
				cout << "\t" << Serialization::decodeFloat(getValue(i, j));
			} else if (schema->attributes[j].type == CHAR_TYPE) {
				string s(getValue(i, j), schema->attributes[j].size);
				cout << "\t" << s;
			} else {
				cout << "?\t";
			}
		}
		cout << endl;
	}
	cout << endl;
}
