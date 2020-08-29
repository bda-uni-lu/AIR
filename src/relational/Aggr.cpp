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
 * Aggr.cpp
 *
 *  Created on: Dec 26, 2017
 *      Author: martin.theobald
 */

#include <iostream>
#include <limits>

#include "Aggr.hpp"

Aggr::Aggr() :
		Attribute() {
	this->aggrAttr = -1;
}

Aggr::Aggr(string name, AttributeType type, int size, bool sorted, int aggrAttr) :
		Attribute(name, type, size, sorted) {
	this->aggrAttr = aggrAttr;
}

Aggr::~Aggr() {
}

void Aggr::addValue(float val) {
	cout << "WARNING: CALL TO GENERIC AGGR.ADDVALUE!" << endl;
}

float Aggr::getAggr() {
	cout << "WARNING: CALL TO GENERIC AGGR.GETAGGR!" << endl;
	return 0;
}

void Aggr::reset() {
	cout << "WARNING: CALL TO GENERIC AGGR.RESET!" << endl;
}

void Aggr::setValues(string name, AttributeType type, int size, bool sorted,
		int aggrAttr) {
	this->name = name;
	this->type = type;
	this->size = size;
	this->sorted = sorted;
	this->aggrAttr = aggrAttr;
}

Min::Min(string name, AttributeType type, int size, bool sorted, int aggrAttr) :
		Aggr(name, type, size, sorted, aggrAttr) {
	this->min = std::numeric_limits<float>::max();
}

void Min::addValue(float val) {
	min = min < val ? min : val;
}

float Min::getAggr() {
	return min;
}

void Min::reset() {
	this->min = std::numeric_limits<float>::max();
}

Max::Max(string name, AttributeType type, int size, bool sorted, int aggrAttr) :
		Aggr(name, type, size, sorted, aggrAttr) {
	this->max = std::numeric_limits<float>::min();
}

void Max::addValue(float val) {
	max = max > val ? max : val;
}

float Max::getAggr() {
	return max;
}

void Max::reset() {
	this->max = std::numeric_limits<float>::min();
}

Count::Count(string name, AttributeType type, int size, bool sorted,
		int aggrAttr) :
		Aggr(name, type, size, sorted, aggrAttr) {
	this->count = 0;
}

void Count::addValue(float val) {
	count++;
}

float Count::getAggr() {
	return count;
}

void Count::reset() {
	count = 0;
}

Sum::Sum(string name, AttributeType type, int size, bool sorted, int aggrAttr) :
		Aggr(name, type, size, sorted, aggrAttr) {
	this->sum = 0e0;
}

void Sum::addValue(float val) {
	cout << "ADD: " << val << " = " << sum << endl;
	sum += val;
}

float Sum::getAggr() {
	return sum;
}

void Sum::reset() {
	sum = 0e0;
}

Avg::Avg(string name, AttributeType type, int size, bool sorted, int aggrAttr) :
		Aggr(name, type, size, sorted, aggrAttr) {
	this->sum = 0e0;
	this->count = 0;
}

void Avg::addValue(float val) {
	sum += val;
	count++;
}

float Avg::getAggr() {
	return sum / count;
}

void Avg::reset() {
	sum = 0e0;
	count = 0;
}
