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
 * Aggr.hpp
 *
 *  Created on: Dec 26, 2017
 *      Author: martin.theobald
 */

#ifndef RELATIONAL_AGGR_HPP_
#define RELATIONAL_AGGR_HPP_

#include "Attribute.hpp"

class Aggr: public Attribute {

public:

	int aggrAttr;

	Aggr();

	Aggr(string name, AttributeType type, int size, bool sorted, int aggrAttr);

	virtual ~Aggr();

	virtual void addValue(float val);

	virtual float getAggr();

	virtual void reset();

	void setValues(string name, AttributeType type, int size, bool sorted,
			int aggrAttr);

};

class Min: public Aggr {

public:

	float min;

	Min(string name, AttributeType type, int size, bool sorted, int aggrAttr);

	void addValue(float val);

	float getAggr();

	void reset();

};

class Max: public Aggr {

public:

	float max;

	Max(string name, AttributeType type, int size, bool sorted, int aggrAttr);

	void addValue(float val);

	float getAggr();

	void reset();
};

class Sum: public Aggr {

public:

	float sum;

	Sum(string name, AttributeType type, int size, bool sorted, int aggrAttr);

	void addValue(float val);

	float getAggr();

	void reset();
};

class Count: public Aggr {

public:

	int count;

	Count(string name, AttributeType type, int size, bool sorted, int aggrAttr);

	void addValue(float val);

	float getAggr();

	void reset();
};

class Avg: public Aggr {

public:

	float sum;

	int count;

	Avg(string name, AttributeType type, int size, bool sorted, int aggrAttr);

	void addValue(float val);

	float getAggr();

	void reset();
};

#endif /* RELATIONAL_AGGR_HPP_ */
