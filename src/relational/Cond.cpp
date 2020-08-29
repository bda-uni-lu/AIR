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
 * Cond.cpp
 *
 *  Created on: Dec 26, 2017
 *      Author: martin.theobald
 */

#include <iostream>
#include <limits>
#include <string>

#include "Cond.hpp"

Cond::Cond() :
		Attribute() {
	this->condAttr = -1;
	this->cond = nullptr;
}

Cond::Cond(AttributeType type, int size, int condAttr, void const* cond) :
		Attribute("", type, size, false) {
	this->condAttr = condAttr;
	this->cond = cond;
}

Cond::~Cond() {
}

void Cond::setValues(AttributeType type, int size, int condAttr,
		void const* cond) {
	this->name = "";
	this->type = type;
	this->size = size;
	this->sorted = false;
	this->condAttr = condAttr;
	this->cond = cond;
}

bool Cond::check(void* val) {
	cout << "WARNING: CALL TO GENERIC COND.CHECK!" << endl;
	return false;
}

Greater::Greater(AttributeType type, int size, int condAttr, void const* cond) :
		Cond(type, size, condAttr, cond) {
}

bool Greater::check(void* val) {
	if (this->type == INT_TYPE) {
		return (*(int*) val) > (*(int*) cond);
	} else if (this->type == FLOAT_TYPE) {
		return (*(float*) val) > (*(float*) cond);
	} else if (this->type == CHAR_TYPE) {
		string l((char*) val, this->size);
		string r((char*) cond, this->size);
		int c = l.compare(r);
		if (c > 0) {
			return true;
		}
	}
	return false;
}

Less::Less(AttributeType type, int size, int condAttr, void const* cond) :
		Cond(type, size, condAttr, cond) {
}

bool Less::check(void* val) {
	if (this->type == INT_TYPE) {
		return (*(int*) val) < (*(int*) cond);
	} else if (this->type == FLOAT_TYPE) {
		return (*(float*) val) < (*(float*) cond);
	} else if (this->type == CHAR_TYPE) {
		string l((char*) val, this->size);
		string r((char*) cond, this->size);
		int c = l.compare(r);
		if (c < 0) {
			return true;
		}
	}
	return false;
}

Equal::Equal(AttributeType type, int size, int condAttr, void const* cond) :
		Cond(type, size, condAttr, cond) {
}

bool Equal::check(void* val) {
	if (this->type == INT_TYPE) {
		return (*(int*) val) == (*(int*) cond);
	} else if (this->type == FLOAT_TYPE) {
		return (*(float*) val) == (*(float*) cond);
	} else if (this->type == CHAR_TYPE) {
		string l((char*) val, this->size);
		string r((char*) cond, this->size);
		int c = l.compare(r);
		if (c == 0) {
			return true;
		}
	}
	return false;
}

Like::Like(AttributeType type, int size, int condAttr, void const* cond) :
		Cond(type, size, condAttr, cond) {
}

bool Like::check(void* val) {
	return false;
}
