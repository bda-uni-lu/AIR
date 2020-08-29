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
 * Cond.hpp
 *
 *  Created on: Dec 26, 2017
 *      Author: martin.theobald
 */

#ifndef RELATIONAL_COND_HPP_
#define RELATIONAL_COND_HPP_

#include "Attribute.hpp"

class Cond: public Attribute {

public:

	int condAttr;

	void const* cond;

	Cond();

	Cond(AttributeType type, int size, int condAttr, void const* cond);

	virtual ~Cond();

	virtual bool check(void* val);

	void setValues(AttributeType type, int size, int condAttr, void const* cond);

};

class Greater: public Cond {

public:

	Greater(AttributeType type, int size, int condAttr, void const* cond);

	bool check(void* val);

};

class Less: public Cond {

public:

	Less(AttributeType type, int size, int condAttr, void const* cond);

	bool check(void* val);

};

class Equal: public Cond {

public:

	Equal(AttributeType type, int size, int condAttr, void const* cond);

	bool check(void* val);

};

class Like: public Cond {

public:

	Like(AttributeType type, int size, int condAttr, void const* cond);

	bool check(void* val);

};

#endif /* RELATIONAL_COND_HPP_ */
