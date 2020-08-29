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
 * Window.hpp
 *
 *  Created on: Nov 27, 2017
 *      Author: martin.theobald, amal.tawakuli, vinu.venugopal
 */

#ifndef COMMUNICATION_WINDOW_HPP_
#define COMMUNICATION_WINDOW_HPP_

#include <iostream> // includes size_t

static const long THROUGHPUT = 25000000;//not used for YSB

static const int PER_SEC_MSG_COUNT = 2;

static const int EVENT_SIZE = 136; //bytes

static const int MAX_WRAPPER_SIZE = 1;

// deprecated
static const long WINDOW_SIZE = (THROUGHPUT / PER_SEC_MSG_COUNT) * EVENT_SIZE
		+ MAX_WRAPPER_SIZE + 4;

// temporary fix:
// might cause bad_alloc if too big, and segfault if too small
static const size_t DEFAULT_WINDOW_SIZE = 1000000; // 1MB

static const long AGG_WIND_SPAN = 10000; //MSEC

// raw container for windowed stream data
class Window {

public:

	char* buffer; // binary buffer

	int size; // actual size used of the binary buffer

	int capacity; // size available of the binary buffer

	Window();

	Window(int capacity);

	virtual ~Window();

	virtual void clear();

	virtual void resize(int capacity);

};

#endif /* COMMUNICATION_WINDOW_HPP_ */
