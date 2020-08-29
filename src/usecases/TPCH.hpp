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
 * TPCH.hpp
 *
 *  Created on: Dec 29, 2017
 *      Author: martin.theobald
 */

#ifndef USECASES_TPCH_HPP_
#define USECASES_TPCH_HPP_

#include <string>

#include "../collector/RowCollector.hpp"
#include "../connector/PartRowConnector.hpp"
#include "../connector/RowConnector.hpp"
#include "../dataflow/Dataflow.hpp"
#include "../relational/Schema.hpp"
#include "../relational/SortAggregation.hpp"
#include "../relational/SortMergeJoin.hpp"

using namespace std;

class TPCH {

public:

	const string s1 = "../data/parts4/region.bin";
	const string s2 = "../data/parts4/nation.bin";
	const string s3 = "../data/parts4/customer.bin";
	const string s4 = "../data/parts4/orders.bin";
	const string s5 = "../data/parts4/lineitem.bin";
	const string s6 = "../data/parts4/supplier.bin";

	Schema* lineitemSchema;
	Schema* customerSchema;
	Schema* partsuppSchema;
	Schema* partSchema;
	Schema* supplierSchema;
	Schema* nationSchema;
	Schema* ordersSchema;
	Schema* regionSchema;

	TPCH();

	~TPCH();

};

class TPCH_Q5: public TPCH, public Dataflow {

public:

	Schema* res1Schema;
	Schema* res2Schema;
	Schema* res3Schema;
	Schema* res4Schema;
	Schema* res5Schema;
	Schema* res6Schema;

	PartRowConnector* rf1;
	PartRowConnector* rf2;
	PartRowConnector* rf3;
	PartRowConnector* rf4;
	PartRowConnector* rf5;
	PartRowConnector* rf6;

	SortMergeJoin* mj1;
	SortMergeJoin* mj2;
	SortMergeJoin* mj3;
	SortMergeJoin* mj4;
	SortMergeJoin* mj5;

	SortAggregation* gb1;
	RowCollector* rc1;

	TPCH_Q5();

	~TPCH_Q5();

	void run();

};

class TPCH_Q6: public TPCH, public Dataflow {

public:

	TPCH_Q6();

	~TPCH_Q6();

	void run();

};

#endif /* USECASES_TPCH_HPP_ */
