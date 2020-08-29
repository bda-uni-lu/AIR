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
 * TPCH.cpp
 *
 *  Created on: Dec 29, 2017
 *      Author: martin.theobald
 */

#include "TPCH.hpp"

#include <iostream>

#include "../communication/Window.hpp"
#include "../connector/PartRowConnector.hpp"
#include "../connector/RowConnector.hpp"
#include "../input/FileInput.hpp"
#include "../relational/Aggr.hpp"
#include "../relational/Attribute.hpp"
#include "../relational/Cond.hpp"
#include "../relational/Schema.hpp"
#include "../relational/SortAggregation.hpp"
#include "../relational/SortMergeJoin.hpp"

using namespace std;

TPCH::TPCH() {

	//---- LINEITEM ----

	Attribute* lineitemAttr = new Attribute[15];
	lineitemAttr[0].setValues("l_orderkey", INT_TYPE, 4, true);
	lineitemAttr[1].setValues("l_partkey", INT_TYPE, 4, false);
	lineitemAttr[2].setValues("l_suppkey", INT_TYPE, 4, false);
	lineitemAttr[3].setValues("l_linenumber", INT_TYPE, 4, false);
	lineitemAttr[4].setValues("l_quantity", FLOAT_TYPE, 4, false);
	lineitemAttr[5].setValues("l_extendedprice", FLOAT_TYPE, 4, false);
	lineitemAttr[6].setValues("l_discount", FLOAT_TYPE, 4, false);
	lineitemAttr[7].setValues("l_tax", FLOAT_TYPE, 4, false);
	lineitemAttr[8].setValues("l_returnflag", CHAR_TYPE, 1, false);
	lineitemAttr[9].setValues("l_linestatus", CHAR_TYPE, 1, false);
	lineitemAttr[10].setValues("l_shipdate", CHAR_TYPE, 10, false);
	lineitemAttr[11].setValues("l_commitdate", CHAR_TYPE, 10, false);
	lineitemAttr[12].setValues("l_receiptdate", CHAR_TYPE, 10, false);
	lineitemAttr[13].setValues("l_shipinstruct", CHAR_TYPE, 25, false);
	lineitemAttr[14].setValues("l_shipmode", CHAR_TYPE, 10, false);
	lineitemSchema = new Schema(lineitemAttr, 15);

	//---- CUSTOMER ----

	Attribute* customerAttr = new Attribute[5];
	customerAttr[0].setValues("c_custkey", INT_TYPE, 4, false);
	customerAttr[1].setValues("c_nationkey", INT_TYPE, 4, true);
	customerAttr[2].setValues("c_phone", CHAR_TYPE, 15, false);
	customerAttr[3].setValues("c_acctbal", FLOAT_TYPE, 4, false);
	customerAttr[4].setValues("c_mktsegment", CHAR_TYPE, 10, false);
	customerSchema = new Schema(customerAttr, 5);

	//---- PART ----

	Attribute* partAttr = new Attribute[6];
	partAttr[0].setValues("p_partkey", INT_TYPE, 4, true);
	partAttr[1].setValues("p_mfgr", CHAR_TYPE, 25, false);
	partAttr[2].setValues("p_brand", CHAR_TYPE, 10, false);
	partAttr[3].setValues("p_size", INT_TYPE, 4, false);
	partAttr[4].setValues("p_container", CHAR_TYPE, 10, false);
	partAttr[5].setValues("p_retailprize", FLOAT_TYPE, 4, false);
	partSchema = new Schema(partAttr, 6);

	//---- SUPPLIER ----

	Attribute* supplierAttr = new Attribute[5];
	supplierAttr[0].setValues("s_suppkey", INT_TYPE, 4, true);
	supplierAttr[1].setValues("s_name", CHAR_TYPE, 25, false);
	supplierAttr[2].setValues("s_nationkey", INT_TYPE, 4, true);
	supplierAttr[3].setValues("s_phone", CHAR_TYPE, 15, false);
	supplierAttr[4].setValues("s_acctbal", FLOAT_TYPE, 4, false);
	supplierSchema = new Schema(supplierAttr, 5);

	//---- ORDERS ----

	Attribute* ordersAttr = new Attribute[8];
	ordersAttr[0].setValues("o_orderkey", INT_TYPE, 4, false);
	ordersAttr[1].setValues("o_custkey", INT_TYPE, 4, true);
	ordersAttr[2].setValues("o_orderstatus", CHAR_TYPE, 1, false);
	ordersAttr[3].setValues("o_totalprice", FLOAT_TYPE, 4, false);
	ordersAttr[4].setValues("o_orderdate", CHAR_TYPE, 10, false);
	ordersAttr[5].setValues("o_orderpriority", CHAR_TYPE, 15, false);
	ordersAttr[6].setValues("o_clerk", CHAR_TYPE, 15, false);
	ordersAttr[7].setValues("o_shippriority", INT_TYPE, 4, false);
	ordersSchema = new Schema(ordersAttr, 8);

	//---- PARTSUPP ----

	Attribute* partsuppAttr = new Attribute[4];
	partsuppAttr[0].setValues("ps_partkey", INT_TYPE, 4, true);
	partsuppAttr[1].setValues("ps_suppkey", INT_TYPE, 4, true);
	partsuppAttr[2].setValues("ps_availqty", INT_TYPE, 4, false);
	partsuppAttr[3].setValues("ps_supplycost", FLOAT_TYPE, 4, false);
	partsuppSchema = new Schema(partsuppAttr, 4);

	//---- REGION ----

	Attribute* regionAttr = new Attribute[2];
	regionAttr[0].setValues("r_regionkey", INT_TYPE, 4, true);
	regionAttr[1].setValues("r_name", CHAR_TYPE, 25, false);
	regionSchema = new Schema(regionAttr, 2);

	//---- NATION ----

	Attribute* nationAttr = new Attribute[3];
	nationAttr[0].setValues("n_nationkey", INT_TYPE, 4, false);
	nationAttr[1].setValues("n_name", CHAR_TYPE, 25, false);
	nationAttr[2].setValues("n_regionkey", INT_TYPE, 4, true);
	nationSchema = new Schema(nationAttr, 3);
}

TPCH::~TPCH() {
	delete lineitemSchema;
	delete customerSchema;
	delete partsuppSchema;
	delete partSchema;
	delete supplierSchema;
	delete nationSchema;
	delete ordersSchema;
	delete regionSchema;
}

// Q5:
//	SELECT
//	    n_name, count(*)
//	FROM
//	    customer,
//	    orders,
//	    lineitem,
//	    supplier,
//	    nation,
//	    region
//	WHERE
//	    c_custkey = o_custkey
//	    AND l_orderkey = o_orderkey
//	    AND l_suppkey = s_suppkey
//	    AND c_nationkey = s_nationkey
//	    AND s_nationkey = n_nationkey
//	    AND n_regionkey = r_regionkey
//	    AND r_name = 'ASIA'
//	    AND o_orderdate >= date '1994-01-01'
//	    AND o_orderdate < date '1994-01-01' + interval '1' year
//	GROUP BY n_name
//	ORDER BY n_name ASC;

TPCH_Q5::TPCH_Q5() :
		Dataflow() {

	int* regionJoinAttr = new int[1];
	regionJoinAttr[0] = 0;
	int* regionProjAttr = nullptr;
	int* regionShardAttr = nullptr;

	rf1 = new PartRowConnector(regionSchema, s1, regionJoinAttr, 1, 0, rank, worldSize);

	Cond** regionCond = new Cond*[1];
	char const* c = "ASIA";
	regionCond[0] = new Equal(CHAR_TYPE, 4, 1, c);

	int* nationJoinAttr = new int[1];
	nationJoinAttr[0] = 2;
	int* nationProjAttr = new int[2];
	nationProjAttr[0] = 0;
	nationProjAttr[1] = 1;
	int* nationShardAttr = new int[1];
	nationShardAttr[0] = 0;

	rf2 = new PartRowConnector(nationSchema, s2, nationJoinAttr, 1, 1, rank, worldSize);

	mj1 = new SortMergeJoin(regionSchema, nationSchema, regionCond, nullptr,
			regionJoinAttr, nationJoinAttr, regionProjAttr, nationProjAttr,
			regionShardAttr, nationShardAttr, 1, 0, 1, 0, 2, 0, 1, 2, rank,
			worldSize);

	int* customerJoinAttr = new int[1];
	customerJoinAttr[0] = 1;
	int* customerProjAttr = new int[1];
	customerProjAttr[0] = 0;
	int* customerShardAttr = new int[1];
	customerShardAttr[0] = 0;

	rf3 = new PartRowConnector(customerSchema, s3, customerJoinAttr, 1, 3, rank,
			worldSize);

	Attribute* res1Attr = new Attribute[2];
	res1Attr[0].setValues("n_nationkey", INT_TYPE, 4, false);
	res1Attr[1].setValues("n_name", CHAR_TYPE, 25, false);
	res1Schema = new Schema(res1Attr, 2);

	int* rightJoinAttr1 = new int[1];
	rightJoinAttr1[0] = 0;
	int* rightProjAttr1 = new int[2];
	rightProjAttr1[0] = 0;
	rightProjAttr1[1] = 1;
	int* rightShardAttr1 = nullptr;

	mj2 = new SortMergeJoin(customerSchema, res1Schema, nullptr, nullptr,
			customerJoinAttr, rightJoinAttr1, customerProjAttr, rightProjAttr1,
			customerShardAttr, rightShardAttr1, 0, 0, 1, 1, 2, 1, 0, 4, rank,
			worldSize);

	int* ordersJoinAttr = new int[1];
	ordersJoinAttr[0] = 1;
	int* ordersProjAttr = new int[1];
	ordersProjAttr[0] = 0;
	int* ordersShardAttr = new int[1];
	ordersShardAttr[0] = 0;

	rf4 = new PartRowConnector(ordersSchema, s4, ordersJoinAttr, 1, 5, rank, worldSize);

	Cond** ordersCond = new Cond*[2];
	ordersCond[0] = new Greater(CHAR_TYPE, 10, 4, &"1993-12-31");
	ordersCond[1] = new Less(CHAR_TYPE, 10, 4, &"1995-01-01");

	Attribute* res2Attr = new Attribute[3];
	res2Attr[0].setValues("c_custkey", INT_TYPE, 4, false);
	res2Attr[1].setValues("n_nationkey", INT_TYPE, 4, false);
	res2Attr[2].setValues("n_name", CHAR_TYPE, 25, false);
	res2Schema = new Schema(res2Attr, 3);

	int* rightJoinAttr2 = new int[1];
	rightJoinAttr2[0] = 0;
	int* rightProjAttr2 = new int[2];
	rightProjAttr2[0] = 1;
	rightProjAttr2[1] = 2;
	int* rightShardAttr2 = nullptr;

	mj3 = new SortMergeJoin(ordersSchema, res2Schema, ordersCond, nullptr,
			ordersJoinAttr, rightJoinAttr2, ordersProjAttr, rightProjAttr2,
			ordersShardAttr, rightShardAttr2, 2, 0, 1, 1, 2, 1, 0, 6, rank,
			worldSize);

	int* lineitemJoinAttr = new int[1];
	lineitemJoinAttr[0] = 0;
	int* lineitemProjAttr = new int[3];
	lineitemProjAttr[0] = 2;
	lineitemProjAttr[1] = 5;
	lineitemProjAttr[2] = 6;
	int* lineitemShardAttr = new int[1];
	lineitemShardAttr[0] = 2;

	rf5 = new PartRowConnector(lineitemSchema, s5, lineitemJoinAttr, 1, 7, rank,
			worldSize);

	Attribute* res3Attr = new Attribute[3];
	res3Attr[0].setValues("o_orderkey", INT_TYPE, 4, false);
	res3Attr[1].setValues("n_nationkey", INT_TYPE, 4, false);
	res3Attr[2].setValues("n_name", CHAR_TYPE, 25, false);
	res3Schema = new Schema(res3Attr, 3);

	int* rightJoinAttr3 = new int[1];
	rightJoinAttr3[0] = 0;
	int* rightProjAttr3 = new int[2];
	rightProjAttr3[0] = 1;
	rightProjAttr3[1] = 2;
	int* rightShardAttr3 = new int[1];
	rightShardAttr3[0] = 1;

	mj4 = new SortMergeJoin(lineitemSchema, res3Schema, nullptr, nullptr,
			lineitemJoinAttr, rightJoinAttr3, lineitemProjAttr, rightProjAttr3,
			lineitemShardAttr, rightShardAttr3, 0, 0, 1, 3, 2, 1, 1, 8, rank,
			worldSize);

	int* supplierJoinAttr = new int[2];
	supplierJoinAttr[0] = 0;
	supplierJoinAttr[1] = 2;
	int* supplierProjAttr = nullptr;
	int* supplierShardAttr = nullptr;

	rf6 = new PartRowConnector(supplierSchema, s6, supplierJoinAttr, 2, 9, rank,
			worldSize);

	Attribute* res4Attr = new Attribute[5];
	res4Attr[0].setValues("l_suppkey", INT_TYPE, 4, false);
	res4Attr[1].setValues("l_extendedprice", FLOAT_TYPE, 4, false);
	res4Attr[2].setValues("l_discount", FLOAT_TYPE, 4, false);
	res4Attr[3].setValues("n_nationkey", INT_TYPE, 4, false);
	res4Attr[4].setValues("n_name", CHAR_TYPE, 25, false);
	res4Schema = new Schema(res4Attr, 5);

	int* rightJoinAttr4 = new int[2];
	rightJoinAttr4[0] = 0;
	rightJoinAttr4[1] = 3;
	int* rightProjAttr4 = new int[3];
	rightProjAttr4[0] = 1;
	rightProjAttr4[1] = 3;
	rightProjAttr4[2] = 4;
	int* rightShardAttr4 = new int[1];
	rightShardAttr4[0] = 3;

	mj5 = new SortMergeJoin(supplierSchema, res4Schema, nullptr, nullptr,
			supplierJoinAttr, rightJoinAttr4, supplierProjAttr, rightProjAttr4,
			supplierShardAttr, rightShardAttr4, 0, 0, 2, 0, 3, 0, 1, 10, rank,
			worldSize);

	Attribute* res5Attr = new Attribute[3];
	res5Attr[0].setValues("l_extendedprice", FLOAT_TYPE, 4, false);
	res5Attr[1].setValues("n_nationkey", INT_TYPE, 4, false);
	res5Attr[2].setValues("n_name", CHAR_TYPE, 25, false);
	res5Schema = new Schema(res5Attr, 3);

	int* groupByAttr = new int[1];
	groupByAttr[0] = 1;
	int* projAttr = new int[1];
	projAttr[0] = 2;
	Aggr** aggr = new Aggr*[1];
	aggr[0] = new Count("count", FLOAT_TYPE, 4, false, 0);

	gb1 = new SortAggregation(res5Schema, aggr, nullptr, groupByAttr, projAttr,
			nullptr, 1, 0, 1, 1, 0, true, 11, rank, worldSize);

	Attribute* res6Attr = new Attribute[2];
	res6Attr[0].setValues("n_name", CHAR_TYPE, 25, false);
	res6Attr[1].setValues("count", FLOAT_TYPE, 4, false);
	res6Schema = new Schema(res6Attr, 2);

	rc1 = new RowCollector(res6Schema, 12, rank, worldSize);

	addLink(rf1, mj1);
	addLink(rf2, mj1);
	addLink(rf3, mj2);
	addLink(mj1, mj2);
	addLink(rf4, mj3);
	addLink(mj2, mj3);
	addLink(rf5, mj4);
	addLink(mj3, mj4);
	addLink(rf6, mj5);
	addLink(mj4, mj5);
	addLink(mj5, gb1);
	addLink(gb1, rc1);

	rf1->initialize();
	rf2->initialize();
	rf3->initialize();
	rf4->initialize();
	rf5->initialize();
	rf6->initialize();

	mj1->initialize();
	mj2->initialize();
	mj3->initialize();
	mj4->initialize();
	mj5->initialize();

	gb1->initialize();
	rc1->initialize();
}

void TPCH_Q5::run() { // direct way of running the query using the processLocal(...) methods

	this->rf1 = nullptr;
	this->rf2 = nullptr;
	this->rf3 = nullptr;
	this->rf4 = nullptr;
	this->rf5 = nullptr;
	this->rf6 = nullptr;
	this->mj1 = nullptr;
	this->mj2 = nullptr;
	this->mj3 = nullptr;
	this->mj4 = nullptr;
	this->mj5 = nullptr;
	this->gb1 = nullptr;
	this->rc1 = nullptr;

	Window lineitemWindow;
	FileInput c1("../data/TPCH-SF1/lineitem.bin", &lineitemWindow);
	Relation lineitem(lineitemSchema, c1.nextWindow());
	cout << "LINEITEM: " << lineitem.size << " TUPLES." << endl;
	//int lineitemSortAttr[] = { 0 };
	//lineitem->sort(lineitemSortAttr, 1);

	Window customerWindow;
	FileInput c2("../data/TPCH-SF1/customer.bin", &customerWindow);
	Relation customer(customerSchema, c2.nextWindow());
	cout << "CUSTOMER: " << customer.size << " TUPLES." << endl;
	//int customerSortAttr[] = { 1 };
	//customer->sort(customerSortAttr, 1);

	Window partWindow;
	FileInput c3("../data/TPCH-SF1/part.bin", &partWindow);
	Relation part(partSchema, c3.nextWindow());
	cout << "PART: " << part.size << " TUPLES." << endl;
	//int partSortAttr[] = { 0 };
	//part->sort(partSortAttr, 1);

	Window supplierWindow;
	FileInput c4("../data/TPCH-SF1/supplier.bin", &supplierWindow);
	Relation supplier(supplierSchema, c4.nextWindow());
	cout << "SUPPLIER: " << supplier.size << " TUPLES." << endl;
	//int supplierSortAttr[] = { 0, 2 };
	//supplier->sort(supplierSortAttr, 2);

	Window ordersWindow;
	FileInput c5("../data/TPCH-SF1/orders.bin", &ordersWindow);
	Relation orders(ordersSchema, c5.nextWindow());
	cout << "ORDERS: " << orders.size << " TUPLES." << endl;
	//int ordersSortAttr[] = { 1 };
	//orders->sort(ordersSortAttr, 1);

	Window partsuppWindow;
	FileInput c6("../data/TPCH-SF1/partsupp.bin", &partsuppWindow);
	Relation partsupp(partsuppSchema, c6.nextWindow());
	cout << "PARTSUPP: " << partsupp.size << " TUPLES." << endl;
	//int partsuppSortAttr[] = { 0, 1 };
	//partsupp->sort(partsuppSortAttr, 2);

	Window regionWindow;
	FileInput c7("../data/TPCH-SF1/region.bin", &regionWindow);
	Relation region(regionSchema, c7.nextWindow());
	cout << "REGION: " << region.size << " TUPLES." << endl;
	//int regionSortAttr[] = { 0 };
	//region->sort(regionSortAttr, 1);

	Window nationWindow;
	FileInput c8("../data/TPCH-SF1/nation.bin", &nationWindow);
	Relation nation(nationSchema, c8.nextWindow());
	cout << "NATION: " << nation.size << " TUPLES." << endl;
	//int nationSortAttr[] = { 2 };
	//nation->sort(nationSortAttr, 1);

	SortMergeJoin mj1;
	int leftJoinAttr1[] = { 0 };
	int rightJoinAttr1[] = { 2 };
	int leftProjAttr1[] = { };
	int rightProjAttr1[] = { 0, 1 };

	Cond* cond1[1];
	char const* c = "ASIA";
	cond1[0] = new Equal(CHAR_TYPE, 4, 1, c);

	Relation* join1 = mj1.join(&region, &nation, cond1, nullptr, leftJoinAttr1,
			rightJoinAttr1, leftProjAttr1, rightProjAttr1, 1, 0, 1, 0, 2);

	int sortAttr1[] = { 0 };
	join1->sort(sortAttr1, 1);
	//join1->print(25);

	SortMergeJoin mj2;
	int leftJoinAttr2[] = { 1 };
	int rightJoinAttr2[] = { 0 };
	int leftProjAttr2[] = { 0, 1 };
	int rightProjAttr2[] = { 1 };

	Relation* join2 = mj2.join(&customer, join1, nullptr, nullptr,
			leftJoinAttr2, rightJoinAttr2, leftProjAttr2, rightProjAttr2, 0, 0,
			1, 2, 1);

	int sortAttr2[] = { 0 };
	join2->sort(sortAttr2, 1);
	//join2->print(25);

	SortMergeJoin mj3;
	int leftJoinAttr3[] = { 1 };
	int rightJoinAttr3[] = { 0 };
	int leftProjAttr3[] = { 0 };
	int rightProjAttr3[] = { 1, 2 };

	Cond* cond2[2];
	cond2[0] = new Greater(CHAR_TYPE, 10, 4, &"1993-12-31");
	cond2[1] = new Less(CHAR_TYPE, 10, 4, &"1995-01-01");

	Relation* join3 = mj3.join(&orders, join2, cond2, nullptr, leftJoinAttr3,
			rightJoinAttr3, leftProjAttr3, rightProjAttr3, 2, 0, 1, 1, 2);

	int sortAttr3[] = { 0 };
	join3->sort(sortAttr3, 1);
	//join3->print(25);

	SortMergeJoin mj4;
	int leftJoinAttr4[] = { 0 };
	int rightJoinAttr4[] = { 0 };
	int leftProjAttr4[] = { 2, 5, 6 };
	int rightProjAttr4[] = { 1, 2 };

	Relation* join4 = mj4.join(&lineitem, join3, nullptr, nullptr,
			leftJoinAttr4, rightJoinAttr4, leftProjAttr4, rightProjAttr4, 0, 0,
			1, 3, 2);

	int sortAttr4[] = { 0, 3 };
	join4->sort(sortAttr4, 2);
	//join4->print(25);

	SortMergeJoin mj5;
	int leftJoinAttr5[] = { 0, 2 };
	int rightJoinAttr5[] = { 0, 3 };
	int leftProjAttr5[] = { };
	int rightProjAttr5[] = { 1, 2, 3, 4 };

	Relation* join5 = mj5.join(&supplier, join4, nullptr, nullptr,
			leftJoinAttr5, rightJoinAttr5, leftProjAttr5, rightProjAttr5, 0, 0,
			2, 0, 4);

	int sortAttr5[] = { 2 };
	join5->sort(sortAttr5, 1);
	//join5->print(25);

	SortAggregation sa;
	int groupByAttr[] = { 2 };
	int projAttr[] = { 3 };
	Aggr* aggr[1];
	aggr[0] = new Count("count", FLOAT_TYPE, 4, false, 0);

	Relation* aggrRelation = sa.groupBy(join5, aggr, nullptr, groupByAttr,
			projAttr, 1, 0, 1, 1);

	int sortAttr6[] = { 0 };
	aggrRelation->sortStrings(sortAttr6, 1);
	aggrRelation->print(25);

	delete join1->window;
	delete join1->schema;
	delete join1;

	delete join2->window;
	delete join2->schema;
	delete join2;

	delete join3->window;
	delete join3->schema;
	delete join3;

	delete join4->window;
	delete join4->schema;
	delete join4;

	delete join5->window;
	delete join5->schema;
	delete join5;

	delete aggrRelation->window;
	delete aggrRelation->schema;
	delete aggrRelation;
}

TPCH_Q5::~TPCH_Q5() {
	if (res1Schema != nullptr)
		delete res1Schema;
	if (res2Schema != nullptr)
		delete res2Schema;
	if (res3Schema != nullptr)
		delete res3Schema;
	if (res4Schema != nullptr)
		delete res4Schema;
	if (res5Schema != nullptr)
		delete res5Schema;
	if (res6Schema != nullptr)
		delete res6Schema;
	if (rf1 != nullptr)
		delete rf1;
	if (rf2 != nullptr)
		delete rf2;
	if (rf3 != nullptr)
		delete rf3;
	if (rf4 != nullptr)
		delete rf4;
	if (rf5 != nullptr)
		delete rf5;
	if (rf6 != nullptr)
		delete rf6;
	if (mj1 != nullptr)
		delete mj1;
	if (mj2 != nullptr)
		delete mj2;
	if (mj3 != nullptr)
		delete mj3;
	if (mj4 != nullptr)
		delete mj4;
	if (mj5 != nullptr)
		delete mj5;
	if (gb1 != nullptr)
		delete gb1;
	if (rc1 != nullptr)
		delete rc1;
}

TPCH_Q6::TPCH_Q6() :
		Dataflow() {
}

void TPCH_Q6::run() {

//  SELECT
//    sum(l_extendedprice * l_discount) as revenue
//  FROM
//    lineitem
//  WHERE
//    l_shipdate >= date '1994-01-01'
//    AND l_shipdate < date '1994-01-01' + interval '1' year
//    AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
//    AND l_quantity < 24;

	Window lineitemWindow;
	FileInput c1("../data/TPCH-SF1/lineitem.bin", &lineitemWindow);
	Relation lineitem(lineitemSchema, c1.nextWindow());
	cout << "LINEITEM: " << lineitem.size << " TUPLES." << endl;

	SortAggregation sa;

	Cond* cond[5];
	float f1 = 24e0;
	cond[0] = new Less(FLOAT_TYPE, 4, 4, &f1);
	cond[1] = new Greater(CHAR_TYPE, 10, 10, &"1993-12-31");
	cond[2] = new Less(CHAR_TYPE, 10, 10, &"1995-01-01");
	float f2 = 0.049e0;
	cond[3] = new Greater(FLOAT_TYPE, 4, 6, &f2);
	float f3 = 0.071e0;
	cond[4] = new Less(FLOAT_TYPE, 4, 6, &f3);

	Aggr* aggr[1];
	aggr[0] = new Count("count", FLOAT_TYPE, 4, false, 5);

	Relation* aggrRelation = sa.groupByAll(&lineitem, aggr, cond, 1, 5);
	aggrRelation->print(25);

	delete aggrRelation;
}

TPCH_Q6::~TPCH_Q6() {
	cout << "TPCH_Q6 DELETED." << endl;
}
