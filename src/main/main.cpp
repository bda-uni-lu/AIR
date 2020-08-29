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

/**
 * Copyright 2020 the original author or authors from the AIR project.
 *
 * This file is part of the AIR project, see https://gitlab.uni.lu/mtheobald/AIR
 * for more information.
 *
 * Licensed under the BSD 4-Clause License;
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://spdx.org/licenses/BSD-4-Clause.html
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cstdlib>
#include <ctime>
#include <iostream>

#include "../usecases/MapReduce.hpp"
#include "../usecases/StreamingTest.hpp"
#include "../usecases/TPCH.hpp"
#include "../usecases/WinAgg.hpp"
#include "../usecases/YSB.hpp"
#include "../usecases/YSB_m.hpp"

using namespace std;

int main(int argc, char* argv[]) {

	Dataflow* dataflow = nullptr;
	bool batchflag = false;
	unsigned long tp = 10000;

	if (argc > 1) {

		if (argc > 2) {
			tp = atol(argv[2]);
		}

		string s(argv[1]);

		if (s.compare("TPCH") == 0) {

			dataflow = new TPCH_Q5();
			batchflag = true;

		} else if (s.compare("MR") == 0) {

			dataflow = new MapReduce();
			batchflag = true;

		} else if (s.compare("YSB") == 0) {

			dataflow = new YSB(tp);

		} else if (s.compare("YSBM") == 0) {

			dataflow = new YSB_m(tp);

		} else if (s.compare("WA") == 0) {

			dataflow = new WinAgg(tp);
		}

	} else {

		dataflow = new StreamingTest();
	}

	clock_t start, startbatch;
	start = clock();

	// Iterative batch processing (completely synchronized between input windows)
	int i = 0;
	if (batchflag) {

		for (i = 0; i < 30; i++) { //repeat 30x
			startbatch = clock();

			double batchlatency = (clock() - startbatch)
					/ (double) CLOCKS_PER_SEC * 1000; //batch latency calculation

			cout << "BATCH-" << i + 1 << " COMPLETED IN " << batchlatency
					<< " MSEC (" << batchlatency / 60000 << " MIN) @ "
					<< dataflow->rank << endl;
		}

	} else {

		// Stream processing (asynchronous across input windows)
		dataflow->streamProcess();

	}

	cout << "DATAFLOW COMPLETED IN "
			<< ((clock() - start) / (double) CLOCKS_PER_SEC * 1000) << " MSEC ("
			<< ((clock() - start) / (double) CLOCKS_PER_SEC * 1000) / 60000
			<< " MIN) @ " << dataflow->rank << endl;

	delete dataflow;

	return 0;
}
