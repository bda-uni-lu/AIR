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
 * FileOutput.cpp
 *
 *  Created on: Aug 9, 2018
 *      Author: martin.theobald
 */

#include "FileOutput.hpp"

#include "../communication/Window.hpp"

#include <unistd.h>
#include <stdlib.h>
#include <ctime>
#include <fstream>
#include <iostream>
#include <cstring>

using namespace std;

FileOutput::FileOutput(string file, Window* window) :
		Output() {
	this->fileName = new char[file.length()];
	this->window = window;
	strcpy(this->fileName, file.c_str());
}

FileOutput::~FileOutput() {
	delete[] fileName;
}

void FileOutput::open() {
}

Window* FileOutput::writeWindow() {
	writeBinaryFile(window);
	return window;
}

void FileOutput::close() {
}

// Write a window into a binary file (size is adjusted if necessary)
void FileOutput::writeBinaryFile(Window* window) {

	ofstream dataSource;
	dataSource.open(fileName, ios::binary | ios::out);

	if (dataSource) {

		dataSource.write(window->buffer, window->size);

		cout << window->size << " BYTES WRITTEN TO [" << fileName << "]"
				<< endl;

	} else {

		char cwd[1024];
		getcwd(cwd, sizeof(cwd));

		cout << "PROBLEM WRITING TO [" << cwd << "/" << fileName << "]" << endl;

	}
}
