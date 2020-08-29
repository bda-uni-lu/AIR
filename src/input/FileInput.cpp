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
 * FileInput.cpp
 *
 *  Created on: Nov 27, 2017
 *      Author: amal.tawakuli, martin.theobald
 */

#include "../communication/Window.hpp"

#include <unistd.h>
#include <stdlib.h>
#include <ctime>
#include <cstring>

#include "FileInput.hpp"

using namespace std;

FileInput::FileInput(string file, Window* window) :
		Input() {
	this->fileName = new char[file.length()];
	this->window = window;
	strcpy(this->fileName, file.c_str());
	this->is_open = false;
	this->file_size = 0;
	this->file_pos = 0;
}

FileInput::~FileInput() {
	delete[] fileName;
}

bool FileInput::isOpen() {
	return is_open;
}

void FileInput::open() {
	//writeBinaryFileIntType(DEFAULT_WINDOW_SIZE / sizeof(int));

	dataSource.open(fileName, ios::binary | ios::in);

	if (dataSource) {

		dataSource.seekg(0, ios::end);
		file_size = dataSource.tellg();
		dataSource.seekg(0, ios::beg);
		is_open = true;

	} else {

		char cwd[1024];
		getcwd(cwd, sizeof(cwd));
		cout << "PROBLEM OPENING [" << cwd << "/" << fileName << "]" << endl;

		is_open = false;
		file_size = 0;

	}

	file_pos = 0;
}

Window* FileInput::nextWindow() {

	if (is_open) {

		if (file_pos + window->capacity < file_size) {

			dataSource.read(&window->buffer[0], window->capacity);
			window->size = window->capacity;
			file_pos += window->capacity;

		} else {

			dataSource.read(&window->buffer[0], file_size - file_pos);
			dataSource.close();
			window->size = file_size - file_pos;
			file_pos = file_size;
			is_open = false;

		}
	}

	//cout << window->size << " BYTES READ FROM [" << fileName << "]" << endl;
	return window;
}

void FileInput::close() {
	if (is_open) {
		dataSource.close();
		is_open = false;
	}
}

// Write a binary file with 'numberOfInts' many int values
void FileInput::writeBinaryFileIntType(int numberOfInts) {

	ofstream dataSource;
	dataSource.open(fileName, ios::binary | ios::out);

	if (dataSource) {

		srand(time(NULL));

		int len = sizeof(int);
		for (int i = 0; i < numberOfInts; i++) {
			int val = i; //rand();
			dataSource.write((char*) &val, len);
			//cout << "WRITE VALUE: " << val << endl;
		}

		cout << numberOfInts * sizeof(int) << " BYTES WRITTEN TO [" << fileName
				<< "]" << endl;

	} else {

		char cwd[1024];
		getcwd(cwd, sizeof(cwd));
		cout << "PROBLEM WRITING TO [" << cwd << "/" << fileName << "]" << endl;

	}
}

// Read a whole binary file into a window (size is adjusted if necessary)
void FileInput::readBinaryFile(Window* window) {

	ifstream dataSource;
	dataSource.open(fileName, ios::binary | ios::in);

	if (dataSource) {

		dataSource.seekg(0, ios::end);
		int size = dataSource.tellg();
		if (size > window->capacity)
			window->resize(size);
		window->size = size;
		dataSource.seekg(0, ios::beg);
		dataSource.read(&window->buffer[0], size);
		//cout << size << " BYTES READ FROM [" << fileName << "]" << endl;

	} else {

		char cwd[1024];
		getcwd(cwd, sizeof(cwd));
		cout << "PROBLEM READING FROM [" << cwd << "/" << fileName << "]"
				<< endl;

	}

	dataSource.close();
}
