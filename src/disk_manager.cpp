#include "disk_manager.h"

#include <cassert>
#include <vector>
#include <string>
#include <sstream>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <chrono>
#include <thread>
#include <memory>

namespace {
	namespace fs = boost::filesystem;
}
using std::string;
using std::vector;
using std::stringstream;
using std::shared_ptr;
using std::printf;

/**Expects path to an already existing directory*/
DiskManager::DiskManager(string path, int cs /*=0*/) {
	dir_path = fs::path(path);
	assert(fs::is_directory(dir_path));
}


/**new_dir_path is considered relative to dir_path */
bool DiskManager::mkdir_ifnotexist(fs::path new_dir_path) const{
	if(!fs::is_directory(dir_path / new_dir_path)) 
		assert(fs::create_directories(dir_path / new_dir_path));
	return true;
}

void DiskManager::delete_file_startingwith(fs::path dir, std::string prefix) const{
	fs::path target_dir = dir_path / dir;
	assert(is_directory(target_dir));
	for (auto&& x : fs::directory_iterator(target_dir))
		if(x.path().filename().string().find(prefix)==0)
			fs::remove(x.path());
}

void DiskManager::create_file(fs::path fpath) const{
	assert(!fs::exists(dir_path / fpath));
	fs::ofstream file(dir_path / fpath);
	file.close();
	assert(fs::exists(dir_path / fpath));
}

void DiskManager::append(fs::path fpath, vector<char>& data) const {
	assert(fs::exists(dir_path / fpath));
	fs::ofstream out_file(dir_path / fpath, std::ofstream::binary | std::ofstream::app);
	out_file.write((char*)&data[0], data.size() * sizeof(char));
	out_file.close();
}

void DiskManager::write(fs::path fpath, const std::string& content) const {
	assert(fs::exists(dir_path / fpath));
	fs::ofstream out_file(dir_path / fpath, std::ofstream::out);
	out_file<<content<<std::endl;
	out_file.close();
}

string DiskManager::slurp(fs::path fpath) const {
	assert(fs::exists(dir_path / fpath));
	fs::ifstream in_file(dir_path / fpath);	
	stringstream sstr;
	sstr << in_file.rdbuf();
	in_file.close();
	return sstr.str();
}

uint DiskManager::file_size(fs::path fpath) const {
	// printf("%s %d\n", (dir_path / fpath).string().c_str(), fs::exists(dir_path / fpath));
	assert(fs::exists(dir_path / fpath));
	return fs::file_size(dir_path / fpath);
}

vector<char> DiskManager::seek_and_read(fs::path fpath, 
		uint offset, uint block_size) const {
	assert(fs::exists(dir_path / fpath));
	std::ifstream is ((dir_path / fpath).string(), std::ifstream::binary);
	assert(is);

	// seek at proper location
	is.seekg(offset, std::ios::beg);
	
	// allocate memory:
	char * buffer = new char [block_size];

	// read data as a block:
	is.read (buffer, block_size);
	if(!is) {

		std::cout << "error: only " << is.gcount() << " could be read"<<std::endl;
		std::cout << offset<<" "<<block_size<<std::endl;
		std::cout << file_size(fpath) <<std::endl;

		assert(false);
	}
	is.close();

	std::vector<char> result(buffer, buffer+block_size);
	delete[] buffer;
	return result;
}

void DiskManager::delete_dir(fs::path d_path, bool ifexists /*=false*/) const {
	if(ifexists) {
		if(fs::is_directory(dir_path / d_path))
			fs::remove_all(dir_path / d_path);
	}
	else{
		assert(fs::is_directory(dir_path / d_path));
		fs::remove_all(dir_path / d_path);
	}
}

shared_ptr<DiskConnection> DiskManager::get_connection(fs::path fpath) const {
	assert(fs::exists(dir_path / fpath));
	return shared_ptr<DiskConnection>(new DiskConnection(dir_path / fpath));
}

DiskConnection::DiskConnection(fs::path fpath) : out_file(fpath, 
	std::ofstream::binary | std::ofstream::app) {}

DiskConnection::~DiskConnection() {
	out_file.close();
}

void DiskConnection::append(std::vector<char>& data) {
	out_file.write((char*)&data[0], data.size() * sizeof(char));
}

void DiskConnection::close() {
	out_file.close();
	assert(out_file);  //!< this will indicate whether there were any errors during the writing of file
}