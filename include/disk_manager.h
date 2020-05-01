#ifndef DISKMANAGER_H
#define DISKMANAGER_H

#include <string>
#include <boost/filesystem.hpp>
#include <memory>

namespace {
	namespace fs = boost::filesystem;
}

class DiskConnection;

/** All disk operations must go through instances of this class*/
class DiskManager {
	int cache_size;  
	fs::path dir_path;  //!< root directory. All file names are relative to this directory.

public:
	DiskManager(std::string path, int cs=0); 

	// utilities
	bool mkdir_ifnotexist(fs::path new_dir_path) const;
	void delete_file_startingwith(fs::path dir, std::string prefix) const;
	void create_file(fs::path fpath) const;
	void append(fs::path fpath, std::vector<char>& data) const;
	void write(fs::path fpath, const std::string& content) const;
	std::string slurp(fs::path fpath) const;
	uint file_size(fs::path fpath) const;
	std::vector<char> seek_and_read(fs::path fpath, 
		uint offset, uint block_size) const;
	void delete_dir(fs::path d_path, bool ifexists=false) const;

	std::shared_ptr<DiskConnection> get_connection(fs::path fpath) const;
};

class DiskConnection {
	fs::ofstream out_file;
	DiskConnection(fs::path fpath);
public:
	friend DiskManager;
	void append(std::vector<char>& data);
	void close();
	
	~DiskConnection();
};

#endif