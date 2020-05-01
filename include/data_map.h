#ifndef DATAMAP_H
#define DATAMAP_H

#include "data.h"

#include <vector>
#include <map>
#include <string>
#include <tsl/htrie_map.h>

// --------------------------------------------------------------------------

/** Similar to STL map for the instances of "Data" */
class DataMap {
	Dtype dtype;
	std::map<int, uint> map_int_vals;  
	tsl::htrie_map<char, uint> trie_str_vals;
	std::string key_buffer;
	std::vector<Data> scanned_data;
	std::vector<std::pair<Data, uint>> dumped_data;
	bool frozen=false;

public:
	// Constructors
	DataMap(Dtype dtp);

	// Usage functions
	void insert(const Data& dt, const uint& val);
	std::vector<Data> scan();
	std::vector<std::pair<Data,uint>> dump();
	std::pair<bool,uint> look_up(const Data& dt);
};

#endif