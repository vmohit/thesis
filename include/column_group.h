#ifndef COLUMNGROUP_H
#define COLUMNGROUP_H

#include "data.h"
#include "data_map.h"
#include "index_table.h"
#include "tuple"

#include <vector>
#include <map>
#include <string>
#include <tsl/htrie_map.h>
#include <memory>
#include <iostream>
#include <boost/filesystem.hpp>

namespace {
	namespace fs = boost::filesystem;
}

// --------------------------------------------------------------------------

/** In memory data structure corresponding to a column group of an index table */
class InMemColumnGroup {
	// int id;
	std::map<uint, uint> offset_to_block_size;   //!< block size of the column group, with depth level+1, starting at 'offset'
	std::vector<std::vector<DataMap>> inmem_data;
	const IndexTable* tab;

	// Constructors
	InMemColumnGroup(const IndexTable& table, uint level, 
		uint offset, uint block_size);

	void insert_tup(const Tuple& tup, uint offset_or_mult, 
		const Tuple& last_tup);

	friend class IndexNavigator;
// public:
// 	static int max_id;
// 	~InMemColumnGroup();
};


/** A streaming data structure for reading a column group of an index table */
struct StreamColumnGroup {
	const IndexTable* tab;
	
	Buffer B;
	uint shard;
	fs::path fpath;
	uint level, f_offset, rem_bytes;
	Tuple curr_tup;
	uint curr_offset, curr_bsize;
	
	StreamColumnGroup(const IndexTable* table, uint lvl, 
		uint shrd, uint ofst, uint block_size);

	bool has_next_tup();
	Tuple next_tup();
	StreamColumnGroup next_cg();

};

#endif