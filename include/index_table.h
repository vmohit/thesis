#ifndef INDEXTABLE_H
#define INDEXTABLE_H

#include "data.h"
#include "tuple.h"
#include "disk_manager.h"
#include "buffer.h"
#include "data_map.h"

#include <vector>
#include <string>
#include <queue>
#include <boost/filesystem.hpp>
#include <map>
#include <memory>

namespace {
	namespace fs = boost::filesystem;
}

class InMemColumnGroup;
class StreamColumnGroup;

/** class for index tables.*/
class IndexTable {

	fs::path dir_path;
	std::string name;
	std::vector<Dtype> dtypes;  
	std::vector<std::vector<Dtype > > col_dtypes;
	std::vector<std::vector<std::string > > col_groups;
	const DiskManager& dm;

	// variables used during construction
	std::vector<Tuple> cached_data;  //!< array of tuples held in main memory during the construction phase
	uint max_cache_size=100000;  //!< max number of tuples held in memory
	std::queue<uint> shard_ids;
	uint max_shard_id = 1;
	uint num_merge_shards = 100;
	uint min_buffer_size = 10000;  //!< during shard merging, 'min_buffer_size' number of bytes are transfered from disk to memory at a time
	uint tuple_size_ub = 1000; //!< upper bound on tuple size in bits. If size of internal buffer falls below this threshold, new data is transfered from disk to memory
	uint max_buffer_size = 320000;  //!< if buffer size becomes higher than this, then we flush it in the disk
	bool frozen=false;

	// code used for index navigation
	uint size_of_first_cg;

	// private utility functions
	bool check_dtypes(const Tuple& tup); //!< checks if the datatype of tuple matches that of index table
	uint flush(uint level, std::vector<Tuple>& data);
	void flush();  //!< transfers the tuples into a new shard
	uint merge_cg(std::vector<StreamColumnGroup> streams, 
		std::vector<std::shared_ptr<DiskConnection>>* connections);
	void merge_shards();
	std::string get_file_name(int shard_id, int col_group=-1) const;
	std::pair<Tuple, std::pair<uint,uint>> decode_tuple(Buffer& B,
		uint level, const Tuple& prev_tup) const;
public:
	friend class IndexNavigator;
	friend class InMemColumnGroup;
	friend class StreamColumnGroup;
	IndexTable(std::string nm, const std::vector<Dtype>& dtps, 
		const std::vector<std::vector<std::string > >& cgs,
		fs::path path,
		const DiskManager& arg_dm);
	
	// functions used for the construction of IndexTable instance
	void add_tuple(const Tuple& val, int sanity_checks=1);  //!< add a tuple to the index table
	void freeze(); //!< freeze the contents of the index table. After calling this function, the index table becomes read-only accessible using IndexNavigator
	std::string show_cache(int verbose=0) const; //!< show cache contents for debugging

	// getters and setters
	std::vector<Dtype> get_dtypes() const;
	std::string get_name() const;
	std::vector<std::string > get_col_names() const;

};


/**Light-weight object used to navigate an index table*/
class IndexNavigator {
	std::shared_ptr<InMemColumnGroup> col_group;
	uint level, offset, col_no, idx;

	IndexNavigator(const IndexTable* tab, uint lvl,
		uint ofst, uint clno, uint arg_idx, uint block_size);
	IndexNavigator(std::shared_ptr<InMemColumnGroup> cg, uint lvl,
		uint ofst, uint clno, uint arg_idx);
public:
	IndexNavigator(const IndexTable* tab);
	std::vector<Data> scan() const;
	std::vector<std::pair<Data,uint>> dump();
	std::pair<bool,IndexNavigator> look_up(const Data& dt) const;
};


#endif