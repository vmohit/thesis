#include "data.h"
#include "tuple.h"
#include "index_table.h"
#include "base_relation.h"
#include "column_group.h"
#include "disk_manager.h"
#include "buffer.h"
#include "data_map.h"

#include <string>
#include <vector>
#include <cstdlib>
#include <cassert>
#include <algorithm>
#include <nlohmann/json.hpp>
#include <boost/filesystem.hpp>
#include <iostream>
#include <cstdio>
#include <queue>
#include <memory>

namespace {
	namespace fs = boost::filesystem;
}

using std::string;
using std::vector;
using std::to_string;
using std::pair;
using std::make_pair;
using std::sort;
using std::greater;
using json = nlohmann::json;
using std::printf;
using std::shared_ptr;
using std::make_shared;
using std::priority_queue;
using std::cout;
using std::endl;

template<class T> using min_heap = priority_queue<T, std::vector<T>, std::greater<T>>;


/*------ IndexTable functions -----------*/

// Constructors

/**the "string" path will be treated relative to the disk manager*/
IndexTable::IndexTable(string nm, const vector<Dtype>& dtps, const vector<vector<string > >& cgs, 
	fs::path path, const DiskManager& arg_dm): name(nm), dm(arg_dm) {
	uint total_cols=0;
	for(auto cg: cgs) {
		total_cols+=cg.size();
	}
	assert(total_cols==dtps.size());
	int start=0;
	for(auto cg: cgs) {
		col_dtypes.push_back(vector<Dtype>(dtps.begin()+start, dtps.begin()+start+cg.size()));
		start+=cg.size();
	}

	dtypes = dtps;
	col_groups = cgs;
	dir_path = path;
	dm.mkdir_ifnotexist(dir_path);
	dm.delete_file_startingwith(dir_path, name);
}

// Public interface functions
// functions used for index construction

void IndexTable::add_tuple(const Tuple& val, int sanity_checks /*=1*/) {
	if(sanity_checks==1) {
		assert(frozen==false);
		assert(check_dtypes(val));
	}
	cached_data.push_back(val);
	// cout<<cached_data.size()<<endl;
	if(cached_data.size()==max_cache_size) {
		flush();
	}
}

string IndexTable::show_cache(int verbose /*=0*/) const{
	string result="[{";
	int i=0;
	for(auto cg: col_groups) {
		for(auto name: cg) {
			result.append("("+name+","+dtype_to_str(dtypes[i])+"), ");
			i++;
		}
	}
	result.append("}\n");

	for(auto& tup: cached_data) {
		result.append(tup.show(verbose)+"\n");
	}
	return result+"]";
}

void IndexTable::freeze() {
	if(cached_data.size()!=0)
		flush();
	merge_shards();
	frozen = true;
	auto j = json::parse(dm.slurp(dir_path / get_file_name(shard_ids.front())));
	size_of_first_cg=j["size_of_first_cg"];
}

// Private utility functions

std::string IndexTable::get_file_name(int shard_id, int col_group/*=-1*/) const {
	assert(col_group>=-1);
	string shard_name = name+"-"+to_string(shard_id);
	if(col_group==-1)
		return shard_name+".txt";
	else
		return shard_name+"-"+to_string(col_group)+".bin";
}

bool IndexTable::check_dtypes(const Tuple& tup) {
	vector<Dtype> tup_dtypes = tup.get_dtypes();
	if(dtypes.size()!=tup_dtypes.size())
		return false;
	else {
		for(uint i=0; i<dtypes.size(); i++) {
			if(dtypes[i]!=tup_dtypes[i] || tup.get(i).isdiff())
				return false;
		}
		return true;
	}
}

/**this function returns (offset, block_size) for the first tuple but for subsequent tuples it returns (0, block_size)*/
pair<Tuple, pair<uint,uint>> IndexTable::decode_tuple(Buffer& B,
	uint level, const Tuple& prev_tup) const {
	if(level+1<col_groups.size()) {
		uint curr_offset=0;
		if(prev_tup.size()==0)
			curr_offset = B.pop_front();
		uint bsize = B.pop_front();
		Tuple curr_tup(&B, col_dtypes[level]);
		if(prev_tup.size()!=0)
			curr_tup = prev_tup + curr_tup;
		return make_pair(curr_tup, make_pair(curr_offset, bsize));
	}
	else {
		uint mult = B.pop_front();
		Tuple curr_tup(&B, col_dtypes[level]);
		if(prev_tup.size()!=0)
			curr_tup = prev_tup + curr_tup;
		curr_tup.set_multiplicity(mult);
		return make_pair(curr_tup, make_pair(mult, mult));
	}
}


/**This function appends the tuples in data where each tuple is for column
groups starting from level. It returns the size of data block that the "level"^th
column group of "data" ended up taking.
"data" is sorted in a descending order*/
uint IndexTable::flush(uint level, vector<Tuple>& data) {
	assert(level<col_groups.size());
	assert(data.size()!=0);
	
	// printf("=====line 136 index table code=====\n");
	// for(auto& tup: data)
	// 	printf("%s\n", tup.show().c_str());
	// printf("-----------\n");

	Tuple curr_tup, prev_tup;
	vector<Tuple> curr_data;
	Buffer B;
	while(data.size()!=0) {
		auto tups = data.back().split(col_groups[level].size());
		curr_tup = tups.first;
		data.pop_back();
		curr_data.push_back(tups.second);
		while(data.size()!=0) {
			tups = data.back().split(col_groups[level].size());
			if(curr_tup==tups.first) {
				curr_tup.add_multiplicity(tups.first.get_multiplicity());
				curr_data.push_back(tups.second);
				data.pop_back();
			}
			else 
				break;
		}
		//!< storing direct offset rather than block size for the first tuple of the block
		if(prev_tup.size()==0 && level+1 < col_groups.size()) 
			B.push(dm.file_size(dir_path / get_file_name(max_shard_id, level+1)));

		//!< storing either the block size or the offset depending on whether it is last level or not
		if(level+1<col_groups.size()) {
			sort(curr_data.begin(), curr_data.end(), greater<Tuple>());
			B.push(flush(level+1, curr_data));
		}
		else
			B.push(curr_tup.get_multiplicity());

		//!< For the first tuple, "curr_tup-prev_tup" is same as "curr_tup" as prev_tup is empty at that point
		(curr_tup-prev_tup).write_buffer(&B); 
		
		prev_tup = curr_tup;
	}
	// printf("=====line 173 index table=====\n");
	vector<char> bytes = B.flush();
	dm.append(dir_path / get_file_name(max_shard_id, level), bytes);
	return bytes.size();
}

void IndexTable::flush() {
	//!< create all the files for the current shard
	dm.create_file(dir_path / get_file_name(max_shard_id));
	for(uint i=0; i<col_groups.size(); i++) {
		dm.create_file(dir_path / get_file_name(max_shard_id, i));
	}

	//!< flush the sorted data from cache
	sort(cached_data.begin(), cached_data.end(), greater<Tuple>());

	uint num_bytes = flush(0, cached_data);
	json stats;
	stats["size_of_first_cg"] = num_bytes;
	dm.write(dir_path / get_file_name(max_shard_id), stats.dump(4));

	shard_ids.push(max_shard_id);
	max_shard_id++;
	cached_data.clear();
}


uint IndexTable::merge_cg(vector<StreamColumnGroup> streams, 
		vector<shared_ptr<DiskConnection>>* connections) {
	
	assert(connections!=NULL);
	min_heap<pair<Tuple, uint>> first_tuples;

	for(uint i=0; i<streams.size(); i++) {
		assert(streams[i].has_next_tup());
		// printf("(%d, %d), ", streams[i].shard, streams[i].level);
		first_tuples.push(make_pair(streams[i].next_tup(), i));
	}
	// printf("\n");

	Buffer out_buffer;
	Tuple curr_tup;
	uint final_block_size = 0;
	uint level = streams[0].level;
	while(!first_tuples.empty()) {
		auto equal_tuples = vector<pair<Tuple, uint>>{first_tuples.top()};
		first_tuples.pop();
		while(!first_tuples.empty())
			if(equal_tuples[0].first==first_tuples.top().first) {
				equal_tuples.push_back(first_tuples.top());
				first_tuples.pop();
			}
			else
				break;
		if(curr_tup.size()==0 && level < col_groups.size()-1)
			out_buffer.push(dm.file_size(dir_path / get_file_name(max_shard_id, level+1)));
		if(level==col_groups.size()-1) {
			uint mult=0;
			for(auto& tup: equal_tuples)
				mult+=tup.first.get_multiplicity();
			out_buffer.push(mult);
		}
		else {
			vector<StreamColumnGroup> arg_streams;
			for(auto& tup: equal_tuples) {
				arg_streams.push_back(streams[tup.second].next_cg());
			}
			out_buffer.push(merge_cg(arg_streams, connections));
		}
		(equal_tuples[0].first-curr_tup).write_buffer(&out_buffer);
		curr_tup = equal_tuples[0].first;

		for(auto& tup: equal_tuples) 
			if(streams[tup.second].has_next_tup()) 
				first_tuples.push(make_pair(streams[tup.second].next_tup(), tup.second));

		if(out_buffer.size()>max_buffer_size) {
			auto bytes = out_buffer.flush();
			final_block_size+=bytes.size();
			(*connections)[level]->append(bytes);
		}
	}
	auto bytes = out_buffer.flush();
	if(bytes.size()!=0) {
		final_block_size+=bytes.size();
		(*connections)[level]->append(bytes);	
	}
	return final_block_size;
}


void IndexTable::merge_shards() {
	while(shard_ids.size()>1) { 
		vector<uint> shards;
		while(!shard_ids.empty() && shards.size()<num_merge_shards){
			shards.push_back(shard_ids.front());
			shard_ids.pop();
		}
		vector<uint> block_sizes(shards.size());
		vector<StreamColumnGroup> streams;
		for(uint i=0; i<shards.size(); i++) {
			auto j = json::parse(dm.slurp(dir_path / get_file_name(shards[i])));
			block_sizes[i] = (uint) j["size_of_first_cg"];
			streams.push_back(StreamColumnGroup(this, 0, shards[i], 0, block_sizes[i]));
		}

		vector<shared_ptr<DiskConnection>> connections;

		dm.create_file(dir_path / get_file_name(max_shard_id));
		for(uint level=0; level<col_groups.size(); level++) {
			dm.create_file(dir_path / get_file_name(max_shard_id, level));
			connections.push_back(dm.get_connection(dir_path / get_file_name(max_shard_id, level)));
		}


		uint num_bytes = merge_cg(streams, &connections);
		
		for(uint level=0; level<connections.size(); level++) 
			connections[level]->close();

		json stats;
		stats["size_of_first_cg"] = num_bytes;
		dm.write(dir_path / get_file_name(max_shard_id), stats.dump(4));
		shard_ids.push(max_shard_id);
		max_shard_id++;

		for(uint i=0; i<shards.size(); i++)
			dm.delete_file_startingwith(dir_path, name+"-"+to_string(shards[i]));
	}
}

// getters and setters

vector<Dtype> IndexTable::get_dtypes() const {
	return dtypes;
} 

string IndexTable::get_name() const {
	return name;
}

vector<string> IndexTable::get_col_names() const {
	vector<string> names;
	for(uint i=0; i<col_groups.size(); i++)
		for(uint j=0; j<col_groups[i].size(); j++)
			names.push_back(col_groups[i][j]);
	return names;
}


/*------ IndexNavigator functions -----------*/

IndexNavigator::IndexNavigator(const IndexTable* tab, uint lvl,
	uint ofst, uint clno, uint arg_idx, uint block_size) {
	assert(tab!=NULL);
	level=lvl; offset=ofst; col_no = clno; idx = arg_idx;
	col_group.reset(new InMemColumnGroup(
		*tab, level, offset, block_size)); //!< transfer from disk to memory the appropriate column group
}

IndexNavigator::IndexNavigator(shared_ptr<InMemColumnGroup> cg, uint lvl,
	uint ofst, uint clno, uint arg_idx) : col_group(cg) {
	level=lvl; offset=ofst; col_no = clno; idx = arg_idx;
}

IndexNavigator::IndexNavigator(const IndexTable* tab) {
	assert(tab!=NULL);
	level=0; offset=0; col_no = 0; idx = 0;
	col_group.reset(new InMemColumnGroup(
		*tab, 0, 0, tab->size_of_first_cg)); //!< transfer from disk to memory the first column group
}


vector<Data> IndexNavigator::scan() const {
	return col_group->inmem_data[col_no][idx].scan();
}

vector<pair<Data,uint>> IndexNavigator::dump() {
	return col_group->inmem_data[col_no][idx].dump();
}

pair<bool, IndexNavigator> IndexNavigator::look_up(const Data& dt) const { 
	//printf("%d, %d\n", level, col_no);
	assert(dt.get_dtype()==col_group->tab->col_dtypes[level][col_no]);
	auto values = col_group->inmem_data[col_no][idx].look_up(dt);
	bool present = values.first;	
	if(!present)
		return make_pair(false, *this);
	else {
		if(col_no+1<col_group->inmem_data.size())
			return make_pair(present, 
				IndexNavigator(col_group, level, offset, col_no+1, values.second));
		else {
			assert(level+1<col_group->tab->col_dtypes.size());
			return make_pair(present, 
				IndexNavigator(col_group->tab, level+1, values.second, 
					0, 0, col_group->offset_to_block_size[values.second]));
		}
	}
}  