#include "column_group.h"
#include "index_table.h"
#include "tuple.h"
#include "buffer.h"

#include <cassert>
#include <memory>
#include <iostream>
#include <cstdio>
#include <algorithm>
#include <cstdlib>

using std::pair;
using std::make_pair;
using std::min;

// private helper functions

void InMemColumnGroup::insert_tup(const Tuple& tup, uint offset_or_mult, 
	const Tuple& last_tup) {
	assert(inmem_data.size()==tup.size());
	
	// std::printf("// %s \n",tup.show().c_str());

	uint col_no=0;   //!< least column number that we've not matched the new tuple with the last inserted tuple
	if(last_tup.size()!=0) { //!< not inserting the first tuple
		while(col_no<tup.size()) {
			if(last_tup.get(col_no)==tup.get(col_no)) 
				col_no++;
			else
				break;
		}
		assert(col_no<tup.size());  //!< we shouldn't be able to match the full tuple unless there is a bug somewhere else
	}

	uint value;
	if(col_no+1<tup.size())
		value = inmem_data[col_no+1].size();
	else
		value = offset_or_mult;
	inmem_data[col_no].back().insert(tup.get(col_no), value);

	for(uint i=col_no+1; i<tup.size(); i++) {
		if(i+1<tup.size())
			value = inmem_data[i+1].size();
		else
			value = offset_or_mult;
		inmem_data[i].push_back(DataMap(tup.get_dtypes()[i]));
		inmem_data[i].back().insert(tup.get(i), value);
	}
}

InMemColumnGroup::InMemColumnGroup(const IndexTable& table, uint level, 
		uint offset, uint block_size) {
	assert(level<table.col_groups.size());
	tab = &table;
	Buffer B;
	B.push(table.dm.seek_and_read(table.dir_path / 
		table.get_file_name(table.shard_ids.front(), level), 
		offset, block_size));	
	assert(B.size()!=0);
	inmem_data.resize(table.col_groups[level].size());
	inmem_data[0].push_back(DataMap(table.col_dtypes[level][0]));

	uint curr_offset, bsize;
	Tuple curr_tup, last_tup;
	auto result = table.decode_tuple(B, level, last_tup);
	curr_tup = result.first;
	if(level+1<table.col_groups.size()) {
		curr_offset = result.second.first;
		bsize = result.second.second;
		insert_tup(curr_tup, curr_offset, last_tup);
		offset_to_block_size[curr_offset] = bsize;
		curr_offset += bsize;
	}
	else 
		insert_tup(curr_tup, result.second.second, last_tup);
	

	while(B.size()!=0) {
		last_tup = curr_tup;
		result = table.decode_tuple(B, level, last_tup);
		curr_tup = result.first;
		if(level+1<table.col_groups.size()) {
			bsize = result.second.second;
			insert_tup(curr_tup, curr_offset, last_tup);
			offset_to_block_size[curr_offset] = bsize;
			curr_offset += bsize;
		}
		else 
			insert_tup(curr_tup, result.second.second, last_tup);
	}
	// id = max_id;
	// InMemColumnGroup::max_id++;
	// std::printf("Constructed column group with id %d\n", id);
}



StreamColumnGroup::StreamColumnGroup(const IndexTable* table, uint lvl, 
		uint shrd, uint ofst, uint block_size) : tab(table), shard(shrd), level(lvl), 
		f_offset(ofst), rem_bytes(block_size) {
	fpath = tab->dir_path / tab->get_file_name(shard, level);
}

bool StreamColumnGroup::has_next_tup() {
	if(B.size()<tab->tuple_size_ub && rem_bytes!=0) {
		uint num_bytes = min(tab->min_buffer_size, rem_bytes);
		auto chunk_data = tab->dm.seek_and_read(fpath, f_offset, num_bytes);
		B.push(chunk_data);
		rem_bytes -= chunk_data.size();
		f_offset += chunk_data.size();
	}
	return B.size()!=0;
}

Tuple StreamColumnGroup::next_tup() {
	assert(has_next_tup());
	auto result = tab->decode_tuple(B, level, curr_tup);
	if(curr_tup.size()==0) {
		curr_offset = result.second.first; 
	}
	else {
		curr_offset += curr_bsize;
	}
	curr_bsize = result.second.second;
	curr_tup = result.first;
	return curr_tup;
}

StreamColumnGroup StreamColumnGroup::next_cg() {
	assert(level+1<tab->col_dtypes.size());
	return StreamColumnGroup(tab, level+1, shard, curr_offset, curr_bsize);
}