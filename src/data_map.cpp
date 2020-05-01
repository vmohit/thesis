#include "data_map.h"
#include "data.h"

#include <vector>
#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include <cassert>

using std::pair;
using std::make_pair;
using std::vector;
using std::printf;
using std::sort;

// Constructors
DataMap::DataMap(Dtype dtp) : dtype(dtp) {}


void DataMap::insert(const Data& dt, const uint& val) {
	assert(dtype == dt.get_dtype());
	assert(dt.isdiff()==false);
	assert(!frozen);
	if(dtype==Dtype::Int) {
		assert(map_int_vals.find(dt.get_int_val()) == map_int_vals.end());
		map_int_vals[dt.get_int_val()] = val;
	}
	else {
		// printf("// %s\n", dt.show().c_str());
		assert(trie_str_vals.find(dt.get_str_val()) == trie_str_vals.end());
		trie_str_vals[dt.get_str_val()] = val;
	}
}

vector<Data> DataMap::scan() {
	if(scanned_data.size()==0) {
		scanned_data.clear();
		if(dtype==Dtype::Int) {
			for(auto it=map_int_vals.begin(); it!=map_int_vals.end(); it++)
				scanned_data.push_back(Data(it->first));
		}
		else {
			for(auto it=trie_str_vals.begin(); it!=trie_str_vals.end(); it++) {
				it.key(key_buffer);
				scanned_data.push_back(Data(key_buffer));
				sort(scanned_data.begin(), scanned_data.end());
			}
		}	
	}
	
	return scanned_data;
}

vector<pair<Data,uint>> DataMap::dump() {
	if(dumped_data.size()==0) {
		dumped_data.clear();
		if(dtype==Dtype::Int) {
			for(auto it=map_int_vals.begin(); it!=map_int_vals.end(); it++)
				dumped_data.push_back(make_pair(Data(it->first), it->second));
		}
		else {
			for(auto it=trie_str_vals.begin(); it!=trie_str_vals.end(); it++) {
				it.key(key_buffer);
				dumped_data.push_back(make_pair(Data(key_buffer), it.value()));
				sort(dumped_data.begin(), dumped_data.end());
			}
		}
	}
	
	return dumped_data;
}

pair<bool,uint> DataMap::look_up(const Data& dt) {
	assert(dt.get_dtype()==dtype);
	assert(dt.isdiff()==false);
	bool answer_b;
	uint answer_idx=0;
	if(dtype==Dtype::Int) {
		auto it = map_int_vals.find(dt.get_int_val());
		answer_b = (it!=map_int_vals.end());
		if(answer_b)
			answer_idx = it->second;
	}
	else {
		auto it = trie_str_vals.find(dt.get_str_val());
		answer_b = (it!=trie_str_vals.end());
		if(answer_b)
			answer_idx = it.value();
	}
	return make_pair(answer_b, answer_idx);
}