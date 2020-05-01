#include "index.h"
#include "expression_tree.h"
#include "data.h"

#include <vector>
#include <queue>
#include <algorithm>
#include <cstdlib>
#include <cassert>
#include <string>
#include <map>
#include <iostream>

using std::priority_queue;
using std::pair;
using std::vector;
using std::make_pair;
using std::string;
using std::to_string;
using std::cout;
using std::endl;
using std::map;
using std::max;

template<class T> using min_heap = priority_queue<T, std::vector<T>, std::greater<T>>;

uint Index::max_first_cg_size=0;
uint Index::max_last_cg_size=0;

Index::Index(const ExpressionTree argT) : ExpressionTree(argT) {
	levels.resize(nodes.size());
	compute_levels(root_id);

	// identify first column
	min_heap<pair<uint, pair<float, uint>>> first_cg_cols;    //!< minimize depth and maximize cardinality for first column groups
	for(uint nid=0; nid<nodes.size(); nid++) {
		name += nodes[nid].name;
		float card=total_cardinalities[nid];
		for(uint i=0; i<children_ids[nid].size(); i++)
			card = card*(children_rels[nid][i]->n_rgl / nodes[nid].ent->cardinality);
		card_col[nid] = card;
		first_cg_cols.push(make_pair(levels[nid], make_pair(-1*card, (uint) nid)));	
	}
	cgs.push_back(vector<int>{(int) first_cg_cols.top().second.second});
	size_cgs.push_back(-1*first_cg_cols.top().second.first);
	first_cg_cols.pop();

	// construct first column group
	float factor=size_cgs[0];
	while(!first_cg_cols.empty()) {
		if(size_cgs[0]+factor*(-1*first_cg_cols.top().second.first) < max_first_cg_size) {
			// cout<<max_first_cg_size<<" "<<size_cgs[0]+factor*(-1*first_cg_cols.top().second.first)<<endl;
			cgs[0].push_back((int) first_cg_cols.top().second.second);
			size_cgs[0] += factor*(-1*first_cg_cols.top().second.first);
			factor*=(-1*first_cg_cols.top().second.first);
			first_cg_cols.pop();
		}
		else
			break;
	}

	min_heap<pair<uint, pair<float, uint>>> rem_cg_cols;  //!< minimize depth followed by cardinality starting from 2nd column group
	while(!first_cg_cols.empty()) {
		auto ele = first_cg_cols.top();
		rem_cg_cols.push(make_pair(ele.first, make_pair(-1*ele.second.first, ele.second.second)));
		first_cg_cols.pop();
	}

	// construct remaining column groups

	while(!rem_cg_cols.empty()) {
		uint node_id = rem_cg_cols.top().second.second;
		cgs.push_back(vector<int>{(int) node_id});
		size_cgs.push_back(rem_cg_cols.top().second.first);
		rem_cg_cols.pop();
	}

	// combine a suffix of column groups
	while(cgs.size()>=3) {
		uint n=cgs.size();
		if(size_cgs[n-2]+size_cgs[n-2]*size_cgs[n-1] <= max_last_cg_size) {
			size_cgs[n-2] = size_cgs[n-2]+size_cgs[n-2]*size_cgs[n-1];
			cgs[n-2].insert(cgs[n-2].end(), cgs[n-1].begin(), cgs[n-1].end());
			cgs.pop_back();
			size_cgs.pop_back();
		}
		else {
			break;
		}
	}
	// compute maint cost
	maint_cost = 1; factor=1;
	for(auto& cg: cgs)
		for(auto& col: cg) {
			maint_cost += factor*card_col[col];
			factor *= card_col[col];
		}

	for(auto col: cgs[0])
		firstcg.insert(col);

	for(auto cg: cgs)
		for(auto col: cg)
			col_order.push_back(col);
	col_order_inv.resize(col_order.size());
	for(uint pos=0; pos<col_order.size(); pos++)
		col_order_inv[col_order[pos]] = pos;
}


uint Index::compute_levels(uint nodeid) {
	uint lvl=0;
	for(auto childid: children_ids[nodeid]) 
		lvl = max(lvl, compute_levels(childid));
	levels[nodeid] = lvl+1;
	return levels[nodeid];
}

string Index::show() const {
	string result= "Index[start_index, name: "+name+ ", Tree:\n"+ ExpressionTree::show()+"\n";
	result += "maintenance cost: "+to_string(maint_cost)+"\n";
	for(uint i=0; i< cgs.size(); i++) {
		result += "Column group "+to_string(i)+" with size "+ to_string(size_cgs[i]) +": ";
		for(auto& col: cgs[i]) 
			result += "("+ nodes[col].name+","+to_string(card_col.at(col)) + ")";
		result += "\n";
	}
	result += " end_index]\n";
	return result;
}


vector<vector<int>> Index::lookup(const map<uint, int>& lookup_values, 
	uint cg, uint colid, const IndexNavigator* nav) const {

	assert(cg<cgs.size());
	assert(colid<cgs[cg].size());
	vector<vector<int>> result;
	vector<int> match_values;
	
	if(lookup_values.find(cgs[cg][colid])!=lookup_values.end()) 
		match_values.push_back(lookup_values.at(cgs[cg][colid]));
	else
		for(auto& dt: nav->scan())
			match_values.push_back(dt.get_int_val());
	for(auto val: match_values) {
		if(cg==cgs.size()-1 && colid==cgs[cg].size()-1) 
			result.push_back(vector<int>{val});
		else {
			auto nextnavpr = nav->look_up(Data(val));
			if(nextnavpr.first) {
				uint newcolid = colid+1; uint newcg=cg;
				if(newcolid==cgs[cg].size()) {
					newcg+=1; newcolid=0;
				}
				for(auto& nextresult: lookup(lookup_values, newcg, newcolid, &nextnavpr.second)) {
					result.push_back(vector<int>{val});
					result.back().insert(result.back().end(), nextresult.begin(), nextresult.end());
				}
			}
		}
	}
	return result;
}

vector<vector<int>> Index::lookup(const map<uint, int>& lookup_values) const {
	return lookup(lookup_values, 0, 0, first_col_nav);
}

void Index::set_index_table(const IndexTable* tab, const IndexNavigator* nav) {
	assert(tab!=NULL);
	for(auto dtype: tab->get_dtypes())
		assert(dtype == Dtype::Int);
	assert(nodes.size() == tab->get_dtypes().size());
	table = tab;
	first_col_nav = nav;
}