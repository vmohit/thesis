
#include "mcd.h"
#include "index_table.h"
#include "query_template.h"

#include <string>
#include <vector>
#include <map>
#include <set>
#include <cstdlib>
#include <cassert>
#include <iostream>
#include <algorithm>

using std::string;
using std::map;
using std::pair;
using std::make_pair;
using std::to_string;
using std::greater;
using std::cout;
using std::min;
using std::endl;

float MCD::disk_seek_cost=10;

MCD::MCD(const Index* ind, const QueryTemplate* qt, 
	const map<uint, uint>& i2q) : index(ind),
	query(qt), index2query(i2q){
	// every node of index must be mapped with a distinct node of query
	//cout<<query->show();
	for(auto it=index2query.begin(); it!=index2query.end(); it++) {
		assert(query2index.find(it->second)==query2index.end());
		assert(index->nodes[it->first].ent == query->nodes[it->second].ent);
		query2index[it->second] = it->first;
		node2root_distances.push_back(query->node2root_distance[it->second]);
	}
	sort(node2root_distances.begin(), node2root_distances.end(), greater<uint>()); 
	// every edge present in index, must also be present in query with the same underlying base relation
	for(uint nid=0; nid < query->nodes.size(); nid++)
		for(uint cid: query->children_ids[nid])
			if((query2index.find(cid)!=query2index.end()) && 
				(query2index.find(nid)!=query2index.end())) {
				assert(index->parent_ids[query2index[cid]] == (int) query2index[nid]);
				assert(index->parent_rels[query2index[cid]]==query->parent_rels[cid]);
				covered_goals.insert(query->edge2goalid.at(make_pair(cid, nid)));
			}
	timeue = 0;
	float num_lookups=1;
	//cout<<index->show();
	for(uint i=0; i<index->cgs.size(); i++) {
		if(i>0)
			timeue += num_lookups*(disk_seek_cost+index->size_cgs[i]);
		for(auto col: index->cgs[i]) 
			num_lookups *= (index->card_col.at(col) * query->instance_cardinalities[index2query[col]]) / index->nodes[col].ent->cardinality;
	}
}

string MCD::get_signature() const {
	string signature = query->name;
	for(auto it=covered_goals.begin(); it!=covered_goals.end(); it++) 
		signature += " "+to_string(*it);
	return signature;
}

string MCD::show() const {
	string result = "MCD [start_mcd, query: "+query->name+", mapping: {";
	for(auto it=index2query.begin(); it!=index2query.end(); it++) {
		result += "("+index->nodes[it->first].name+","+query->nodes[it->second].name+"), ";
	}
	result += "}, timeue: "+to_string(timeue)+", timeoe: "+to_string(timeoe)+", covered_goals: ";
	for(auto gid: covered_goals) 
		result += "("+query->nodes[query->goalid2edge.at(gid).first].name+", "
				+ query->nodes[query->goalid2edge.at(gid).second].name+"), ";
	result += "end_mcd]\n";
	return result;
}


bool comp_mcd(const MCD* mcd1, const MCD* mcd2) {
    uint n=min(mcd1->node2root_distances.size(), mcd2->node2root_distances.size());
    for(uint i=0; i<n; i++)
    	if(mcd1->node2root_distances[i]!=mcd2->node2root_distances[i])
    		return mcd1->node2root_distances[i] > mcd2->node2root_distances[i];
    return mcd1->node2root_distances.size() > mcd2->node2root_distances.size();
}