#include "query_template.h"
#include "utils.h"

#include <string>
#include <vector>
#include <cstdlib>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <queue>
#include <algorithm>

using std::string;
using std::vector;
using std::to_string;
using std::pair;
using std::make_pair;
using esutils::apowb;
using std::greater;
using std::cout;
using std::endl;
using std::queue;

QueryTemplate::QueryTemplate(string nm, float wt, const ExpressionTree argT) :
	ExpressionTree(argT), name(nm), weight(wt) {
	uint num_goals=0;
	for(uint nid=0; nid<nodes.size(); nid++) 
		if(parent_ids[nid]!=-1) {
			edge2goalid[make_pair(nid, (uint) parent_ids[nid])] = num_goals;
			goalid2edge[num_goals] = make_pair(nid, (uint) parent_ids[nid]);
			goals.insert(num_goals);
			num_goals++;
		}

	node2root_distance.resize(nodes.size(), 0);
	queue<uint> Q;
	Q.push(root_id);

	while(!Q.empty()) {
		for(auto cid: children_ids[Q.front()]) {
			node2root_distance[cid] = node2root_distance[Q.front()]+1;
			Q.push(cid);
		}
		Q.pop();
	}
	
	// vector<pair<uint, uint>> col_order_n2rd;
	// for(uint nid=0; nid<nodes.size(); nid++) 
	// 	col_order_n2rd.push_back(make_pair(node2root_distance[nid], nid));
	// sort(col_order_n2rd.begin(), col_order_n2rd.end(), greater<pair<uint,uint>>());
}


string QueryTemplate::show() const {
	string result = "(Query) "+name+", weight:"+to_string(weight)+"\nTree:\n";
	result += ExpressionTree::show();
	result += "subgoals: ";
	for(auto it=goalid2edge.begin(); it!=goalid2edge.end(); it++) {
		result+=to_string(it->first)+"("+nodes[it->second.first].name+","+nodes[it->second.second].name+") ";
	}
	result+="\n";
	result += "node2root distances: ";
	for(uint nid=0; nid<nodes.size(); nid++)
		result += "("+nodes[nid].name+", "+to_string(node2root_distance[nid])+"), ";
	result += "\n";
	return result;
}