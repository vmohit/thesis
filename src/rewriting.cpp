#include "rewriting.h"
#include "mcd.h"
#include "expression_tree.h"
#include "utils.h"

#include <string>
#include <vector>
#include <map>
#include <set>
#include <cstdlib>
#include <cassert>
#include <iostream>
#include <list>
#include <queue>
#include <algorithm>
#include <tsl/htrie_map.h>

using std::string;
using std::map;
using std::pair;
using std::make_pair;
using std::set;
using std::list;
using std::cout;
using std::queue;
using std::endl;
using std::min;
using std::vector;
using std::to_string;
using esutils::set_intersection;

Rewriting::Rewriting() : query(NULL) {}

Rewriting::Rewriting(const QueryTemplate* qry, const map<uint, const MCD*>& goal2brmcds) :
	query(qry), querygoal2brmcd(goal2brmcds){
	assert(query!=NULL);
}

void Rewriting::add_mcd(const MCD* mcd) {
	assert(mcd!=NULL);
	assert(query!=NULL);
	assert(query==mcd->query);
	assert(mcds.find(mcd)==mcds.end());

	vector<int> parents(query->nodes.size(), -1);
	vector<vector<uint>> children(query->nodes.size());
	for(auto gid: covered_goals) {
		auto edge = query->goalid2edge.at(gid);
		parents[edge.first] = edge.second;
		children[edge.second].push_back(edge.first);
	}
	list<ExpressionTree> trees;
	map<uint, pair<const ExpressionTree*, uint>> query2tree;
	TreeBuilder tbuilder(vector<float>{}, map<const BaseRelation*, float>{});
	for(uint nid=0; nid<query->nodes.size(); nid++) {
		if(parents[nid]==-1 && children[nid].size()!=0) {
			trees.push_back(ExpressionTree(query->nodes[nid].ent, query->nodes[nid].name, query->nodes[nid].value, query->nodes[nid].isconstant));
			query2tree[nid] = make_pair(&trees.back(), 0);
			queue<uint> Q;
			Q.push(nid);
			while(!Q.empty()) {
				uint pid=Q.front(); Q.pop();
				for(uint cid: children[pid]) {
					tbuilder.add_leaf(trees.back(), query2tree[pid].second, query->parent_rels[cid], query->nodes[cid].name, query->nodes[cid].value, query->nodes[cid].isconstant);
					query2tree[cid] = make_pair(&trees.back(), trees.back().nodes.size()-1);
					Q.push(cid);
				}
			}
		}
	}
	
	float num_lookups=1;
	//cout<<mcd->index->cgs.size()<<" "<<mcd->index->size_cgs.size();
	for(uint i=0; i<mcd->index->cgs.size(); i++) {
		if(i>0) {
			time_cost += num_lookups*(mcd->disk_seek_cost+mcd->index->size_cgs[i]);
		}
		for(auto& col: mcd->index->cgs[i]) {
			auto q_nid = mcd->index2query.at(col);
			if(query2tree.find(q_nid)!=query2tree.end()) {
				auto pr = query2tree[q_nid];
				num_lookups *= pr.first->instance_cardinalities[pr.second];
			}
			else
				num_lookups *= (mcd->query->children_ids[mcd->index2query.at(col)].size()==0 ? 1 
					: mcd->index->card_col.at(col));
		}
	}

	mcds.insert(mcd);
	for(auto it=mcd->query2index.begin(); it!=mcd->query2index.end(); it++)
		querynode2mcds[it->first].insert(mcd);
	for(uint gid: mcd->covered_goals) {
		covered_goals.insert(gid);
		querygoal2brmcd.erase(gid);
	}

}

std::string Rewriting::show() const {
	assert(query!=NULL);
	string result = "Rewriting of query: "+query->name+", MCDs: {";
	for(auto mcd: mcds)
		result += mcd->show()+", \n";
	result += "}, covered_goals: {";
	for(auto gid: covered_goals) {
		auto edge = query->goalid2edge.at(gid);
		result += "("+query->nodes[edge.first].name+", "+query->nodes[edge.second].name+"), ";
	}
	result += "}\n";
	return result;
}

// float Rewriting::timeoe(const MCD* mcd) const {
// 	assert(query!=NULL);
// 	assert(mcd->query==query);

// 	vector<const MCD*> mcd_ord{mcd};
// 	for(auto tmpmcd: mcds)
// 		if(tmpmcd!=mcd) 
// 			mcd_ord.push_back(tmpmcd);
// 	for(auto it=querygoal2brmcd.begin(); it!=querygoal2brmcd.end(); it++) {
// 		if(mcds.find(it->second)==mcds.end() && mcd->covered_goals.find(*(it->second->covered_goals.begin()))==mcd->covered_goals.end())
// 			mcd_ord.push_back(it->second);
// 	}
// 	sort(mcd_ord.begin(), mcd_ord.end(), comp_mcd);
// 	set<uint> prev_goals;
// 	for(auto tmpmcd: mcd_ord) {
// 		if(tmpmcd==mcd)
// 			break;
// 		else
// 			prev_goals.insert(tmpmcd->covered_goals.begin(), tmpmcd->covered_goals.end());
// 	}
// 	vector<int> parents(query->nodes.size(), -1);
// 	vector<vector<uint>> children(query->nodes.size());
// 	for(auto gid: prev_goals) {
// 		auto edge = query->goalid2edge.at(gid);
// 		parents[edge.first] = edge.second;
// 		children[edge.second].push_back(edge.first);
// 	}
// 	list<ExpressionTree> trees;
// 	map<uint, pair<const ExpressionTree*, uint>> query2tree;
// 	TreeBuilder tbuilder(vector<float>{}, map<const BaseRelation*, float>{});
// 	for(uint nid=0; nid<query->nodes.size(); nid++) {
// 		if(parents[nid]==-1 && children[nid].size()!=0) {
// 			trees.push_back(ExpressionTree(query->nodes[nid].ent, query->nodes[nid].name, query->nodes[nid].value, query->nodes[nid].isconstant));
// 			query2tree[nid] = make_pair(&trees.back(), 0);
// 			queue<uint> Q;
// 			Q.push(nid);
// 			while(!Q.empty()) {
// 				uint pid=Q.front(); Q.pop();
// 				for(uint cid: children[pid]) {
// 					tbuilder.add_leaf(trees.back(), query2tree[pid].second, query->parent_rels[cid], query->nodes[cid].name, query->nodes[cid].value, query->nodes[cid].isconstant);
// 					query2tree[cid] = make_pair(&trees.back(), trees.back().nodes.size()-1);
// 					Q.push(cid);
// 				}
// 			}
// 		}
// 	}

// 	float result=0;
// 	float num_lookups=1;
// 	//cout<<mcd->index->cgs.size()<<" "<<mcd->index->size_cgs.size();
// 	for(uint i=0; i<mcd->index->cgs.size(); i++) {
// 		if(i>0) {
// 			result += num_lookups*(mcd->disk_seek_cost+mcd->index->size_cgs[i]);
// 		}
// 		for(auto& col: mcd->index->cgs[i]) {
// 			auto q_nid = mcd->index2query.at(col);
// 			if(query2tree.find(q_nid)!=query2tree.end()) {
// 				auto pr = query2tree[q_nid];
// 				num_lookups *= pr.first->instance_cardinalities[pr.second];
// 			}
// 			else
// 				num_lookups *= (mcd->query->children_ids[mcd->index2query.at(col)].size()==0 ? 1 
// 					: mcd->index->card_col.at(col));
// 		}
// 	}

// 	return result;
// }


bool Rewriting::iscomplete() const {
	assert(query!=NULL);
	return query->goalid2edge.size()==covered_goals.size();
}
