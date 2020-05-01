#include "query_processor.h"
#include "rewriting.h"
#include "query_template.h"
#include "mcd.h"
#include "disk_manager.h"
#include "data.h"
#include "utils.h"

#include <vector>
#include <map>
#include <set>
#include <cassert>
#include <cstdlib>
#include <string>
#include <iostream>
#include <algorithm>
#include <queue>
#include <boost/filesystem.hpp>

using std::vector;
using std::map;
using std::to_string;
using std::set;
using std::cout;
using std::endl;
using std::queue;
using std::make_pair;
using std::pair;
using std::string;
using boost::filesystem::path;
using esutils::set_intersection;


Dataframe::Dataframe(const QueryTemplate* qry, vector<uint> columns) 
	: query(qry), cols(columns) {
	assert(columns.size()!=0);
}


vector<vector<int>> Dataframe::reorder_cols() const {
	assert(query->nodes.size() == cols.size());
	vector<int> pos(cols.size(), -1);
	for(uint i=0; i<cols.size(); i++) {
		assert(cols[i]<pos.size());
		assert(pos[cols[i]]==-1);
		pos[cols[i]] = i;
	}
	vector<vector<int>> result;
	vector<int> reordered_record(cols.size(), 0);
	for(auto& record: values) {
		for(uint qid=0; qid<cols.size(); qid++)
			reordered_record[qid] = record[pos[qid]];
		result.push_back(reordered_record);
	}
	return result;
}

string Dataframe::show() const {
	string result ="Dataframe header: ";
	for(auto col: cols)
		result += query->nodes[col].name+", ";
	result += "\n";
	for(auto& record: values) {
		result += "[";
		for(auto cell: record)
			result += to_string(cell)+", ";
		result += "]\n";
	}
	return result;
}

vector<vector<int>> execute_instance(const Rewriting* R, map<uint, int> leaf_values) {
	auto query = R->query;
	// Initialize dataframes
	vector<Dataframe> dfs;
	for(uint nid=0; nid < query->nodes.size(); nid++) 
		if(query->children_ids[nid].size()==0) {
			auto df = Dataframe(query, vector<uint>{nid});
			assert(leaf_values.find(nid)!=leaf_values.end());
			if(query->nodes[nid].isconstant)
				assert(query->nodes[nid].value == leaf_values[nid]);
			df.values.push_back(vector<int>{leaf_values[nid]});
			dfs.push_back(df);
		}
	// Process mcds in increasing order of their node2root distance vectors
	vector<const MCD*> mcds(R->mcds.begin(), R->mcds.end());
	sort(mcds.begin(), mcds.end(), comp_mcd);
	for(auto mcd: mcds) {
		// Identify dataframes to merge
		vector<Dataframe> newdfs;
		vector<Dataframe> proc_dfs;
		for(auto& df: dfs) {
			bool added = false;
			for(auto col: df.cols) {
				if(mcd->query2index.find(col)!=mcd->query2index.end()) {
					proc_dfs.push_back(df); 
					added = true; break;
				}
			}
			if(!added)
				newdfs.push_back(df);
		}
		assert(proc_dfs.size()!=0);
		vector<uint> merged_cols;
		for(auto& df: proc_dfs) {
			for(auto col: df.cols)
				if(mcd->query2index.find(col)==mcd->query2index.end()) 
					merged_cols.push_back(col);
		}
		for(auto cg: mcd->index->cgs)
			for(auto col: cg)
				merged_cols.push_back(mcd->index2query.at(col));
		Dataframe merged_df(query, merged_cols);

		// cout<<"Processing: "<<mcd->show();
		// cout<<"Merging following dataframes:\n";
		// for(auto& df: proc_dfs)
		// 	cout<<df.show()<<endl;
		
		//Perform cross join on dataframes to be merged with memoization for index lookups
		map<vector<int>, vector<vector<int>>> cached_lookups;
		vector<uint> S(proc_dfs.size(), 0);
		while(S.size()!=0) {
			// perform look-up on the mcd
			map<uint, int> lookup_values;
			vector<int> lookup_signature;
			vector<int> prefix_record;
			map<uint, int> known_values;
			for(uint dfid=0; dfid<S.size(); dfid++)
				for(uint i=0; i<proc_dfs[dfid].cols.size(); i++) {
					known_values[proc_dfs[dfid].cols[i]] = proc_dfs[dfid].values[S[dfid]][i];
					if(mcd->query2index.find(proc_dfs[dfid].cols[i])!=mcd->query2index.end()) {
						if(mcd->index->firstcg.find(mcd->query2index.at(proc_dfs[dfid].cols[i]))!=mcd->index->firstcg.end()) {
							lookup_values[mcd->query2index.at(proc_dfs[dfid].cols[i])] = proc_dfs[dfid].values[S[dfid]][i];
							lookup_signature.push_back(proc_dfs[dfid].values[S[dfid]][i]);
						}
					}
					else
						prefix_record.push_back(proc_dfs[dfid].values[S[dfid]][i]);
				}
			// cout<<"First CG: ";
			// for(auto col: mcd->index->firstcg)
			// 	cout<<query->nodes[mcd->index2query.at(col)].name<<" ";
			// cout<<endl;
			if(cached_lookups.find(lookup_signature)==cached_lookups.end())	{
				// cout<<"Performing the following look-up: ";
				// for(auto it=lookup_values.begin(); it!=lookup_values.end(); it++)
				// 	cout<<query->nodes[mcd->index2query.at(it->first)].name<<"= "<<it->second<<", ";
				// cout<<endl;
				cached_lookups[lookup_signature] = mcd->index->lookup(lookup_values);
			}
			else {
				// cout<<"The following look-up was already cached: ";
				// for(auto it=lookup_values.begin(); it!=lookup_values.end(); it++)
				// 	cout<<query->nodes[mcd->index2query.at(it->first)].name<<"= "<<it->second<<", ";
				// cout<<endl;
			}
			// collect results into merged df
			for(auto& record: cached_lookups[lookup_signature]) {
				auto merged_record = prefix_record;
				bool merge_flag = true;
				for(uint inid=0; inid<record.size(); inid++) {
					if(known_values.find(mcd->index2query.at(mcd->index->col_order[inid]))!=known_values.end())
						if(known_values.at(mcd->index2query.at(mcd->index->col_order[inid]))!=record[inid]) {
							merge_flag = false;
						}
				}
				if(merge_flag) {
					merged_record.insert(merged_record.end(), record.begin(), record.end());
					merged_df.values.push_back(merged_record);
				}
			}
			while(S.back()==proc_dfs[S.size()-1].values.size()-1) {
				S.pop_back();
				if(S.size()==0)
					break;
			}
			if(S.size()==0)
				break;
			S.back()+=1;
			while(S.size()<proc_dfs.size())
				S.push_back(0);
		}
		// cout<<"Merged dataframe:\n";
		// cout<<merged_df.show()<<endl;
		if(merged_df.values.size()==0)
			return vector<vector<int>>();
		dfs = newdfs;
		dfs.push_back(merged_df);
	}
	assert(dfs.size()==1);
	return dfs[0].reorder_cols();
}


void add_tuples(IndexTable& table, const Index* index, map<uint, set<int>>& doms, 
	const vector<uint>& node_order, vector<int>& values) {
	// for(auto val: values)
	// 	cout<<val<<" ";
	// cout<<endl;
	// cout<<"doms: \n";
	// for(auto it=doms.begin(); it!=doms.end(); it++) {
	// 	cout<<index->nodes[it->first].name<<": ";
	// 	for(auto val: it->second)
	// 		cout<<val<<", ";
	// 	cout<<endl;
	// }
	if(values.size()==index->nodes.size()) {
		vector<Data> tupvals(values.size(), Data(0));
		for(uint i=0; i<values.size(); i++)
			tupvals[index->col_order_inv[node_order[i]]] = Data(values[i]);
		table.add_tuple(Tuple(tupvals, 1));
		//cout<<"add tu1ple called on tuple"<<Tuple(tupvals, 1).show()<<"\n";
	}
	else {
		uint nid = node_order[values.size()];
		if(index->root_id == nid) {
			assert(doms.find(nid)!=doms.end());
			for(auto val: doms.at(nid)) {
				values.push_back(val);
				add_tuples(table, index, doms, node_order, values);
				values.pop_back();
			}
		}
		else {
			IndexNavigator nav(index->parent_rels[nid]->tab);
			bool check = (doms.find(nid)!=doms.end());
			set<int> empty_set;
			auto domain = &(check? doms.at(nid) : empty_set);

			bool pardom_absent = doms.find((uint) index->parent_ids[nid])==doms.end();
			auto pardom = pardom_absent ? empty_set : doms.at((uint) index->parent_ids[nid]);
			for(auto& dt: nav.scan()) {
				if(check) {
					if(domain->find(dt.get_int_val())==domain->end())
						continue;
				}
				auto nextnav = nav.look_up(dt).second;
				set<int> newpardom;
				for(auto& nextdt: nextnav.scan())
					newpardom.insert(nextdt.get_int_val());
				if(pardom_absent)
					doms[(uint) index->parent_ids[nid]] = newpardom;
				else
					doms[(uint) index->parent_ids[nid]] = set_intersection(newpardom, pardom);
				// cout<<"Parent: "<<index->nodes[index->parent_ids[nid]].name<<endl;
				// cout<<"newpardom: \n";
				// for(auto it=newpardom.begin(); it!=newpardom.end(); it++) {
				// 	cout<<*it<<", ";
				// }
				// cout<<endl<<"---------1\n";
				// cout<<"pardom: \n";
				// for(auto it=pardom.begin(); it!=pardom.end(); it++) {
				// 	cout<<*it<<", ";
				// }
				// cout<<endl<<"---------2\n";
				values.push_back(dt.get_int_val());
				add_tuples(table, index, doms, node_order, values);
				values.pop_back();
			}
			if(pardom_absent)
				doms.erase((uint) index->parent_ids[nid]);
			else
				doms[(uint) index->parent_ids[nid]] = pardom;
		}
	}
}

IndexTable create_index_table(const Index* index, string name, path db_path, const DiskManager& dm) {
	vector<vector<string>> col_names;
	vector<Dtype> dtypes;
	for(auto cg: index->cgs) {
		col_names.push_back(vector<string>());
		for(auto col: cg) {
			dtypes.push_back(Dtype::Int);
			col_names.back().push_back(index->nodes[col].name);
		}
	}
	IndexTable table(name, dtypes, col_names, 
		db_path / "indexes", dm);

	vector<pair<uint,uint>> node_levels;
	for(uint nid=0; nid<index->nodes.size(); nid++)
		node_levels.push_back(make_pair(index->levels[nid], nid));
	sort(node_levels.begin(), node_levels.end());
	vector<uint> node_order;
	auto doms = map<uint, set<int>>{};
	for(auto pr: node_levels) {
		node_order.push_back(pr.second);
		//cout<<pr.second<<" ";
		if(index->nodes[pr.second].isconstant)
			doms[pr.second] = set<int>{index->nodes[pr.second].value};
	}
	//cout<<endl;
	vector<int> values;
	add_tuples(table, index, doms, node_order, values);
	table.freeze();
	return table;
}