#include "application.h"
#include "base_relation.h"
#include "query_template.h"
#include "index.h"
#include "expression_tree.h"
#include "rewriting.h"
#include "utils.h"
#include "query_processor.h"

#include <vector>
#include <string>
#include <set>
#include <list>
#include <map>
#include <cstdlib>
#include <limits>
#include <iostream>
#include <cmath>
#include <queue>
#include <algorithm>

using std::vector;
using std::string;
using std::set;
using std::pair;
using std::map;
using std::make_pair;
using std::to_string;
using std::cout;
using std::endl;
using std::queue;
using std::list;
using std::priority_queue;
using std::min;
using esutils::set_difference;
using esutils::set_intersection_size;
using esutils::set_difference_inplace;

float Application::wt_maint=1;
uint Application::it_deep_k=0;
uint Application::pick_fn=0;
int Application::num_expand=-1;
int Application::max_iters=-1;

Application::Design::Design(std::set<const Index*> inds, std::map<const QueryTemplate*, Rewriting> rwts)
	: stored_indexes(inds), rewritings(rwts) {
	num_rem_goals = 0;
	lb_cost = 0;
	for(auto& ind: stored_indexes) {
		lb_cost += ind->maint_cost * Application::wt_maint;
	}
	for(auto it=rewritings.begin(); it!=rewritings.end(); it++) {
		num_rem_goals += it->first->goals.size()-set_intersection_size(it->first->goals, it->second.covered_goals);
		for(auto gid: it->first->goals) 
			if(it->second.covered_goals.find(gid)==it->second.covered_goals.end())
				rem_goals.insert(make_pair(it->first, gid));
		lb_cost += it->second.time_cost * it->first->weight;
	}
	ub_cost = lb_cost;
	cost = lb_cost;
	if(inds.size()>0) { // to use this constructor, design must either be empty or complete
		for(auto it=rewritings.begin(); it!=rewritings.end(); it++)
			assert(it->second.iscomplete());
	}
	last_updated = Application::it_deep_k;
} 

string Application::Design::show(const map<const MCD*, uint>& mcd_to_order) const {
	string result = (this->iscomplete()? "[COMPLETE]": "[INCOMPLETE]");
	result += " lb_cost: "+to_string(lb_cost)+", num_rem_goals: "+to_string(num_rem_goals)+" ";
	result += "mcd sequence : { ";
	for(auto it=rewritings.begin(); it!=rewritings.end(); it++) 
		for(auto mcd: it->second.mcds)
			result += to_string(mcd_to_order.at(mcd))+", ";
	result += "}\n";
	return result;
}

bool Application::Design::operator<(const Application::Design& D) const {
	return num_rem_goals>D.num_rem_goals;
}

void Application::Design::add_mcd(const MCD* mcd, const map<pair<const QueryTemplate*, uint>, uint>& goal2lastpos,
	const map<const MCD*, uint>& mcd_to_order) {
	assert(mcd_to_order.at(mcd)>=min_mcd_pos);
	min_mcd_pos = mcd_to_order.at(mcd);
	if(stored_indexes.find(mcd->index)==stored_indexes.end()) {
		stored_indexes.insert(mcd->index);
	}
	auto rewriting = &rewritings[mcd->query];
	assert(!rewriting->iscomplete());
	num_rem_goals -= (mcd->covered_goals.size() 
		- set_intersection_size(mcd->covered_goals, rewriting->covered_goals));
	cost -= rewriting->time_cost * mcd->query->weight;
	rewriting->add_mcd(mcd);
	cost += rewriting->time_cost * mcd->query->weight;
	for(auto gid: mcd->covered_goals)
		rem_goals.erase(make_pair(mcd->query, gid));

	for(auto goal: rem_goals)
		assert(goal2lastpos.at(goal)>min_mcd_pos);
}

float Application::Design::get_priority(uint id) const {
	assert(id<3);
	if(id==0)
		return -1*lb_cost;
	if(id==1)
		return -1*ub_cost;
	if(id==2)
		return -1*num_rem_goals;
	return 0;
}

bool Application::Design::iscomplete() const {
	return num_rem_goals==0;
}

Application::Application(vector<const Entity*> ents, vector<const BaseRelation*> rels,
		vector<const QueryTemplate*> query_workload, const DiskManager& disk_manager,
		boost::filesystem::path pth) : dm(disk_manager), db_path(pth), entities(ents),
		base_rels(rels), workload(query_workload), 
		D(set<const Index*>{}, map<const QueryTemplate*, Rewriting>{}) {

	initialize_candidates();
	map<const QueryTemplate*, Rewriting> rewritings;
	float num_nodes=0;
	for(uint i=0; i<workload.size(); i++) {
		num_nodes += workload[i]->nodes.size();
		rewritings[workload[i]] = Rewriting(workload[i], querygoal2br[i]);
	}
	approx_factor = 2*log(num_nodes)*log(num_nodes);
	D = Design(set<const Index*>{}, rewritings);
}

string Application::show_candidates() const {
	string result;
	result += "Number of indexes: "+to_string(I.size())+", Number of MCDs: "+to_string(mcds.size())+"\n";
	if(mcds.size()<=10) {
		uint count=0;
		for(auto it=M.begin(); it!=M.end(); it++) {
			result += "Index#"+to_string(count++)+"\n";
			result += it->first->show();
			result += "MCDs: {\n";
			for(auto mcd: it->second) 
				result += mcd->show()+", ";
			result += "}\n\n";
		}
		result += "goal2lastpos: ";
		for(auto it=goal2lastpos.begin(); it!=goal2lastpos.end(); it++)
			result += "("+it->first.first->name+", "+to_string(it->first.second)
					+"): "+to_string(it->second)+"; ";
		result += "\nMCD order: \n";
		for(auto mcd: mcd_order)
			result += "["+to_string(mcd_to_order.at(mcd))+"] "+ mcd->show();
		result += "signatures: {";
		string signature;
	    for(auto it = signatures.begin(); it != signatures.end(); it++) {
	        it.key(signature);
	        result += "(" + signature + ", {";
	        for(auto it2=it.value().begin(); it2!=it.value().end(); it2++)
	        	result += to_string(*it2)+",";
	        result += "}), ";
	    }
	    result += "\n";
	}
	return result;
}

void Application::initialize_candidates() {

	TreeBuilder tbuilder(vector<float>{}, map<const BaseRelation*, float>{});
	map<pair<const BaseRelation*, pair<int,int>>, Index*> base_indexes;
	for(auto query: workload) {
		querygoal2br.push_back(map<uint, const MCD*>{});

		for(uint nid=0; nid<query->nodes.size(); nid++) 
			for(uint i=0; i<query->children_ids[nid].size(); i++) {
				uint cid = query->children_ids[nid][i];
				bool isconstant = query->nodes[cid].isconstant;
				vector<pair<const BaseRelation*, pair<int,int>>> base_sigs;
				base_sigs.push_back(make_pair(query->children_rels[nid][i], 
					make_pair(isconstant, isconstant? query->nodes[cid].value : 0)));
				if(isconstant)
					base_sigs.push_back(make_pair(query->children_rels[nid][i], 
						make_pair(false, 0)));

				for(auto base_index_sig : base_sigs) {
					if(base_indexes.find(base_index_sig)==base_indexes.end()) {
						I.push_back(tbuilder.make_tree(*query, cid, nid));
						base_indexes[base_index_sig] = &I.back();
						M[&I.back()] = set<const MCD*>{};
					}
					auto index = base_indexes[base_index_sig];
					auto mcd = MCD(index, query, map<uint,uint>{{index->root_id, nid}, 
						{(uint) (1-index->root_id), cid}});
					mcds.push_back(mcd);
					M[index].insert(&mcds.back());

					if(base_index_sig.second.first == false)
						querygoal2br.back()[query->edge2goalid.at(make_pair(cid, nid))]=&mcds.back();

					auto signature = mcd.get_signature();
					if(signatures.find(signature)==signatures.end())
						signatures[signature] = set<uint>{};
					signatures[signature].insert((uint) index->maint_cost);
				}
			}
	}
}

void Application::generate_candidates(uint max_depth, float max_maint_cost) {

	TreeBuilder tbuilder(vector<float>{}, map<const BaseRelation*, float>{});
	queue<Index*> indexes;
	for(auto it=I.begin(); it!=I.end(); it++)
		indexes.push(&(*it));
	while(!indexes.empty()) {
		auto index = indexes.front();
		for(auto mcd: M[index]) {
			auto query = mcd->query;
			vector<uint> expansions;
			if(query->parent_ids[mcd->index2query.at(index->root_id)]!=-1)
				expansions.push_back(mcd->index2query.at(index->root_id));
			for(auto it=mcd->query2index.begin(); it!=mcd->query2index.end(); it++)
				for(auto cid: query->children_ids[it->first])
					if(mcd->query2index.find(cid)==mcd->query2index.end())
						expansions.push_back(cid);
			for(auto cid: expansions) {
				auto newIndexTree = (ExpressionTree) (*index);
				if(cid==mcd->index2query.at(index->root_id)) 
					tbuilder.add_root(newIndexTree, query->parent_rels[cid], 
						query->name+"::"+query->nodes[(uint) query->parent_ids[cid]].name);
				else {
					tbuilder.add_leaf(newIndexTree, mcd->query2index.at((uint) query->parent_ids[cid]), 
						query->parent_rels[cid], query->name+"::"+query->nodes[cid].name, 
						query->nodes[cid].value, query->nodes[cid].isconstant);
				}
				I.push_back(Index(newIndexTree));
				if(I.back().levels[I.back().root_id]>max_depth || I.back().maint_cost>max_maint_cost) {
					I.pop_back(); continue;
				}
				auto newIndex = &I.back();
				vector<MCD> new_mcds;
				set<string> curr_signatures;
				bool abort_search = false;
				for(auto old_mcd: M[index]) {
					if(cid==mcd->index2query.at(index->root_id)) {
						auto new_index2query = old_mcd->index2query;
						if(old_mcd->query->parent_ids[new_index2query[index->root_id]]!=-1)
							if(old_mcd->query->parent_rels[new_index2query[index->root_id]]==newIndex->parent_rels[index->root_id])
								new_index2query[newIndex->root_id] = (uint) old_mcd->query->parent_ids[new_index2query[index->root_id]];
							else
								continue;
						else
							continue;
						MCD new_mcd(newIndex, old_mcd->query, new_index2query);
						auto signature = new_mcd.get_signature();
						if(signatures.find(signature)!=signatures.end())
							if(signatures[signature].find((uint) newIndex->maint_cost)!=signatures[signature].end()) {
								abort_search=true; break;
							}
						if(curr_signatures.find(signature)!=curr_signatures.end())
							continue;
						new_mcds.push_back(new_mcd);
						curr_signatures.insert(signature);
					}
					else {
						uint old_q_pid = old_mcd->index2query.at(mcd->query2index.at((uint) query->parent_ids[cid]));
						for(uint old_q_cid : old_mcd->query->children_ids[old_q_pid]) {
							if(old_mcd->query2index.find(old_q_cid)==old_mcd->query2index.end())
								if(old_mcd->query->parent_rels[old_q_cid]==query->parent_rels[cid]) {
									if(query->nodes[cid].isconstant && 
										(!old_mcd->query->nodes[old_q_cid].isconstant || query->nodes[cid].value!=old_mcd->query->nodes[old_q_cid].value))
										continue;
									auto new_index2query = old_mcd->index2query;
									new_index2query[newIndex->nodes.size()-1] = old_q_cid;
									MCD new_mcd(newIndex, old_mcd->query, new_index2query);
									auto signature = new_mcd.get_signature();
									if(signatures.find(signature)!=signatures.end())
										if(signatures[signature].find((uint) newIndex->maint_cost)!=signatures[signature].end()) {
											abort_search=true; break;
										}
									if(curr_signatures.find(signature)!=curr_signatures.end())
										continue;
									new_mcds.push_back(new_mcd);
									curr_signatures.insert(signature);
								}
						}
					}
				}
				if(abort_search)
					I.pop_back();
				else {
					set<const MCD*> MI;
					for(auto& new_mcd: new_mcds) {
						auto signature = new_mcd.get_signature();
						if(signatures.find(signature)==signatures.end())
							signatures[signature] = set<uint>{};
						signatures[signature].insert((uint) newIndex->maint_cost);
						mcds.push_back(new_mcd);
						MI.insert(&mcds.back());
					}
					M[newIndex] = MI;
					indexes.push(newIndex);
				}
			}
		}
		indexes.pop();
	}

	for(auto it=M.begin(); it!=M.end(); it++) 
		for(auto mcd: it->second) 
			mcd_order.push_back(mcd);
	sort(mcd_order.begin(), mcd_order.end(), comp_mcd);
	for(uint i=0; i<mcd_order.size(); i++)
		mcd_to_order[mcd_order[i]] = i;
	for(int i=mcd_order.size()-1; i>=0; i--) 
		for(uint gid: mcd_order[i]->covered_goals)
			if(goal2lastpos.find(make_pair(mcd_order[i]->query, gid))==goal2lastpos.end()) 
				goal2lastpos[make_pair(mcd_order[i]->query, gid)] = i+1;
	// computing oe costs for every mcd
	for(auto it=mcds.begin(); it!=mcds.end(); it++) {
		auto mcd = &(*it);
		uint pos=0;
		while(pos<workload.size()) {
			if(mcd->query==workload[pos])
				break;
			pos++;
		}
		Rewriting R(mcd->query, querygoal2br[pos]);
		for(uint i=0; i<mcd_to_order[mcd]; i++) {
			if(mcd_order[i]->query == mcd->query && mcd_order[i]->covered_goals.size()==1)
				R.add_mcd(mcd_order[i]);
		}
		float cost_before = R.time_cost;
		R.add_mcd(mcd);
		mcd->timeoe = R.time_cost - cost_before;
	}
}


vector<Application::Design> Application::expand(const Application::Design* design) const {
	//cout<<"Input design: "<<design->show(mcd_to_order)<<endl;
	vector<Application::Design> neighbors;
	uint max_mcd_pos=mcd_order.size();
	for(auto goal: design->rem_goals)
		max_mcd_pos = min(max_mcd_pos, goal2lastpos.at(goal));
	priority_queue<pair<float, uint>> best_designs;
	for(uint pos=design->min_mcd_pos; pos<max_mcd_pos; pos++) {
		auto mcd = mcd_order[pos];
		auto rewriting = &(design->rewritings.at(mcd->query));
		if(!rewriting->iscomplete()) {
			int extra_cov_goals = mcd->covered_goals.size()-set_intersection_size(mcd->covered_goals, rewriting->covered_goals);
			if(rewriting->mcds.find(mcd)==rewriting->mcds.end() && extra_cov_goals!=0) {
				neighbors.push_back(*design);
				neighbors.back().add_mcd(mcd, goal2lastpos, mcd_to_order);
				if(neighbors.back().last_updated > it_deep_k) {
					neighbors.back().lb_cost = optimize_wsc(neighbors.back(), false).cost / approx_factor;
					neighbors.back().ub_cost = optimize_wsc(neighbors.back(), true).cost;
					neighbors.back().last_updated=0;
				}
				neighbors.back().last_updated++;
				best_designs.push(make_pair((-1*neighbors.back().cost/extra_cov_goals), neighbors.size()-1));
				//cout<<"Expanded design: "<<neighbors.back().show(mcd_to_order)<<endl;
			}
		}
	}

	if(num_expand>0) {
		vector<Application::Design> filtered_neighbors;
		while(!best_designs.empty() && ((int) filtered_neighbors.size())<num_expand) {
			filtered_neighbors.push_back(neighbors[best_designs.top().second]);
			best_designs.pop();
		}
		neighbors = filtered_neighbors;
	}
	return neighbors;
}

vector<pair<uint,float>> Application::optimize() {
	vector<pair<uint,float>> result;
	cout<<"############ STARTING OPTIMIZE ###############\n";
	priority_queue<pair<float, Application::Design>> F;
	//cout<<"Insert: "<<D.show(mcd_to_order)<<"\n====================\n";
	D.lb_cost = optimize_wsc(D, false).lb_cost / approx_factor;
	D.ub_cost = optimize_wsc(D, true).lb_cost;
	cout<<"Initial global lower bound: "<<D.lb_cost<<endl;
	cout<<"Initial global upper bound: "<<D.ub_cost<<endl;
	F.push(make_pair(D.get_priority(pick_fn), D));
	auto bestD = D; 
	float Lub = std::numeric_limits<float>::max();
	uint num_expanded=0;
	while(!F.empty()) {
		auto topD = F.top().second;
		//cout<<"Delete: "<<topD.show(mcd_to_order)<<"\n";
		F.pop();
		if(topD.iscomplete()) {
			if(topD.cost < Lub) {
				Lub = topD.cost;
				bestD = topD;
				cout<<"num_expanded: "<<num_expanded<<", Lub: "<<Lub<<endl;
				result.push_back(make_pair(num_expanded, Lub));
			}
		}
		else {
			// cout<<"F.top() design: "<<topD.show(mcd_to_order)<<endl;
			for(auto& design: expand(&topD)) {
				num_expanded+=1;
				//cout<<"expanded design: "<<design.show(mcd_to_order)<<endl;
				if(design.lb_cost < Lub)  {
					if(design.ub_cost<Lub) {
						Lub = design.ub_cost;
						bestD = optimize_wsc(design, true);
						cout<<"num_expanded: "<<num_expanded<<", Lub: "<<Lub<<endl;
						result.push_back(make_pair(num_expanded, Lub));
					}
					//cout<<"Insert:  "<<design.show(mcd_to_order)<<"\n====================\n";
					F.push(make_pair(design.get_priority(pick_fn), design));
				}
			}
		}
		if(max_iters>0)
			if((int)num_expanded>max_iters)
				break;
	}
	cout<<"Total number of states expanded: "<<num_expanded<<endl;
	D = bestD;
	result.push_back(make_pair(num_expanded, D.cost));
	return result;
}


set<const MCD*> Application::inner_greedy(const Index* index, uint& negc,
	float& cost, const map<const QueryTemplate*, set<uint>>& rem_goals, uint num_rem_goals,
	const map<const QueryTemplate*, Rewriting>& rewritings, bool oe, uint min_mcd_pos) const {
	
	auto best_set = set<const MCD*>{};
	float best_cost = 0;  uint best_negc = 0;

	for(uint target=1; target<=num_rem_goals; target++) {
		auto cand_set = set<const MCD*>{}; float cand_cost = index->maint_cost * wt_maint; uint cand_negc=0; 
		auto cand_rem_goals = rem_goals; // goals yet to be covered by cand
		while(cand_negc<target) {
			const MCD* best_mcd=NULL;	float best_mcd_cost=-1;  uint best_mcd_negc=0;
			for(auto mcd: M.at(index))	{
				// cout<<mcd->show()<<endl;
				// cout<<mcd_to_order.at(mcd)<<endl;
				if(mcd_to_order.at(mcd)<min_mcd_pos)
					continue;
				float mcd_cost = (oe? mcd->timeoe : mcd->timeue) * mcd->query->weight;
				uint mcd_negc = min((uint) (target-cand_negc), 
					set_intersection_size(mcd->covered_goals, cand_rem_goals[mcd->query]));
				bool replace = ((best_mcd==NULL || best_mcd_negc==0) ? true: 
					mcd_negc==0 ? false: mcd_cost/mcd_negc < best_mcd_cost/best_mcd_negc);
				if(replace) {
					best_mcd = mcd; best_mcd_cost = mcd_cost; best_mcd_negc = mcd_negc;
				}
			}
			if(best_mcd==NULL || best_mcd_negc==0)
				break;
			cand_set.insert(best_mcd);
			cand_cost += best_mcd_cost;   
			best_mcd_negc = set_intersection_size(best_mcd->covered_goals, cand_rem_goals[best_mcd->query]);
			cand_negc += best_mcd_negc;   
			set_difference_inplace(cand_rem_goals[best_mcd->query], best_mcd->covered_goals);
		}
		if(cand_negc<target)
			break;
		bool replace = (cand_negc==0 ? false : best_negc==0 ? true : cand_cost/cand_negc < best_cost/best_negc);
		if(replace) {
			best_set = cand_set; best_cost = cand_cost; best_negc = cand_negc;
		}
	}
	cost = best_cost; negc = best_negc; 
	return best_set;
}

Application::Design Application::optimize_wsc(const Design& Dinit, bool oe) const {
	auto indexes = Dinit.stored_indexes;
	auto rewritings = Dinit.rewritings;
	//cout<<"Got input design: "<<Dinit.show(mcd_to_order)<<endl;
	map<const QueryTemplate*, set<uint>> rem_goals;
	uint num_rem_goals=0;
	for(auto query: workload) {
		rem_goals[query] = set_difference(query->goals, rewritings[query].covered_goals);
		num_rem_goals += rem_goals[query].size();
	}

	while(num_rem_goals>0) {
		pair<const Index*, set<const MCD*>> best_set;
		float best_cost=0; uint best_negc=0;   // cost and number of extra goals covered by best_set
		for(auto it=M.begin(); it!=M.end(); it++) {
			auto index = it->first;
			pair<const Index*, set<const MCD*>> cand_set;
			float cand_cost=0; uint cand_negc = 0;
			if(indexes.find(index)==indexes.end()) {
				cand_set = make_pair(index, inner_greedy(index, cand_negc, cand_cost, 
					rem_goals, num_rem_goals, rewritings, oe, Dinit.min_mcd_pos));
			}
			else {
				const MCD* best_mcd;  uint best_mcd_negc=0;  float best_mcd_cost=0;
				for(auto mcd: it->second) {
					if(mcd_to_order.at(mcd)<Dinit.min_mcd_pos)
						continue;
					float mcd_cost = (oe ? mcd->timeoe : mcd->timeue) * mcd->query->weight;
					float mcd_negc = set_intersection_size(mcd->covered_goals, rem_goals[mcd->query]);
					bool replace=(best_mcd_negc==0 ? true : mcd_negc==0? false : 
									mcd_cost/mcd_negc < best_mcd_cost/best_mcd_negc);
					if(replace) {
						best_mcd = mcd; best_mcd_negc=mcd_negc; best_mcd_cost=mcd_cost;
					} 
				}
				cand_set = make_pair(index, set<const MCD*>{best_mcd});
				cand_cost = best_mcd_cost;  cand_negc = best_mcd_negc;
			}
			bool replace = ((best_set.first==NULL || best_negc==0) ? true: 
				cand_negc==0 ? false : cand_cost/cand_negc < best_cost/best_negc);
			if(replace) {
				best_set = cand_set; best_cost = cand_cost;  best_negc = cand_negc;
			}
		}
		assert(best_negc>0);
		assert(best_set.first!=NULL);
		assert(best_set.second.size()!=0);
		num_rem_goals -= best_negc;
		indexes.insert(best_set.first);
		for(auto mcd: best_set.second) {
			set_difference_inplace(rem_goals[mcd->query], mcd->covered_goals);
			rewritings[mcd->query].add_mcd(mcd);
		}
	}
	//cout<<"Produced output design: "<<Design(indexes, rewritings).show(mcd_to_order)<<endl;
	return Design(indexes, rewritings);
}


string Application::show_design() const {
	string result = D.show(mcd_to_order);
	for(auto index: D.stored_indexes)
		result += index->show()+"\n";
	for(auto it=D.rewritings.begin(); it!=D.rewritings.end(); it++) {
		result += "Query: "+it->first->name+", Rewriting: "+it->second.show()+"\n";
	}
	return result;
}

void Application::create_indexes() {
	assert(D.iscomplete());
	uint count=0;
	cout<<"Creating Indexes ...\n";
	for(auto& index: I) {
		if(D.stored_indexes.find(&index)!=D.stored_indexes.end()) {
			cout<<"Working on the following index"<<endl;
			cout<<index.show()<<endl;
			tables.push_back(create_index_table(&index, "I"+to_string(count++), db_path, dm));
			navs.push_back(IndexNavigator(&tables.back()));
			index.set_index_table(&tables.back(), &navs.back());
		}
	}
}

vector<vector<int>> Application::execute_query_instance(const QueryTemplate* query, map<uint,int> leaf_vals) {
	assert(D.iscomplete());
	assert(D.rewritings.find(query)!=D.rewritings.end());
	return execute_instance(&D.rewritings.at(query), leaf_vals);
}