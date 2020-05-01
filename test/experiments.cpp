#include "utils.h"
#include "index_table.h"
#include "disk_manager.h"
#include "tuple.h"
#include "base_relation.h"
#include "expression_tree.h"
#include "query_template.h"
#include "index.h"
#include "application.h"
#include "mcd.h"
#include "rewriting.h"
#include "query_processor.h"
#include "disk_manager.h"

#include <ctime>
#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <cmath>
#include <limits>
#include <list>
#include <nlohmann/json.hpp>
#include <cassert>
#include <algorithm>
#include <boost/filesystem.hpp>

using namespace std;
using namespace boost::filesystem;
using esutils::range;
using esutils::apowb;
using json = nlohmann::json;

DiskManager dm("temp");
string corpus_name = "toy_dataset1";
string db_name = "toy_dataset1";
path db_path(db_name);


IndexTable build_random_index(uint num_tuples, Entity le, Entity re) {
	string name =""; name += le.name.at(0); name += re.name.at(0);
	//cout<<"Building random index with name: "<<name<<endl;
	auto dtypes = vector<Dtype>{Dtype::Int, Dtype::Int};
	auto col_names = vector<vector<string>>{{""+le.name[0]}, {""+re.name[0]}};
	IndexTable tab(name, dtypes, col_names, 
		db_path / "indexes", dm);
	uint n=le.values.size();
	uint m=re.values.size();
	for(uint i=0; i<num_tuples; i++) {
		auto tup = Tuple(vector<Data>{Data(le.values[rand()%n]), 
				Data(re.values[rand()%m])}, 1);
		tab.add_tuple(tup);
		//printf("add_tuple: %s\n", tup.show().c_str());
	}
	//cout<<"Finished building random index with name: "<<name<<endl;
	tab.freeze();
	return tab;
}

IndexTable invert(const IndexTable& tab) {
	string name = tab.get_name();
	reverse(name.begin(), name.end());
	auto dtypes = vector<Dtype>{Dtype::Int, Dtype::Int};
	auto tcn = tab.get_col_names();
	auto col_names = vector<vector<string>>{{tcn[1]}, {tcn[0]}};
	IndexTable inv_tab(name, dtypes, col_names,
		db_path / "indexes", dm);
	IndexNavigator nav(&tab);
	for(auto& dt_l: nav.scan()) {
		auto nav_r = nav.look_up(dt_l.get_int_val()).second;
		// cout<<dt_l.get_int_val()<<endl;
		for(auto& dt_r: nav_r.scan()) {
			inv_tab.add_tuple(Tuple(vector<Data>{Data(dt_r.get_int_val()), 
				Data(dt_l.get_int_val())}, 1));
			// cout<<"\t"<<dt_r.get_int_val()<<endl;
		}
	}
	inv_tab.freeze();
	return inv_tab;
}

float n_rgl(float nlr, float nl, float nr) {
	return nr*(1-apowb(1-1.0/nr, ((float) nlr)/nl));
}

float expt_n_rgl(const IndexTable& tab) {
	IndexNavigator nav(&tab);
	float sum=0, num_values=0;
	for(auto& dt_l: nav.scan()) {
		auto nav_r = nav.look_up(dt_l.get_int_val()).second;
		num_values+=1;
		sum += nav_r.scan().size();
	}
	return sum/num_values;
}

void show_nodeids(const ExpressionTree& T) {
	for(auto nid: range(T.nodes.size())) {
		cout<<"("<<nid<<", "<<T.nodes[nid].name<<"), ";
	}
	cout<<endl;
}

void show_values(Index& index) {
	for(auto cg: index.cgs)
		for(auto col: cg)
			cout<<index.nodes[col].name<<", ";
	cout<<"\n[";
	for(auto& record: index.lookup(map<uint,int>{})) {
		cout<<"[";
		for(auto& val: record)
			cout<<val<<", ";
		cout<<"]\n";
	}
	cout<<"]\n";
}

void show_values(Rewriting& R, map<uint,int> values) {
	assert(R.iscomplete());
	for(uint i=0; i<R.query->nodes.size(); i++)
		cout<<R.query->nodes[i].name<<", ";
	cout<<"\n[";
	for(auto& record: execute_instance(&R, values)) {
		cout<<"[";
		for(auto& val: record)
			cout<<val<<", ";
		cout<<"]\n";
	}
	cout<<"]\n";
}

void show_results(const QueryTemplate* query, vector<vector<int>> result) {
	for(uint i=0; i<query->nodes.size(); i++)
		cout<<query->nodes[i].name<<", ";
	cout<<"\n[";
	for(auto& record: result) {
		cout<<"[";
		for(auto& val: record)
			cout<<val<<", ";
		cout<<"]\n";
	}
	cout<<"]\n";
}

int main(int argc, char* argv[]) {
	assert(argc==12);
	// initialze seed to make random numbers predictable
	srand (atoi(argv[11]));
	// schema 0
	DiskManager dm_schema("data");
	auto schema_json = json::parse(dm_schema.slurp("configurations.json"));
	cout<<schema_json<<endl;

	// fixed configuration parameters
	MCD::disk_seek_cost = 10000;
	Application::wt_maint = 0.1;
	Index::max_last_cg_size = numeric_limits<uint>::max();
	
	// input parameters
	int experiment_id=atoi(argv[1]);
	float scale_factor=stof(argv[2]);
	uint num_queries=atoi(argv[3]);
	string schema_code=argv[4];
	uint depth_per_query=atoi(argv[5]);
	uint nodes_per_query=atoi(argv[6]);
	uint iterative_deeping_param=atoi(argv[7]);
	int max_iters=atoi(argv[8]);
	int expand_param=atoi(argv[9]);
	uint pick_fn=atoi(argv[10]); //!< 0->best LB, 1->best UB, 2->least remaining goals

	// setting values
	Index::max_first_cg_size = 1010*scale_factor;
	Application::it_deep_k = iterative_deeping_param;
	assert(pick_fn<3);
	Application::pick_fn = pick_fn;
	Application::num_expand = expand_param;
	Application::max_iters = max_iters;
	cout<<Application::max_iters<<endl;

	// preparing variables

	list<Entity> ents;
	list<IndexTable> index_tables;
	list<IndexNavigator> index_navs;
	list<BaseRelation> base_relations;

	auto schema_data = schema_json[schema_code];
	cout<<schema_data<<endl;
	map<string, const Entity*> name2entity;
	// create entities
	for(auto it=schema_data["entities"].begin(); it!=schema_data["entities"].end(); it++) {
		uint num_elements=((int) it.value().begin().value()) * scale_factor;
		ents.push_back(Entity(it.value().begin().key(), num_elements, range(num_elements)));
		name2entity[it.value().begin().key()] = &ents.back();
		cout<<ents.back().name<<" "<<ents.back().cardinality<<endl;
	}
	cout<<"Building base index tables"<<endl;
	for(auto it=schema_data["base_relations"].begin(); it!=schema_data["base_relations"].end(); it++) {
		const Entity* left_ent = name2entity.at(it.value().begin().key());
		const Entity* right_ent = name2entity.at(it.value().begin().value().begin().key());
		uint num_tuples = it.value().begin().value().begin().value();
		index_tables.push_back(build_random_index(num_tuples, *left_ent, *right_ent));
		index_navs.push_back(IndexNavigator(&(index_tables.back())));
		base_relations.push_back(BaseRelation(left_ent->name+"-"+right_ent->name, *left_ent, *right_ent, 
			&index_tables.back(), n_rgl(num_tuples, left_ent->cardinality, right_ent->cardinality), 
			n_rgl(num_tuples, right_ent->cardinality, left_ent->cardinality)));
		index_tables.push_back(invert(index_tables.back()));
		index_navs.push_back(IndexNavigator(&(index_tables.back())));
		base_relations.push_back(BaseRelation(right_ent->name+"-"+left_ent->name, *right_ent, *left_ent, 
			&index_tables.back(), n_rgl(num_tuples, right_ent->cardinality, left_ent->cardinality), 
			n_rgl(num_tuples, left_ent->cardinality, right_ent->cardinality)));
		cout<<left_ent->name<<" "<<right_ent->name<<" "<<num_tuples<<endl;
	}

	map<const BaseRelation*, float> rel_dist;
	for(auto it=base_relations.begin(); it!=base_relations.end(); it++)
		rel_dist[&(*it)] = 1.0/base_relations.size();

	vector<float> depth_dist(depth_per_query+1, 1.0/depth_per_query);

	TreeBuilder rand_tree_builder(depth_dist, rel_dist);
	vector<const QueryTemplate*> workload;
	list<QueryTemplate> queries;
	
	// Generate queries
	float max_card=50;
	cout<<"Generating random query workload"<<endl;
	for(auto i : range(num_queries)) {
		QueryTemplate Q("Q"+to_string(i), rand()%10, rand_tree_builder.get_random_tree(NULL, depth_per_query, max_card, 0));
		while(abs((Q.nodes.size()-nodes_per_query)/(1.0*nodes_per_query)) > 0.1) {
			cout<<nodes_per_query<<" "<<Q.nodes.size()<<" "<<max_card<<endl;
			if(Q.nodes.size()>nodes_per_query) {
				max_card *= 1.1;
			}
			else
				max_card *= 0.9 ;
			Q = QueryTemplate("Q"+to_string(i), rand()%1000, rand_tree_builder.get_random_tree(NULL, depth_per_query, max_card, 0));
		}
		queries.push_back(Q);
		workload.push_back(&queries.back());
	}
	float norm_factor=0;
	for(auto it=queries.begin(); it!=queries.end(); it++)
		norm_factor+=it->weight;
	for(auto it=queries.begin(); it!=queries.end(); it++)
		it->weight /= norm_factor;

	// Console outputs
	cout<<"WORKLOAD: \n";
	for(auto query: workload) 
		cout<<query->show()<<endl;

	vector<const Entity*> entities;
	for(auto it=ents.begin(); it!=ents.end(); it++) 
		entities.push_back(&(*it));
	vector<const BaseRelation*> base_rels;
	for(auto it=base_relations.begin(); it!=base_relations.end(); it++) 
		base_rels.push_back(&(*it));

	Application app(entities, base_rels, workload, dm, db_path);
	app.generate_candidates(3, 1000000);
	cout<<"CANDIDATES: \n "<<app.show_candidates()<<"\n-----------------\n";

	auto result = app.optimize();
	cout<<"\n\n\nFINAL SOLUTION:\n";
	cout<<app.show_design()<<"\n-----------------\n";

	std::ofstream expts_csv("test/experiments.csv", std::ofstream::app);

    if(expts_csv.is_open()) {
        expts_csv<<experiment_id<<", "<<schema_code<<", "<<scale_factor<<", ";
        expts_csv<<num_queries<<", "<<depth_per_query<<", "<<nodes_per_query<<", ";
        expts_csv<<iterative_deeping_param<<", "<<max_iters<<", "<<expand_param<<", ";
        expts_csv<<pick_fn<<", "<<result.back().first<<", "<<result.back().second<<endl;
        expts_csv.close();
    }
    else cout<<"Unable to open file"<<endl;

    std::ofstream time_csv("test/time.csv", std::ofstream::app);

    if(time_csv.is_open()) {
        for(auto pr: result)
        	time_csv<<experiment_id<<", "<<pr.first<<", "<<pr.second<<endl;
        time_csv.close();
    }
    else cout<<"Unable to open file"<<endl;

	
	// app.create_indexes();
	// show_results(&Qe, app.execute_query_instance(&Qe, map<uint, int>{{2,0}, {3,1}, {4,1}}));
	// cout<<"Index kd values\n";
	// show_values(ind_kd);
	// cout<<"Index de values\n";
	// show_values(ind_de);
	// cout<<"Index ce values\n";
	// show_values(ind_ce);



	// Rewriting Qe_R_einv(&Qe, qe_goal2brmcds);
	// Qe_R_einv.add_mcd(&m_qe_k1dec);
	// Qe_R_einv.add_mcd(&m_qe_k2dec);
	// cout<<Qe_R_einv.timeoe(&m_qe_k1dec)+Qe_R_einv.timeoe(&m_qe_k2dec)+Application::wt_maint*(ind_ent_inv.maint_cost)<<endl;
	// cout<<Qe_R_einv.timeoe(&m_qe_k1dec)<<endl;

	// Rewriting Qe_R_dinv(&Qe, qe_goal2brmcds);
	// Qe_R_dinv.add_mcd(&m_qe_k1d);
	// Qe_R_dinv.add_mcd(&m_qe_k2d);
	// Qe_R_dinv.add_mcd(&m_qe_dce);
	// cout<<Qe_R_dinv.timeoe(&m_qe_k1d)+Qe_R_dinv.timeoe(&m_qe_k1d)+Qe_R_dinv.timeoe(&m_qe_dce)+Application::wt_maint*(ind_kd.maint_cost+ind_dinv.maint_cost)<<endl;
	// cout<<Qe_R_dinv.timeoe(&m_qe_dce)<<endl;

	// IndexTable ent_inv_table = create_index_table(&ind_ent_inv, "ent_inv", db_path, dm);
	// IndexNavigator ent_inv_nav(&ent_inv_table);
	// ind_ent_inv.set_index_table(&ent_inv_table, &ent_inv_nav);
	// show_values(ind_ent_inv);

	// Rewriting Qe_R(&Qe, qe_goal2brmcds);
	// Qe_R.add_mcd(&m_qe_k1d);
	// Qe_R.add_mcd(&m_qe_k2d);
	// Qe_R.add_mcd(&m_qe_de);
	// Qe_R.add_mcd(&m_qe_ce);
	// cout<<"\n-----------------\n";
	// show_values(Qe_R, map<uint,int>{{2,3}, {3,3}, {4,0}});
	// cout<<"\n-----------------\n";

	// Rewriting Qe_R_einv(&Qe, qe_goal2brmcds);
	// Qe_R_einv.add_mcd(&m_qe_k1dec);
	// Qe_R_einv.add_mcd(&m_qe_k2dec);
	// cout<<"\n-----------------\n";
	// show_values(Qe_R_einv, map<uint,int>{{2,3}, {3,3}, {4,0}});
	// cout<<"\n-----------------\n";



	// Old console output snippets

	// printf("%f %f \n", n_rgl(nkd, nk, nd), n_rgl(nkd, nd, nk));
	// printf("%f %f \n", n_rgl(nkd, nd, nk), n_rgl(nkd, nk, nd));
	// printf("%f %f \n", n_rgl(ned, ne, nd), n_rgl(ned, nd, ne));
	// printf("%f %f \n", n_rgl(ned, nd, ne), n_rgl(ned, ne, nd));
	// printf("%f %f \n", n_rgl(nec, ne, nc), n_rgl(nec, nc, ne));
	// printf("%f %f \n", n_rgl(nec, nc, ne), n_rgl(nec, ne, nc));
	//workload.push_back(&Qe);
	//workload.push_back(&Qe2);

	// cout<<ind_bd.show()<<"\n----------\n";
	// cout<<Qtoe.show()<<"\n----------\n";
	// cout<<Qtoe_R.timeoe(&m_qtoe_bd)<<"\n----------\n";
	// Qtoe_R.add_mcd(&m_qtoe_k2d1e);
	// cout<<Qtoe_R.timeoe(&m_qtoe_bd);




	return 0;
}
