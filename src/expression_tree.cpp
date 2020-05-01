#include "utils.h"
#include "expression_tree.h"
#include "base_relation.h"

#include <string>
#include <vector>
#include <map>
#include <cstdlib>
#include <iostream>
#include <list>
#include <algorithm>
#include <queue>

using std::string;
using std::vector;
using std::to_string;
using std::cout;
using std::endl;
using std::map;
using std::list;
using esutils::apowb;
using std::pair;
using std::make_pair;
using std::queue;
using std::max;
using esutils::normalize;

ExpressionTree::Node::Node(const Entity* ent_ptr, bool isconst, int val, 
	std::string nm) : ent(ent_ptr), isconstant(isconst), 
	value(val), name(nm) {}

string ExpressionTree::Node::show() const {
	string result = name+"("+ent->name+")";
	if(isconstant)
		result += "["+to_string(value)+"]";
	return result;
}

ExpressionTree::ExpressionTree(const Entity* ent_ptr, std::string name, 
	int value, bool isconst) {
	assert(ent_ptr!=NULL);
	leaf_ids.push_back(0);
	root_id=0;
	nodes.push_back(Node(ent_ptr, isconst, value, name));
	parent_ids.push_back(-1);
	children_ids.push_back(vector<uint>(0));
	parent_rels.push_back(NULL);
	children_rels.push_back(vector<const BaseRelation*>(0));

	compute_cardinalities();
}

ExpressionTree::ExpressionTree(const Entity* root, string name, vector<const ExpressionTree*> subtrees, 
		vector<const BaseRelation*> subtree_rels) {
	assert(subtrees.size()!=0);
	assert(root!=NULL);
	nodes.push_back(Node(root, false, 0, name));
	root_id = 0;
	parent_ids.push_back(-1);
	children_ids.push_back(vector<uint>(0));
	parent_rels.push_back(NULL);
	children_rels.push_back(vector<const BaseRelation*>(0));

	//cout<<"===============================\n";
	for(uint pos=0; pos<subtrees.size(); pos++) {
		auto subtree = subtrees[pos];
		assert(subtree!=NULL);
		//cout<<subtree->show().c_str()<<"\n----------------\n";
		uint offset = nodes.size();
		for(uint i=0; i<subtree->nodes.size(); i++) {
			nodes.push_back(subtree->nodes[i]);
			parent_ids.push_back(subtree->parent_ids[i]+offset);
			vector<uint> cids;
			for(auto cid: subtree->children_ids[i])
				cids.push_back(cid+offset);
			children_ids.push_back(cids);
			parent_rels.push_back(subtree->parent_rels[i]);
			children_rels.push_back(subtree->children_rels[i]);
		}
		parent_ids[subtree->root_id + offset] = 0;
		parent_rels[subtree->root_id + offset] = subtree_rels[pos];
		children_ids[0].push_back(subtree->root_id + offset);
		children_rels[0].push_back(subtree_rels[pos]);
		assert(subtree_rels[pos]->left_ent == nodes[subtree->root_id + offset].ent);
		assert(subtree_rels[pos]->right_ent == nodes[root_id].ent);
		for(uint i=0; i<subtree->leaf_ids.size(); i++)
			leaf_ids.push_back(subtree->leaf_ids[i]+offset);
	}
	compute_cardinalities();
	//cout<<"===============================\n";
}

float ExpressionTree::compute_instance_card(uint nid, int pid) {
	//cout<<nid<<" "<<pid;
	float instance_card = (children_ids[nid].size()==0 ? 1 : nodes[nid].ent->cardinality);
	if(parent_ids[nid]!=-1 && parent_ids[nid]!=pid) 
		instance_card *= (1-apowb(1 - parent_rels[nid]->n_lgr / nodes[nid].ent->cardinality, compute_instance_card(parent_ids[nid], nid)));
	for(uint i=0; i< children_ids[nid].size(); i++)
		if(((int) children_ids[nid][i])!=pid)
			instance_card *= (1-apowb(1 - children_rels[nid][i]->n_rgl / nodes[nid].ent->cardinality, 
				compute_instance_card(children_ids[nid][i], nid)));
	return instance_card;
}

float ExpressionTree::compute_total_card(uint nid, int pid) {
	//cout<<nid<<" "<<pid;
	float total_card = (nodes[nid].isconstant ? 1 : nodes[nid].ent->cardinality);
	if(parent_ids[nid]!=-1 && parent_ids[nid]!=pid) 
		total_card *= (1-apowb(1 - parent_rels[nid]->n_lgr / nodes[nid].ent->cardinality, compute_total_card(parent_ids[nid], nid)));
	for(uint i=0; i< children_ids[nid].size(); i++)
		if(((int) children_ids[nid][i])!=pid)
			total_card *= (1-apowb(1 - children_rels[nid][i]->n_rgl / nodes[nid].ent->cardinality, 
				compute_total_card(children_ids[nid][i], nid)));
	return total_card;
}

void ExpressionTree::compute_cardinalities() {
	instance_cardinalities.clear();
	for(uint nid=0; nid<nodes.size(); nid++) 
		instance_cardinalities.push_back(compute_instance_card(nid, -1));

	total_cardinalities.clear();
	for(uint nid=0; nid<nodes.size(); nid++) 
		total_cardinalities.push_back(compute_total_card(nid, -1));
}

string ExpressionTree::show(uint id, uint level) const {
	string result = string(level, '\t') + nodes[id].show();
	result += ", ic: "+to_string(instance_cardinalities[id])+", "; 
	result += ", tc: "+to_string(total_cardinalities[id])+" {"+"\n";
	for(uint i=0; i<children_ids[id].size(); i++) {
		result += string(level+1, '\t') + to_string(i)+"th child: "+ children_rels[id][i]->name+"\n" + show(children_ids[id][i], level+1);
	}
	result +=  string(level, '\t')+"}\n";
	return result;
}

string ExpressionTree::show() const{
	return show(root_id, 0);
}

////////////////////////////////////////////////////////////

TreeBuilder::TreeBuilder(vector<float> dd, map<const BaseRelation*, float> rd) : 
	depth_dist(dd), rel_dist(rd) {}

const Entity* TreeBuilder::sample_target_entity() {
	map<const Entity*, float> Pent;
	for(auto it=rel_dist.begin(); it!=rel_dist.end(); it++)
		if(Pent.find(it->first->right_ent)==Pent.end())
			Pent[it->first->right_ent] = it->second;
		else
			Pent[it->first->right_ent] += it->second;
	vector<const Entity*> ents;
	vector<float> probs;
	for(auto it=Pent.begin(); it!=Pent.end(); it++) {
		ents.push_back(it->first);
		probs.push_back(it->second);
	}
	return ents[rng.sample(probs)];
}

pair<vector<const BaseRelation*>, vector<float>> 
	TreeBuilder::conditional_rel_dist(const Entity* target_entity) {
	vector<const BaseRelation*> rels;
	vector<float> rel_probs;
	for(auto it=rel_dist.begin(); it!=rel_dist.end(); it++)
		if(it->first->right_ent == target_entity) {
			rels.push_back(it->first);
			rel_probs.push_back(it->second);
		}
	rel_probs = normalize(rel_probs);
	return make_pair(rels, rel_probs);
}

ExpressionTree TreeBuilder::get_random_tree(const Entity* root_ent, int depth,
		float max_card, float prob_const) {
	assert(max_card>0.1);
	if(root_ent==NULL)
		root_ent = sample_target_entity();
	if(depth<0)
		depth = rng.sample(depth_dist);

	if(depth==0) {
		if(num_vars.find(root_ent->name)==num_vars.end())
			num_vars[root_ent->name] = 0;
		string name = root_ent->name + to_string(num_vars[root_ent->name]++);
		bool isconst=false;  int value=0;
		if(root_ent->cardinality == root_ent->values.size() && root_ent->cardinality<=10) {
			isconst = (rng.sample(vector<float>{prob_const, 1-prob_const})==0);
			value = root_ent->values[rand() % root_ent->cardinality];
		}
		return ExpressionTree(root_ent, name, value, isconst);
	}
	else {
		auto result = conditional_rel_dist(root_ent);
		auto rels = result.first; 
		auto rel_probs = result.second;
		vector<float> new_depth_dist = depth_dist;
		new_depth_dist.resize(depth+1);
		new_depth_dist.erase(new_depth_dist.begin());
		new_depth_dist = normalize(new_depth_dist);

		float root_card = root_ent->cardinality;
		list<ExpressionTree> subtrees;
		vector<const ExpressionTree*> subtree_ptrs;
		vector<const BaseRelation*> subtree_rels; 

		// constructing first subtree of depth 'depth-1'
		subtree_rels.push_back(rels[rng.sample(rel_probs)]);
		subtrees.push_back(get_random_tree(subtree_rels.back()->left_ent, depth-1, max_card, prob_const));
		subtree_ptrs.push_back(&subtrees.back());
		float factor = 1-apowb(1-subtree_rels.back()->n_rgl / root_ent->cardinality, 
			subtrees.back().instance_cardinalities[subtrees.back().root_id]);
		root_card *= factor;

		while(root_card > max_card) {
			int subtree_depth = (int) rng.sample(new_depth_dist);
			subtree_rels.push_back(rels[rng.sample(rel_probs)]);
			subtrees.push_back(get_random_tree(subtree_rels.back()->left_ent, subtree_depth, max_card, prob_const));
			subtree_ptrs.push_back(&subtrees.back());
			float factor = 1-apowb(1-subtree_rels.back()->n_rgl / root_ent->cardinality, 
				subtrees.back().instance_cardinalities[subtrees.back().root_id]);
			root_card *= factor;
		}
		string name = root_ent->name + to_string(num_vars[root_ent->name]++);
		return ExpressionTree(root_ent, name, subtree_ptrs, 
			 subtree_rels);
	}

}


ExpressionTree TreeBuilder::make_tree(const ExpressionTree& tree, uint cid, uint pid) {
	assert((cid<tree.nodes.size()) && (pid<tree.nodes.size()));
	assert(tree.parent_ids[cid]== (int)pid);
	ExpressionTree child(tree.nodes[cid].ent, tree.nodes[cid].name, tree.nodes[cid].value, tree.nodes[cid].isconstant);
	auto result = ExpressionTree(tree.nodes[pid].ent, tree.nodes[pid].name, vector<const ExpressionTree*>{&child}, 
		vector<const BaseRelation*>{tree.parent_rels[cid]});
	// cout<<result.show()<<endl;
	return result;
}

void TreeBuilder::add_leaf(ExpressionTree& tree, uint nid, const BaseRelation* rel, string name, int value, bool isconst) {
	if(tree.children_ids[nid].size()==0) {
		int idx=-1;
		for(uint i=0; i<tree.leaf_ids.size(); i++)
			if(tree.leaf_ids[i]==nid) {
				idx = i; break;
			}
		assert(idx!=-1);
		tree.leaf_ids.erase(tree.leaf_ids.begin()+idx);
	}
	tree.nodes.push_back(ExpressionTree::Node(rel->left_ent, isconst, value, name));
	tree.parent_ids.push_back(nid);
	tree.parent_rels.push_back(rel);
	tree.children_ids.push_back(vector<uint>{});
	tree.children_ids[nid].push_back(tree.nodes.size()-1);
	tree.children_rels.push_back(vector<const BaseRelation*>{});
	tree.children_rels[nid].push_back(rel);
	tree.leaf_ids.push_back(tree.nodes.size()-1);
	tree.compute_cardinalities();

	for(uint id=0; id<tree.nodes.size(); id++)
		if(tree.children_ids[id].size()==0) {
			int idx=-1;
			for(uint i=0; i<tree.leaf_ids.size(); i++)
				if(tree.leaf_ids[i]==id) {
					idx = i; break;
				}
			assert(idx!=-1);
		}
}

void TreeBuilder::add_root(ExpressionTree& tree, const BaseRelation* rel, string name) {
	tree.nodes.push_back(ExpressionTree::Node(rel->right_ent, false, 0, name));
	tree.parent_ids[tree.root_id] = tree.nodes.size()-1;
	tree.parent_ids.push_back(-1);
	tree.parent_rels[tree.root_id] = rel;
	tree.parent_rels.push_back(NULL);
	tree.children_ids.push_back(vector<uint>{tree.root_id});
	tree.children_rels.push_back(vector<const BaseRelation*>{rel});

	tree.root_id = tree.nodes.size()-1;
	tree.compute_cardinalities();
}