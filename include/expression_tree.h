#ifndef EXPRESSIONTREE_H
#define EXPRESSIONTREE_H

#include "base_relation.h"
#include "utils.h"

#include <string>
#include <vector>
#include <set>
#include <map>

class TreeBuilder;

struct ExpressionTree {
	struct Node {
		const Entity* ent;
		const bool isconstant;
		const int value;
		const std::string name;
		Node(const Entity* ent_ptr, bool isconst, int val, std::string nm);
		std::string show() const;
	};
	std::string show(uint id, uint level) const;

	std::vector<Node> nodes;
	std::vector<uint> leaf_ids;
	uint root_id;
	std::vector<int> parent_ids;
	std::vector<float> instance_cardinalities;
	std::vector<float> total_cardinalities;
	std::vector<std::vector<uint>> children_ids;
	std::vector<const BaseRelation*> parent_rels;
	std::vector<std::vector<const BaseRelation*>> children_rels;

	friend TreeBuilder;
	ExpressionTree(const Entity* ent_ptr, std::string name, int value, bool isconst); //!< constructor for single node tree
	ExpressionTree(const Entity* root, std::string name, std::vector<const ExpressionTree*> subtrees, 
		std::vector<const BaseRelation*> subtree_rels);  //!< construct a tree with given root node and a list of its child trees
	
	std::string show() const;
	float compute_instance_card(uint nid, int pid); //!< helper function for compute_cardinalities
	float compute_total_card(uint nid, int pid); //!< helper function for compute_cardinalities
	void compute_cardinalities();  //!< clears previous cardinality estimates and recomputes them
};


class TreeBuilder {
	esutils::random_number_generator rng;  
	/**a multinomial distribution on depth. For a single node tree, depth=0. During random tree generation,
	depth of the generated tree is sampled from this distribution*/
	std::vector<float> depth_dist;  
	/**a multinomial distribution on relations to use. During random tree generation,
	edges of the generated tree are sampled using this distribution*/
	std::map<const BaseRelation*, float> rel_dist;

	std::map<std::string, uint> num_vars;  //!< keeps track of number of nodes produced for a given name of entity

	const Entity* sample_target_entity();  //!< uses rel_dist to obtain a multinomial distribution over target entities. Then samples from this multinomial distribution
	std::pair<std::vector<const BaseRelation*>, std::vector<float>> 
		conditional_rel_dist(const Entity* target_entity); //!< returns a conditional probability of generating a relation given its target entity
public:
	TreeBuilder(std::vector<float> dd, std::map<const BaseRelation*, float> rd);
	/**Generate a random tree.
	Params: 
	1. root_ent : root entity. Generates a random entity is root_ent is NULL
	2. depth : depth of the generated tree. If depth <0, it samples a depth from depth_dist
	3. max_card : in the generated tree, every internal node will have estimated cardinality <= max_card
	4. prob_const : bernoulli distribution for a leaf node to take constant value provided cardinality of the underlying entity is <=10
	*/
	ExpressionTree get_random_tree(const Entity* root_ent, int depth, 
		float max_card, float prob_const);

	ExpressionTree make_tree(const ExpressionTree& tree, uint cid, uint pid);  //!< returns a tree containing a single edge matching the given edge (child id, parent id)
	/**Adds a new leaf, with given specification, as a child of node par in the given tree.
	It makes sure the previous ids are preserved.*/
	void add_leaf(ExpressionTree& tree, uint nid, const BaseRelation* rel, std::string name, int value, bool isconst); 
	/**Creates a new root, with given specification, as the parent of old root.
	It makes sure that the previous ids are preserved*/
	void add_root(ExpressionTree& tree, const BaseRelation* rel, std::string name); 

};

#endif