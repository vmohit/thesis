#ifndef INDEX_H
#define INDEX_H

#include "expression_tree.h"
#include "index_table.h"

#include <string>
#include <vector>
#include <map>
#include <set>

/**Index is an expression tree whose executions are stored*/
struct Index : public ExpressionTree {
	std::vector<uint> levels;  //!< levels[node_id] = longest distance from a leaf. levels[leaf]=1
	std::vector<std::vector<int>> cgs; //!< column groups made up of nodes ids of non-constant nodes
	std::set<int> firstcg;
	std::vector<uint> col_order; //!< col_order[i] = node id of ith column
	std::vector<uint> col_order_inv; //!< col_order_inv[nid] = column number of node nid
	std::vector<float> size_cgs;  //!< sizes of column groups
	std::map<int, float> card_col;  //!< card_col[nodeid] = cardinality of nodeid when all columns before it take a constant value
	const IndexTable* table=NULL;  //!< implementation of the index structure
	const IndexNavigator* first_col_nav=NULL;
	float maint_cost;
	std::string name;

	//!< configuration variables
	static uint max_first_cg_size; //!< max_first_cg_size places limits on the size of the first column group as it is cached.
	static uint max_last_cg_size;  //!< last column group should not have more than 100 values if possible

	uint compute_levels(uint nodeid);  

	Index(const ExpressionTree argT);  

	std::string show() const;

	std::vector<std::vector<int>> lookup(const std::map<uint, int>& lookup_values, uint cg, uint colid, const IndexNavigator* nav) const;
	std::vector<std::vector<int>> lookup(const std::map<uint, int>& lookup_values) const;
	void set_index_table(const IndexTable* tab, const IndexNavigator* nav);
};


#endif