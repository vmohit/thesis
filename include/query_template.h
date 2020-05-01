#ifndef QUERYTEMPLATE_H
#define QUERYTEMPLATE_H

#include "expression_tree.h"

#include <string>
#include <vector>
#include <map>
#include <set>
#include <cstdlib>

/**Query template defined using a tree. Non-constant leaves of the tree are inputs (or bound head variables)
 and all internal nodes are output (or free head variables)*/
struct QueryTemplate : public ExpressionTree {
	std::string name;  
	float weight;
	std::map<std::pair<uint,uint>, uint> edge2goalid;  //!< edge2goalid[(id1, id2)] = id of goal corresponding to child-parent edge (id1, id2)
	std::map<uint, std::pair<uint,uint>> goalid2edge;  //!< goalid2edge[gid] = child-parent edge (id1, id2) for goal gid
	std::set<uint> goals;
	std::vector<uint> goal_order;  //!< query is executed by including goals from mcds in the order specified 
	std::vector<uint> node2root_distance; 
	//std::vector<uint> col_order;

	QueryTemplate(std::string nm, float wt, const ExpressionTree argT);

	std::string show() const;
};



#endif