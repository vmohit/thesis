import time
import subprocess
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def consume_expt_data():
	line = ""
	with open("test/experiments.csv", 'r') as f:
		line = f.readlines()[0]
	open('test/experiments.csv', 'w').close()
	line = line.strip().split()
	line = [tok.split(',')[0] for tok in line]
	return line

def consume_time_data():
	lines = []
	with open("test/time.csv", 'r') as f:
		lines = [[tok.split(',')[0] for tok in line.strip().split()] for line in f.readlines()]
	open('test/time.csv', 'w').close()
	return lines

def reset_files():
	open('test/experiments.csv', 'w').close()
	open('test/time.csv', 'w').close()

reset_files()

def run_expt(exp_id, scale_factor, num_queries, schema_code,
				depth_per_query, nodes_per_query, iter_deep_param, max_iters, expand_param, pick_fn, seed):
	bashCommand = "./bin/experiments {} {} {} {} {} {} {} {} {} {} {}".format(exp_id, scale_factor, num_queries, schema_code,
				depth_per_query, nodes_per_query, iter_deep_param, max_iters, expand_param, pick_fn, seed)
	print ("running {}".format(bashCommand))
	process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
	output, error = process.communicate()
	print (error)

def normalize(lst):
	lst = [ele-min(lst) for ele in lst]
	lst = [ele/max(lst) for ele in lst]
	return lst

# ###################################################

# reset_files()

# exp_id=1
# scale_factor=0.6
# num_queries=1
# schema_code="1"
# depth_per_query=4
# nodes_per_query = 6
# iter_deep_param = 0
# max_iters = -1
# expand_param = -1
# pick_fn = 0
# seed = 0

# schema_codes = ["0", "1"]
# num_node_vals = []
# schema_values = [[], []]
# for nodes_per_query in range(6,12):
# 	num_node_vals.append(nodes_per_query)
# 	for sch in range(2):
# 		total = 0
# 		nexpts = 3
# 		for _ in range(nexpts):
# 			seed+=1
# 			run_expt(exp_id, scale_factor, num_queries, schema_codes[sch],
# 				depth_per_query, nodes_per_query, iter_deep_param, max_iters, expand_param, pick_fn, seed)
# 			expt_data = consume_expt_data()
# 			print (expt_data)
# 			total += float(expt_data[-2])
# 		schema_values[sch].append(total/nexpts)

# plt.title('Effect of nodes per query', fontsize=20)
# plt.plot(num_node_vals, schema_values[0], '-g', label='entity search')
# plt.plot(num_node_vals, schema_values[1], ':b', label='IMDB')
# plt.legend()
# plt.xlabel('number of nodes', fontsize=18)
# plt.ylabel('number of states expanded', fontsize=16)
# plt.savefig("test/numnodes.png")

# ############################################################

# time to optimize vs depth

# reset_files()

# exp_id=1
# scale_factor=0.6
# num_queries=1
# schema_code="1"
# depth_per_query=4
# nodes_per_query = 10
# iter_deep_param = 0
# max_iters = -1
# expand_param = -1
# pick_fn = 0
# seed = 0

# schema_codes = ["0", "1"]
# depth_vals = []
# schema_values = [[], []]
# for depth_per_query in range(3,8):
# 	depth_vals.append(depth_per_query)
# 	for sch in range(2):
# 		total = 0
# 		nexpts = 3
# 		for _ in range(nexpts):
# 			seed+=1
# 			run_expt(exp_id, scale_factor, num_queries, schema_codes[sch],
# 				depth_per_query, nodes_per_query, iter_deep_param, max_iters, expand_param, pick_fn, seed)
# 			expt_data = consume_expt_data()
# 			print (expt_data)
# 			total += float(expt_data[-2])
# 		schema_values[sch].append(total/nexpts)

# plt.title('Effect of depth on optimization time', fontsize=20)
# plt.plot(depth_vals, schema_values[0], '-g', label='entity search')
# plt.plot(depth_vals, schema_values[1], ':b', label='IMDB')
# plt.legend()
# plt.xlabel('depth', fontsize=18)
# plt.ylabel('number of states expanded', fontsize=16)
# plt.savefig("test/depth.png")

###################################################

# time to optimize vs num queries

# reset_files()

# exp_id=1
# scale_factor=0.6
# num_queries=1
# schema_code="1"
# depth_per_query=3
# nodes_per_query = 4
# iter_deep_param = 0
# max_iters = -1
# expand_param = -1
# pick_fn = 0
# seed = 0

# schema_codes = ["0", "1"]
# num_query_vals = []
# schema_values = [[], []]
# for num_queries in range(1,6):
# 	num_query_vals.append(num_queries)
# 	for sch in range(2):
# 		total = 0
# 		nexpts = 3
# 		for _ in range(nexpts):
# 			seed+=1
# 			run_expt(exp_id, scale_factor, num_queries, schema_codes[sch],
# 				depth_per_query, nodes_per_query, iter_deep_param, max_iters, expand_param, pick_fn, seed)
# 			expt_data = consume_expt_data()
# 			print (expt_data)
# 			total += float(expt_data[-2])
# 		schema_values[sch].append(total/nexpts)

# plt.title('Number of queries on optimization time', fontsize=20)
# plt.plot(num_query_vals, schema_values[0], '-g', label='entity search')
# plt.plot(num_query_vals, schema_values[1], ':b', label='IMDB')
# plt.legend()
# plt.xlabel('number of queries', fontsize=18)
# plt.ylabel('number of states expanded', fontsize=16)
# plt.savefig("test/numqueries.png")


############################

# reset_files()

# exp_id=3
# scale_factor=0.6
# num_queries=2
# schema_code="1"
# depth_per_query=3
# nodes_per_query = 7
# iter_deep_param = 0
# max_iters = -1
# expand_param = -1
# pick_fn = 0
# seed = 0

# schema_codes = ["0", "1"]
# schema_values = [[], []]
# for sch in range(2):
# 	seed+=1
# 	run_expt(exp_id, scale_factor, num_queries, schema_codes[sch],
# 		depth_per_query, nodes_per_query, iter_deep_param, max_iters, expand_param, pick_fn, seed)
# 	time_data = consume_time_data()
# 	print (consume_expt_data())
# 	print (time_data)
# 	indices = [int(row[1]) for row in time_data]
# 	costs = [float(row[2]) for row in time_data]
# 	costs = normalize(costs)
# 	schema_values[sch] = (indices, costs)

# plt.title('Cost vs no. of expanded nodes', fontsize=20)
# plt.plot(schema_values[0][0], schema_values[0][1], '-g', label='entity search')
# plt.plot(schema_values[1][0], schema_values[1][1], ':b', label='IMDB')
# plt.legend()
# plt.xlabel('number of states expanded', fontsize=18)
# plt.ylabel('least upper bound on cost', fontsize=16)
# plt.savefig("test/time_profile_exhaustive.png")


############################

# reset_files()
# exp_id=1
# scale_factor=0.6
# iter_deep_param = 0
# max_iters = -1
# expand_param = -1
# pick_fn = 0
# seed = 3
# schema_code="1"

# num_queries = 2
# nodes_per_query = 6
# depth_per_query = 3

# avg_nodes_expanded = [0 for i in range(6)]
# avg_cost = [0 for i in range(6)]
# N=3
# for seed in range(N):
# 	run_expt(exp_id, scale_factor, num_queries, schema_code,
# 			depth_per_query, nodes_per_query,iter_deep_param, max_iters, expand_param, pick_fn, seed)
# 	expt_data = consume_expt_data()
# 	print (expt_data)
# 	opt_nodes_expanded = float(expt_data[-2])
# 	opt_cost = float(expt_data[-1])

# 	i=0
# 	for expand_param in [1,2,3,4,5,6]:
# 		run_expt(exp_id, scale_factor, num_queries, schema_code,
# 			depth_per_query, nodes_per_query,iter_deep_param, max_iters, expand_param, pick_fn, seed)
# 		expt_data = consume_expt_data()
# 		print (expt_data)
# 		avg_nodes_expanded[i] += float(expt_data[-2])/opt_nodes_expanded
# 		avg_cost[i] += float(expt_data[-1])/opt_cost
# 		i+=1
# avg_nodes_expanded = [ele/N for ele in avg_nodes_expanded]
# avg_cost = [ele/N for ele in avg_cost]
# plt.title('Fraction of expanded nodes w.r.t exhaustive', fontsize=20)
# plt.plot([i+1 for i in range(6)], avg_nodes_expanded)
# plt.xlabel('number of neighbor expanded per step', fontsize=18)
# plt.ylabel('fraction of nodes expanded ', fontsize=16)
# plt.savefig("test/expand_node_expansion.png")

# plt.clf()

# plt.title('Relative cost w.r.t exhaustive', fontsize=20)
# plt.plot([i+1 for i in range(6)], avg_cost)
# plt.xlabel('number of neighbor expanded per step', fontsize=18)
# plt.ylabel('cost relative to exhaustive', fontsize=16)
# plt.savefig("test/expand_cost.png")
#############################

# reset_files()
# exp_id=1
# scale_factor=0.6
# iter_deep_param = 0
# max_iters = 5000
# expand_param = -1
# pick_fn = 0
# seed = 0

# schema_code="0"
# schema_codes = ["0", "1"]

# num_queries = 3
# nodes_per_query = 8
# depth_per_query = 3
# schema_values = [[0,0,0], [0,0,0]]
# for sch in [0,1]:
# 	for pick_fn in [0,1,2]:
# 		run_expt(exp_id, scale_factor, num_queries, schema_codes[sch],
# 			depth_per_query, nodes_per_query,iter_deep_param, max_iters, expand_param, pick_fn, seed)
# 		time_data = consume_time_data()
# 		print (consume_expt_data())
# 		print (time_data)
# 		indices = [int(row[1]) for row in time_data]
# 		costs = [float(row[2]) for row in time_data]
# 		costs = normalize(costs)
# 		schema_values[sch][pick_fn] = (indices, costs)

# plt.title('Comparision of pick function (entity search)', fontsize=20)
# plt.plot(schema_values[0][0][0], schema_values[0][0][1], '-g', label='lb_cost')
# plt.plot(schema_values[0][1][0], schema_values[0][1][1], ':b', label='ub_cost')
# plt.plot(schema_values[0][2][0], schema_values[0][2][1], '-r', label='num_rem_goals')
# plt.legend()
# plt.xlabel('number of states expanded', fontsize=18)
# plt.ylabel('least upper bound on cost', fontsize=16)
# plt.savefig("test/time_profile_pick_entity_search.png")

# plt.clf()

# plt.title('Comparision of pick function (IMDB)', fontsize=20)
# plt.plot(schema_values[1][0][0], schema_values[1][0][1], '-g', label='lb_cost')
# plt.plot(schema_values[1][1][0], schema_values[1][1][1], ':b', label='ub_cost')
# plt.plot(schema_values[1][2][0], schema_values[1][2][1], '-r', label='num_rem_goals')
# plt.legend()
# plt.xlabel('number of states expanded', fontsize=18)
# plt.ylabel('least upper bound on cost', fontsize=16)
# plt.savefig("test/time_profile_pick_imdb.png")
##########################

reset_files()
exp_id=1
scale_factor=0.6
iter_deep_param = 0
max_iters = -1
expand_param = -1
pick_fn = 0
seed = 0

schema_code = "0"
schema_codes = ["0", "1"]

num_queries = 3
nodes_per_query = 6
depth_per_query = 3
schema_values = [[0 for _ in range(5)], [0 for _ in range(5)]]
for sch in [0,1]:
	for seed in range(5):
		i = 0
		for iter_deep_param in [0,2,4,6,8]:
			t = time.process_time()
			run_expt(exp_id, scale_factor, num_queries, schema_codes[sch],
				depth_per_query, nodes_per_query,iter_deep_param, max_iters, expand_param, pick_fn, seed)
			print (consume_expt_data())
			print (consume_time_data())
			elapsed_time = time.process_time() - t
			print (elapsed_time)
			schema_values[sch][i] += elapsed_time
			i+=1

for sch in [0,1]:
	schema_values[sch] = [ele/5 for ele in schema_values[sch]]
plt.title('Effects of reducing calls to WSC algorithm', fontsize=20)
plt.plot([0,2,4,6,8], schema_values[0], '-g', label='entity search')
plt.plot([0,2,4,6,8], schema_values[1], ':b', label='IMDB')
plt.legend()
plt.xlabel('Bound update frequency "l"', fontsize=18)
plt.ylabel('time taken to find the optimal', fontsize=16)
plt.savefig("test/time_iterative_deep.png")