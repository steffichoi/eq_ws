f = open("relations.txt", "r")

line = f.readline().strip('\n')
node_map = {}
# create the node mapping (could also define a node class with edges, but this is faster for dev purposes)
while (line):
    task_1 = line.split('->')[0]
    task_2 = line.split('->')[1]
    if (task_1 not in node_map):
        node_map[task_1] = [task_2]
    else:
        node_map[task_1].append(task_2)

    line = f.readline().strip('\n')
f.close()

# get the start and goal tasks
f = open("question.txt", "r")
start = f.readline().strip('\n').split(': ')[-1]
end = f.readline().strip('\n').split(': ')[-1]
f.close()

# start = '39'
# end = '62'

# keep track of the node being explored and initialize path
cur_node = start
path = [start]
# keep track of which nodes have alternative routes 
# so we can back track if the current path has no solution(DFS)
split = []
while (cur_node != end):

    if (cur_node in node_map):
        next_node = node_map[cur_node][0]
    else:
        not_ok = True
        while not_ok:
            # remove the node that can't reach goal node
            no_path = cur_node
            path.remove(no_path)
            # if path is empty then we have checked all paths from start node and there is no solution
            # exit program
            if (len(path) == 0):
                print('no path available')
                exit()
            # otherwise, set current node to the last node
            # and remove the no_path node from it's list of possible tasks
            cur_node = path[-1]
            node_map[cur_node].remove(no_path)

            # if there are possible nodes that can still reach the goal 
            # from the current last node in path, set next_node to that node
            # otherwise, repeat the loop until a possible path is found 
            # (keep removing the last node in the path until there are possible paths to goal node)
            if (len(node_map[cur_node])>0):
                next_node = node_map[cur_node][0]
                not_ok = False

    path.append(next_node)
    # keep track of nodes with alternative paths and don't add twice
    if (len(node_map[cur_node]) > 1 and cur_node not in split):
        split.append(cur_node)
    
    cur_node = next_node
    
    # check that there are not more than 6 tasks
    # if there are 6 tasks and the last task is not the goal, find an alternative path
    if (len(path) == 6 and next_node != end):
        
        # if there is no node with apternative paths, there is no possible path
        if (len(split) == 0):
            print('no path available')
            exit()

        # check the list for the last node with altenative path 
        split_node = split[-1]

        # get the index of the next task belonging to the node with alternative paths
        # remove invalid node from list of dependent tasks
        idx = path.index(split_node)+1
        node_map[split_node].remove(path[idx])

        # new path does not include invalid node
        path = path[0:idx]

        # if there is only one path left, remove it from the list of nodes with alternative paths
        if (len(node_map[split_node]) == 1):
            split.remove(split_node)

        cur_node = path[-1]

f = open('results.txt', 'w')
f.write(str(path))
f.close()

print(path)