#pragma once

#include "backend.hpp"

#include <queue>
#include <unordered_map>

template <typename Fn>
inline void dag_group_for_each_node_topo(DAG* dag, Fn&& fn) {
    std::queue<DAGNode*> topo_queue;
    std::unordered_map<DAGNode*, int> remaining_parents;
    remaining_parents.reserve(dag->nodes.size());

    for (auto& [id, node] : dag->nodes) {
        remaining_parents[node] = node->num_parents;
        if (node->num_parents == 0)
            topo_queue.push(node);
    }

    int processed = 0;
    while (!topo_queue.empty()) {
        DAGNode* dag_node = topo_queue.front();
        topo_queue.pop();
        processed++;
        fn(dag_node);

        for (DAGNode* child : dag_node->children) {
            auto it = remaining_parents.find(child);
            if (it == remaining_parents.end())
                continue;
            if (--it->second == 0)
                topo_queue.push(child);
        }
    }

    if (processed != static_cast<int>(dag->nodes.size()))
        CkAbort("dag_group_for_each_node_topo processed %d/%d nodes", processed,
                static_cast<int>(dag->nodes.size()));
}
