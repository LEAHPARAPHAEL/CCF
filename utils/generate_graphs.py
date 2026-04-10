import os
import random

def random_graph(n: int, m: int, seed: int = 42) -> list:
    edge_rng = random.Random(seed)
    m = min(m, n * (n - 1) // 2)
    edge_set = set()
    while len(edge_set) < m:
        u, v = edge_rng.randint(0, n - 1), edge_rng.randint(0, n - 1)
        if u != v:
            edge_set.add((min(u, v), max(u, v)))
    return list(edge_set)

def chain_graph(n: int) -> list:
    return [(i, i + 1) for i in range(n - 1)]

def star_graph(n: int) -> list:
    return [(0, i) for i in range(1, n)]

def multi_component_graph(n: int, k: int, m: int, seed: int = 42) -> list:
    edge_rng = random.Random(seed)
    
    k = max(1, min(k, n))
    sizes = [(n // k) + (1 if i < n % k else 0) for i in range(k)]
    capacities = [size * (size - 1) // 2 for size in sizes]
    total_capacity = sum(capacities)
    
    m = max(0, min(m, total_capacity))
    m_assigned = [0] * k
    if total_capacity > 0:
        for i in range(k):
            m_assigned[i] = int(m * (capacities[i] / total_capacity))

        remainder = m - sum(m_assigned)
        idx = 0
        while remainder > 0:
            if m_assigned[idx] < capacities[idx]:
                m_assigned[idx] += 1
                remainder -= 1
            idx = (idx + 1) % k

    edges = []
    offset = 0
    for i in range(k):
        comp_edges = random_graph(sizes[i], m_assigned[i], seed=edge_rng.randint(0, 10**6))
        for u, v in comp_edges:
            edges.append((u + offset, v + offset))
        offset += sizes[i]
        
    return edges

def scale_free_graph(n: int, m0: int = 3, seed: int = 42) -> list:
    if n <= m0:
        return [(i, j) for i in range(n) for j in range(i + 1, n)]

    edge_rng = random.Random(seed)
    edges = []

    for i in range(m0):
        for j in range(i + 1, m0):
            edges.append((i, j))
            
    pool = [i for i in range(m0) for _ in range(m0 - 1)] 
    
    if m0 == 1:
        pool = [0]

    for new_node in range(m0, n):
        targets = set()

        while len(targets) < m0:
            targets.add(edge_rng.choice(pool))
            
        for t in targets:
            edges.append((min(t, new_node), max(t, new_node)))

            pool.append(t)
            pool.append(new_node)
            
    return edges

def social_community_graph(n: int, k: int, m_internal: int, m_external: int, seed: int = 42) -> list:
    edge_rng = random.Random(seed)
    edge_set = set()
    
    def get_community(node): return node % k

    while len(edge_set) < m_internal:
        u = edge_rng.randint(0, n - 1)
        v = edge_rng.randint(0, n - 1)
        if u != v and get_community(u) == get_community(v):
            edge_set.add((min(u, v), max(u, v)))

    bridge_edges = set()
    while len(bridge_edges) < m_external:
        u = edge_rng.randint(0, n - 1)
        v = edge_rng.randint(0, n - 1)
        if get_community(u) != get_community(v):
            bridge_edges.add((min(u, v), max(u, v)))
            
    return list(edge_set.union(bridge_edges))

def get_filename(gtype, nodes, folder="data"):
    return f"{folder}/{gtype}_{nodes}.txt"

def save_graph(gtype, nodes, edges, comp, edge_list, folder="data"):
    os.makedirs(folder, exist_ok=True)
    filename = get_filename(gtype, nodes, folder)
    
    with open(filename, "w") as f:
        f.write(f"# graph_type: {gtype}\n")
        f.write(f"# n_nodes: {nodes}\n")
        f.write(f"# n_edges: {edges}\n")
        f.write(f"# n_components: {comp}\n")
        
        for u, v in edge_list:
            f.write(f"{u} {v}\n")
            
    print(f"Saved: {filename} ({edges} edges)")

def main():
    print("Generating benchmark graphs...")
    folder = "data"
    os.makedirs(folder, exist_ok=True)
    
    # Star Graphs
    for n in [10000, 100000, 1000000]:
        if not os.path.exists(get_filename("star", n, folder)):
            save_graph("star", n, n-1, 1, star_graph(n), folder)
        else:
            print(f"Skipped: {get_filename('star', n, folder)} already exists.")
        
    # Chain Graphs
    for n in [10, 100, 1000]:
        if not os.path.exists(get_filename("chain", n, folder)):
            save_graph("chain", n, n-1, 1, chain_graph(n), folder)
        else:
            print(f"Skipped: {get_filename('chain', n, folder)} already exists.")

    # Multi-Component Graphs
    for n, k, m in [(100000, 10, 500000), (500000, 10, 2500000), (1000000, 10, 5000000)]:
        if not os.path.exists(get_filename("multi_comp", n, folder)):
            edges = multi_component_graph(n, k, m)
            save_graph("multi_comp", n, len(edges), k, edges, folder)
        else:
            print(f"Skipped: {get_filename('multi_comp', n, folder)} already exists.")

    # Scale-free Graphs
    for n in [100000, 500000, 1000000]:
        if not os.path.exists(get_filename("scale_free", n, folder)):
            edges = scale_free_graph(n, m0=3)
            save_graph("scale_free", n, len(edges), 1, edges, folder)
        else:
            print(f"Skipped: {get_filename('scale_free', n, folder)} already exists.")

    # Social network
    for n, k, m_in, m_out in [(100000, 10, 500000, 1000), (500000, 10, 2500000, 5000), (1000000, 10, 5000000, 10000)]:
        if not os.path.exists(get_filename("social_net", n, folder)):
            edges = social_community_graph(n, k, m_in, m_out)
            save_graph("social_net", n, len(edges), 1, edges, folder)
        else:
            print(f"Skipped: {get_filename('social_net', n, folder)} already exists.")

if __name__ == "__main__":
    main()