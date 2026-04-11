import os
import random
import argparse
import urllib.request
import gzip
import yaml

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

def diameter_graph(n: int, m: int, d: int, seed: int = 42) -> list:
    """
    Fast O(N+M) generator for a random connected graph with exactly n nodes, m edges, and diameter d.
    Uses a 'Layered Band' topology to ensure maximum randomness without O(N^2) checks.
    """
    if d >= n:
        raise ValueError(f"Diameter {d} must be strictly less than the number of nodes {n}.")
    if m < n - 1:
        raise ValueError(f"Number of edges {m} must be at least n - 1 for the graph to be connected.")

    if d == 1:
        return random_graph(n, min(m, n * (n - 1) // 2), seed)

    edge_rng = random.Random(seed)
    edges = set()
    
    # Track which nodes belong to which coordinate layer
    layers = {i: [] for i in range(d + 1)}
    node_to_layer = {}

    # 1. The Backbone
    for i in range(d + 1):
        layers[i].append(i)
        node_to_layer[i] = i
        if i > 0:
            edges.add((i - 1, i))

    # 2. Distribute remaining nodes randomly across internal layers
    for v in range(d + 1, n):
        L = edge_rng.randint(1, d - 1)
        layers[L].append(v)
        node_to_layer[v] = L
        # Connect to the backbone node of that layer to guarantee connectivity
        # distance to backbone is exactly 1, preserving max diameter mathematically.
        edges.add((L, v)) 

    # 3. Fast Random Edge Addition (Direct Sampling)
    nodes_list = list(range(n))
    attempts = 0
    max_attempts = (m - len(edges)) * 10 # Safety valve against infinite loops
    
    while len(edges) < m and attempts < max_attempts:
        attempts += 1
        
        # Pick a random source node
        u = edge_rng.choice(nodes_list)
        L_u = node_to_layer[u]
        
        # Pick a random valid target layer (L-1, L, or L+1)
        valid_target_layers = [L_u]
        if L_u > 0: valid_target_layers.append(L_u - 1)
        if L_u < d: valid_target_layers.append(L_u + 1)
        
        L_v = edge_rng.choice(valid_target_layers)
        
        # Pick a random node in that target layer
        v = edge_rng.choice(layers[L_v])
        
        if u != v:
            edge = (min(u, v), max(u, v))
            if edge not in edges:
                edges.add(edge)
                attempts = 0 # Reset safety valve on success

    if len(edges) < m:
        print(f"Warning: Reached local maximum density for this topology. Stopped at {len(edges)} edges.")
        
    return list(edges)

def get_filename(gtype, folder="data", **kwargs):
    parts = [gtype]
    
    # Sort keys to ensure consistent naming, but force 'n' to be the first parameter
    keys = list(kwargs.keys())
    if 'n' in keys:
        keys.remove('n')
        keys = ['n'] + sorted(keys)
    else:
        keys = sorted(keys)
        
    for k in keys:
        parts.append(f"{k}{kwargs[k]}")
        
    return os.path.join(folder, "_".join(parts) + ".txt")

def save_graph(filepath, gtype, nodes, edges, comp, edge_list, diameter=None):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, "w") as f:
        f.write(f"# graph_type: {gtype}\n")
        f.write(f"# n_nodes: {nodes}\n")
        f.write(f"# n_edges: {edges}\n")
        f.write(f"# n_components: {comp}\n")
        
        if diameter is not None:
            f.write(f"# diameter: {diameter}\n")
            
        for u, v in edge_list:
            f.write(f"{u} {v}\n")
            
    print(f"Saved: {filepath} ({edges} edges)")

def fetch_web_google_graph(folder="data"):
    url = "https://snap.stanford.edu/data/web-Google.txt.gz"
    gz_path = os.path.join(folder, "web-Google.txt.gz")
    
    expected_nodes = 875713 
    final_txt_path = get_filename("web_google", folder, n=expected_nodes)

    if os.path.exists(final_txt_path):
        print(f"Skipped: {final_txt_path} already exists.")
        return

    os.makedirs(folder, exist_ok=True)
    print(f"Downloading {url} (this might take a moment)...")
    urllib.request.urlretrieve(url, gz_path)

    print("Extracting and parsing graph...")
    edges = []
    unique_nodes = set()
    
    with gzip.open(gz_path, 'rt') as f:
        for line in f:
            if not line.startswith('#'):
                u, v = map(int, line.strip().split())
                edges.append((u, v))
                unique_nodes.add(u)
                unique_nodes.add(v)

    n_nodes = len(unique_nodes)
    n_edges = len(edges)
    save_graph(final_txt_path, "web_google", n_nodes, n_edges, 4336, edges)
    os.remove(gz_path)


def generate(config):
    folder = os.path.join("data", config.get("data_path", "data_example"))
    graphs_config = config.get("graphs", {})

    print(f"Generating benchmark graphs into: {folder} ...\n")

    for gtype, params in graphs_config.items():
        if gtype == "web_google":
            fetch_web_google_graph(folder)
            continue
            
        if not params:
            continue

        keys = list(params.keys())
        
        for k in keys:
            if not isinstance(params[k], list):
                params[k] = [params[k]]
                
        num_runs = len(params[keys[0]])

        for i in range(num_runs):
            # Extract the arguments for this specific run
            kwargs = {k: params[k][i] for k in keys}
            n = kwargs["n"]
            
            # Dynamically generate the filename using all parameters
            filepath = get_filename(gtype, folder, **kwargs)
            
            if os.path.exists(filepath):
                print(f"Skipped: {filepath} already exists.")
                continue

            print(f"Generating {gtype} graph (n={n})...")
            
            if gtype == "star":
                save_graph(filepath, gtype, n, n-1, 1, star_graph(n))
            elif gtype == "chain":
                save_graph(filepath, gtype, n, n-1, 1, chain_graph(n))
            elif gtype == "multi_comp":
                edges = multi_component_graph(n, kwargs["k"], kwargs["m"])
                save_graph(filepath, gtype, n, len(edges), kwargs["k"], edges)
            elif gtype == "scale_free":
                edges = scale_free_graph(n, kwargs["m0"])
                save_graph(filepath, gtype, n, len(edges), 1, edges)
            elif gtype == "social_net":
                edges = social_community_graph(n, kwargs["k"], kwargs["m_internal"], kwargs["m_external"])
                save_graph(filepath, gtype, n, len(edges), 1, edges)
            elif gtype == "fixed_diameter":
                edges = diameter_graph(n, kwargs["m"], kwargs["d"])
                save_graph(filepath, gtype, n, len(edges), 1, edges, diameter=kwargs["d"])
            else:
                print(f"Warning: Unknown graph type '{gtype}'")

def generate_from_config(config_path):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    generate(config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Graph Generator Pipeline")
    parser.add_argument("--config", "-c", type=str, default="topologies.yaml", help="Path to YAML configuration file")
    args = parser.parse_args()
    config_path = os.path.join("configs", args.config)
    if os.path.exists(config_path):
        generate_from_config(config_path)
    else:
        print(f"Error: Configuration file '{config_path}' not found.")