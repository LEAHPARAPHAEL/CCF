import pandas as pd
import matplotlib.pyplot as plt
import os

py_path = os.path.join("results", "csv", "benchmark_pyspark.csv")
sc_path = os.path.join("results", "csv", "benchmark_scala.csv")

df_py = pd.read_csv(py_path)
df_sc = pd.read_csv(sc_path)

graph_types = pd.concat([df_py['graph_type'], df_sc['graph_type']]).unique()
print("Generating plots...")
for g_type in graph_types:
    plt.figure(figsize=(8, 5))
    
    subset_g_py = df_py[df_py['graph_type'] == g_type].sort_values(by='n_nodes')
    subset_g_sc = df_sc[df_sc['graph_type'] == g_type].sort_values(by='n_nodes')
    
    algos = set(subset_g_py['algo'].unique()).union(set(subset_g_sc['algo'].unique()))
    
    for algo in algos:
        subset_algo_py = subset_g_py[subset_g_py['algo'] == algo]
        subset_algo_sc = subset_g_sc[subset_g_sc['algo'] == algo]
        
        algo_color = None
        if not subset_algo_sc.empty:
            line = plt.plot(subset_algo_sc['n_nodes'], subset_algo_sc['mean_time_s'], 
                            marker='o', linestyle='-', label=f'{algo} (Scala)')
            algo_color = line[0].get_color()
            
        if not subset_algo_py.empty:
            if algo_color:
                plt.plot(subset_algo_py['n_nodes'], subset_algo_py['mean_time_s'], 
                         marker='X', linestyle='--', color=algo_color, label=f'{algo} (PySpark)')
            else:
                plt.plot(subset_algo_py['n_nodes'], subset_algo_py['mean_time_s'], 
                         marker='X', linestyle='--', label=f'{algo} (PySpark)')
    
    plt.title(f'Algorithm Performance on {g_type.replace("_", " ").title()} Graphs')
    plt.xlabel('Number of Nodes ($n$)')
    plt.ylabel('Mean Wall Time (s)')
    
    plt.legend(title='Algorithm (Engine)', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    
    filename = os.path.join("results", "plots", f"{g_type}_comparison.png")
    plt.savefig(filename)
    plt.close()
    print(f" -> Saved {filename}")


def generate_latex_table(df, caption, label):
    pivot_df = df.groupby(['graph_type', 'n_nodes', 'n_edges', 'n_components', 'algo'])['mean_time_s'].mean().unstack('algo')
    pivot_df = pivot_df.fillna(-1)

    algos = list(pivot_df.columns)
    n_cols = 4 + len(algos)

    latex_lines = []
    latex_lines.append("\\begin{table}[H]")
    latex_lines.append(f"    \\caption{{{caption}}}")
    latex_lines.append(f"    \\label{{{label}}}")
    latex_lines.append("    \\resizebox{0.45\\textwidth}{!}{%}")

    col_format = "llll" + "r" * len(algos)
    latex_lines.append(f"    \\begin{{tabular}}{{{col_format}}}")
    latex_lines.append("    \\toprule")

    header_algos = [a.replace('_', '\\_') for a in algos]
    latex_lines.append(f"    Graph & Nodes & Edges & $k$ & {' & '.join(header_algos)} \\\\")
    latex_lines.append("    \\midrule")

    for g_type, group in pivot_df.groupby(level='graph_type'):
        n_rows = len(group)
        
        for i, (idx, row) in enumerate(group.iterrows()):
            _, n_nodes, n_edges, n_components = idx
            
            valid_times = [val for val in row if val > 0]
            min_time = min(valid_times) if valid_times else None
            
            formatted_vals = []
            for val in row:
                if val <= 0:
                    formatted_vals.append("-1")
                elif min_time is not None and val == min_time:
                    formatted_vals.append(f"\\textbf{{{val:.2f}}}")
                else:
                    formatted_vals.append(f"{val:.2f}")
                    
            vals_str = " & ".join(formatted_vals)
            
            escaped_g_type = str(g_type).replace('_', '\\_')
            if n_rows == 1:
                g_str = escaped_g_type
            else:
                if i == 0:
                    g_str = f"\\multirow[t]{{{n_rows}}}{{*}}{{{escaped_g_type}}}"
                else:
                    g_str = ""
            
            latex_lines.append(f"    {g_str} & {n_nodes} & {n_edges} & {n_components} & {vals_str} \\\\")
        
        latex_lines.append(f"    \\cline{{1-{n_cols}}}")

    latex_lines.append("    \\bottomrule")
    latex_lines.append("    \\end{tabular}%")
    latex_lines.append("}")
    latex_lines.append("\\end{table}")

    return "\n".join(latex_lines)


print("\n--- Generated LaTeX Table: PySpark ---\n")
print(generate_latex_table(
    df_py, 
    "Mean wall time (s) of PySpark evaluated across various graph types and sizes.", 
    "tab:benchmark_pyspark"
))

print("\n--- Generated LaTeX Table: Scala ---\n")
print(generate_latex_table(
    df_sc, 
    "Mean wall time (s) of Scala evaluated across various graph types and sizes.", 
    "tab:benchmark_scala"
))