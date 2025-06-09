import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Mapping aggiornato con i nuovi numeri
QUERY_MAPPING = {
    1: "Query_1_DF_Parquet",
    11: "Query_1_DF_CSV",
    2: "Query_2_DF_Parquet",
    12: "Query_2_DF_CSV",
    3: "Query_3_DF_Approx_Parquet",
    13: "Query_3_DF_Approx_CSV",
    4: "Query_3_DF_Exact_Parquet",
    14: "Query_3_DF_Exact_CSV",
    5: "Query_1_SQL_Parquet",
    15: "Query_1_SQL_CSV",
    6: "Query_2_SQL_Parquet",
    16: "Query_2_SQL_CSV",
    7: "Query_3_SQL_Parquet",
    17: "Query_3_SQL_CSV",
    8: "Query_1_RDD",
    9: "Query_2_RDD",
    10: "Query_3_RDD"
}


def plot_query_performance(csv_filename):
    """
    Genera grafici per confrontare le performance delle query:
    1. Query 1: Confronto CSV vs PARQUET (DF e SQL insieme)
    2. Query 2: Confronto CSV vs PARQUET (DF e SQL insieme)
    3. Query 3: Confronto CSV vs PARQUET (DF e SQL insieme)
    4. Confronto RDD vs DF/SQL (CSV) per tutte le query
    """

    # Leggi il CSV
    df = pd.read_csv(csv_filename)
    print(f"Caricato CSV con {len(df)} righe")

    # Mappa i numeri delle query ai nomi
    df['query_name'] = df['query'].map(QUERY_MAPPING)

    # Calcola le medie per ogni combinazione query-workers
    avg_results = df.groupby(['query', 'query_name', 'workers_number'])['tempo_totale'].mean().reset_index()

    # Configura lo stile
    plt.style.use('default')

    # 1. QUERY 1 - Confronto CSV vs PARQUET (DF e SQL)
    print("Creando grafico Query 1: CSV vs PARQUET...")

    plt.figure(figsize=(12, 7))

    query_1_data = [
        (1, "DF Parquet", '#1f77b4', '-', 'o'),
        (11, "DF CSV", '#1f77b4', '--', 's'),
        (5, "SQL Parquet", '#ff7f0e', '-', 'o'),
        (15, "SQL CSV", '#ff7f0e', '--', 's')
    ]

    for query_num, label, color, linestyle, marker in query_1_data:
        query_data = avg_results[avg_results['query'] == query_num]
        if not query_data.empty:
            plt.plot(query_data['workers_number'], query_data['tempo_totale'],
                     marker=marker, linewidth=2.5, markersize=7,
                     color=color, linestyle=linestyle, label=label)

    plt.xlabel('Numero di Workers', fontsize=12)
    plt.ylabel('Tempo di Esecuzione (secondi)', fontsize=12)
    plt.title('Query 1 - Confronto CSV vs PARQUET (DF e SQL)', fontsize=14, fontweight='bold')
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    filename = "query_1_csv_vs_parquet.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.show()
    print(f"Grafico salvato: {filename}")

    # 2. QUERY 2 - Confronto CSV vs PARQUET (DF e SQL)
    print("\nCreando grafico Query 2: CSV vs PARQUET...")

    plt.figure(figsize=(12, 7))

    query_2_data = [
        (2, "DF Parquet", '#1f77b4', '-', 'o'),
        (12, "DF CSV", '#1f77b4', '--', 's'),
        (6, "SQL Parquet", '#ff7f0e', '-', 'o'),
        (16, "SQL CSV", '#ff7f0e', '--', 's')
    ]

    for query_num, label, color, linestyle, marker in query_2_data:
        query_data = avg_results[avg_results['query'] == query_num]
        if not query_data.empty:
            plt.plot(query_data['workers_number'], query_data['tempo_totale'],
                     marker=marker, linewidth=2.5, markersize=7,
                     color=color, linestyle=linestyle, label=label)

    plt.xlabel('Numero di Workers', fontsize=12)
    plt.ylabel('Tempo di Esecuzione (secondi)', fontsize=12)
    plt.title('Query 2 - Confronto CSV vs PARQUET (DF e SQL)', fontsize=14, fontweight='bold')
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    filename = "query_2_csv_vs_parquet.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.show()
    print(f"Grafico salvato: {filename}")

    # 3. QUERY 3 - Confronto CSV vs PARQUET (DF e SQL, Approx e Exact)
    print("\nCreando grafico Query 3: CSV vs PARQUET...")

    plt.figure(figsize=(12, 7))

    query_3_data = [
        (3, "DF Approx Parquet", '#1f77b4', '-', 'o'),
        (13, "DF Approx CSV", '#1f77b4', '--', 's'),
        (4, "DF Exact Parquet", '#2ca02c', '-', 'o'),
        (14, "DF Exact CSV", '#2ca02c', '--', 's'),
        (7, "SQL Parquet", '#ff7f0e', '-', 'o'),
        (17, "SQL CSV", '#ff7f0e', '--', 's')
    ]

    for query_num, label, color, linestyle, marker in query_3_data:
        query_data = avg_results[avg_results['query'] == query_num]
        if not query_data.empty:
            plt.plot(query_data['workers_number'], query_data['tempo_totale'],
                     marker=marker, linewidth=2.5, markersize=7,
                     color=color, linestyle=linestyle, label=label)

    plt.xlabel('Numero di Workers', fontsize=12)
    plt.ylabel('Tempo di Esecuzione (secondi)', fontsize=12)
    plt.title('Query 3 - Confronto CSV vs PARQUET (DF e SQL)', fontsize=14, fontweight='bold')
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    filename = "query_3_csv_vs_parquet.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.show()
    print(f"Grafico salvato: {filename}")

    # 4. QUERY 1 - Confronto RDD vs DF/SQL (CSV)
    print("\nCreando grafico Query 1: RDD vs DF/SQL...")

    plt.figure(figsize=(12, 7))

    query_1_rdd_data = [
        (8, "RDD", '#d62728', '-', 'o'),
        (11, "DF CSV", '#1f77b4', '--', 's'),
        (15, "SQL CSV", '#ff7f0e', '--', '^')
    ]

    for query_num, label, color, linestyle, marker in query_1_rdd_data:
        query_data = avg_results[avg_results['query'] == query_num]
        if not query_data.empty:
            plt.plot(query_data['workers_number'], query_data['tempo_totale'],
                     marker=marker, linewidth=2.5, markersize=7,
                     color=color, linestyle=linestyle, label=label)

    plt.xlabel('Numero di Workers', fontsize=12)
    plt.ylabel('Tempo di Esecuzione (secondi)', fontsize=12)
    plt.title('Query 1 - Confronto RDD vs DF/SQL (CSV)', fontsize=14, fontweight='bold')
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    filename = "query_1_rdd_vs_df_sql.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.show()
    print(f"Grafico salvato: {filename}")

    # 5. QUERY 2 - Confronto RDD vs DF/SQL (CSV)
    print("\nCreando grafico Query 2: RDD vs DF/SQL...")

    plt.figure(figsize=(12, 7))

    query_2_rdd_data = [
        (9, "RDD", '#d62728', '-', 'o'),
        (12, "DF CSV", '#1f77b4', '--', 's'),
        (16, "SQL CSV", '#ff7f0e', '--', '^')
    ]

    for query_num, label, color, linestyle, marker in query_2_rdd_data:
        query_data = avg_results[avg_results['query'] == query_num]
        if not query_data.empty:
            plt.plot(query_data['workers_number'], query_data['tempo_totale'],
                     marker=marker, linewidth=2.5, markersize=7,
                     color=color, linestyle=linestyle, label=label)

    plt.xlabel('Numero di Workers', fontsize=12)
    plt.ylabel('Tempo di Esecuzione (secondi)', fontsize=12)
    plt.title('Query 2 - Confronto RDD vs DF/SQL (CSV)', fontsize=14, fontweight='bold')
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    filename = "query_2_rdd_vs_df_sql.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.show()
    print(f"Grafico salvato: {filename}")

    # 6. QUERY 3 - Confronto RDD vs DF/SQL (CSV)
    print("\nCreando grafico Query 3: RDD vs DF/SQL...")

    plt.figure(figsize=(12, 7))

    query_3_rdd_data = [
        (10, "RDD", '#d62728', '-', 'o'),
        (13, "DF Approx CSV", '#1f77b4', '--', 's'),
        (14, "DF Exact CSV", '#2ca02c', '-.', 'D'),
        (17, "SQL CSV", '#ff7f0e', '--', '^')
    ]

    for query_num, label, color, linestyle, marker in query_3_rdd_data:
        query_data = avg_results[avg_results['query'] == query_num]
        if not query_data.empty:
            plt.plot(query_data['workers_number'], query_data['tempo_totale'],
                     marker=marker, linewidth=2.5, markersize=7,
                     color=color, linestyle=linestyle, label=label)

    plt.xlabel('Numero di Workers', fontsize=12)
    plt.ylabel('Tempo di Esecuzione (secondi)', fontsize=12)
    plt.title('Query 3 - Confronto RDD vs DF/SQL (CSV)', fontsize=14, fontweight='bold')
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    filename = "query_3_rdd_vs_df_sql.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.show()
    print(f"Grafico salvato: {filename}")


def print_summary_stats(csv_filename):
    """
    Stampa statistiche riassuntive per analisi rapida
    """
    df = pd.read_csv(csv_filename)
    df['query_name'] = df['query'].map(QUERY_MAPPING)

    print("\n" + "=" * 60)
    print("STATISTICHE RIASSUNTIVE")
    print("=" * 60)

    # Confronto medio CSV vs PARQUET
    parquet_queries = [1, 2, 3, 4, 5, 6, 7]
    csv_queries = [11, 12, 13, 14, 15, 16, 17]

    parquet_avg = df[df['query'].isin(parquet_queries)]['tempo_totale'].mean()
    csv_avg = df[df['query'].isin(csv_queries)]['tempo_totale'].mean()

    print(f"Tempo medio PARQUET: {parquet_avg:.2f}s")
    print(f"Tempo medio CSV: {csv_avg:.2f}s")
    print(f"Differenza percentuale: {((csv_avg - parquet_avg) / parquet_avg * 100):+.1f}%")

    # Confronto DF vs SQL vs RDD
    df_queries = [1, 2, 3, 4, 11, 12, 13, 14]
    sql_queries = [5, 6, 7, 15, 16, 17]
    rdd_queries = [8, 9, 10]

    df_avg = df[df['query'].isin(df_queries)]['tempo_totale'].mean()
    sql_avg = df[df['query'].isin(sql_queries)]['tempo_totale'].mean()
    rdd_avg = df[df['query'].isin(rdd_queries)]['tempo_totale'].mean()

    print(f"\nTempo medio DataFrame: {df_avg:.2f}s")
    print(f"Tempo medio SQL: {sql_avg:.2f}s")
    print(f"Tempo medio RDD: {rdd_avg:.2f}s")


if __name__ == "__main__":
    CSV_FILE = "../benchmark/execution_time.csv"
    plot_query_performance(CSV_FILE)
    print_summary_stats(CSV_FILE)