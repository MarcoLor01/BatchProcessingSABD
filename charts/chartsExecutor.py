import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

QUERY_MAPPING = {
    1: "Query_1_DF",
    2: "Query_2_DF",
    3: "Query_3_DF_Approx",
    4: "Query_3_DF_Exact",
    5: "Query_1_SQL",
    6: "Query_2_SQL",
    7: "Query_3_SQL",
    8: "Query_1_RDD",
    9: "Query_2_RDD",
    10: "Query_3_RDD"
}


QUERY_GROUPS = {
    "Query_1": [1, 5, 8],    # Query_1, Query_1_SQL, Query_1_RDD
    "Query_2": [2, 6, 9],    # Query_2, Query_2_SQL, Query_2_RDD
    "Query_3": [3, 4, 7, 10] # Query_3_Approx, Query_3_Exact, Query_3_SQL, Query_3_RDD
}

def plot_query_performance(csv_filename):

    # Leggi il CSV
    df = pd.read_csv(csv_filename)
    print(f"Caricato CSV con {len(df)} righe")

    # Mappa i numeri delle query ai nomi
    df['query_name'] = df['query'].map(QUERY_MAPPING)

    # Calcola le medie per ogni combinazione query-workers
    avg_results = df.groupby(['query', 'query_name', 'workers_number'])['tempo_totale'].mean().reset_index()

    # Configura lo stile
    plt.style.use('default')

    # Crea un grafico per ogni gruppo di query
    for group_name, query_numbers in QUERY_GROUPS.items():
        plt.figure(figsize=(10, 6))

        # Filtra i dati per questo gruppo
        group_data = avg_results[avg_results['query'].isin(query_numbers)]

        # Plotta ogni query del gruppo
        for query_num in query_numbers:
            query_data = group_data[group_data['query'] == query_num]
            if not query_data.empty:
                plt.plot(query_data['workers_number'],
                        query_data['tempo_totale'],
                        marker='o',
                        linewidth=2,
                        markersize=6,
                        label=QUERY_MAPPING[query_num])

        plt.xlabel('Numero di Workers')
        plt.ylabel('Tempo di Esecuzione (secondi)')
        plt.title(f'Performance {group_name} al variare dei Workers')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        # Salva il grafico
        filename = f"{group_name.lower()}_performance.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.show()
        print(f"Grafico salvato: {filename}")


if __name__ == "__main__":
    CSV_FILE = "../../results.csv"
    plot_query_performance(CSV_FILE)
