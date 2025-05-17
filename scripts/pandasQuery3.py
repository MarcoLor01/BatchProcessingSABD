import pandas as pd
import numpy as np
import logging

# Configura il logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Percorsi
CSV_PATH = "../../../mnt/d/Download/ElectricityMerge.csv"

# 1) Lettura del CSV
df = pd.read_csv(CSV_PATH, parse_dates=["Datetime (UTC)"])
logger.info(f"Dati caricati: {df.shape[0]} righe, {df.shape[1]} colonne")
print("Colonne disponibili:", df.columns.tolist())

# 2) Estrazione dell'ora
df['hour'] = df['Datetime (UTC)'].dt.hour

# Rinominare le colonne per uniformità
df = df.rename(columns={
    'carbon_intensity': 'carbon_intensity',
    'cfe': 'cfe_percentage'
})

# 3) Calcolo della media oraria per ciascun Paese
intermediate = (
    df.groupby(['Country', 'hour'])
      .agg(
          avg_hour_carbon_intensity=('carbon_intensity', 'mean'),
          avg_hour_cfe_percentage=('cfe_percentage', 'mean')
      )
      .reset_index()
)
logger.info(f"Intermedio generato: {intermediate.shape[0]} righe")

# Stampa l'aggregazione oraria intermedia
print("Aggregazione oraria intermedia:")
print(intermediate.head(48).to_string(index=False))

# 4) Funzione per calcolare percentili
def compute_percentiles(group, column):
    values = group[column].values
    percentili = np.quantile(values, [0.0, 0.25, 0.5, 0.75, 1.0])
    return pd.Series({
        'min': percentili[0],
        'perc_25': percentili[1],
        'perc_50': percentili[2],
        'perc_75': percentili[3],
        'max': percentili[4]
    })

# 5) Calcolo percentili per carbon_intensity
ci_stats = (
    intermediate.groupby('Country')
    .apply(lambda g: compute_percentiles(g, 'avg_hour_carbon_intensity'))
    .reset_index()
)
ci_stats['data'] = 'carbon-intensity'

# 6) Calcolo percentili per cfe_percentage
cfe_stats = (
    intermediate.groupby('Country')
    .apply(lambda g: compute_percentiles(g, 'avg_hour_cfe_percentage'))
    .reset_index()
)
cfe_stats['data'] = 'cfe'

# 7) Unione dei risultati finali
final = pd.concat([
    ci_stats[['Country', 'data', 'min', 'perc_25', 'perc_50', 'perc_75', 'max']],
    cfe_stats[['Country', 'data', 'min', 'perc_25', 'perc_50', 'perc_75', 'max']]
], ignore_index=True)

# 8) Ordinamento e stampa
table = final.sort_values(['Country', 'data'])
print("Risultati percentili:")
print(table.to_string(index=False))

# Facoltativo: salva su CSV
# output_path = "../mnt/d/Download/query3_pandas_result.csv"
# table.to_csv(output_path, index=False)
# logger.info(f"Risultati salvati in {output_path}")
