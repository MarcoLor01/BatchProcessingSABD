# BatchProcessingSABD
Progetto di analisi dati batch sviluppato per il corso di **Sistemi e architettura per Big Data (SABD)**, Università di Roma Tor Vergata.

## 📊 Descrizione

Il progetto ha l’obiettivo di analizzare dati storici orari relativi alla **Carbon Intensity** e alla **Carbon-Free Energy Percentage (CFE%)** in **Italia** e **Svezia** tra il 2021 e il 2024. L'intera pipeline è implementata usando strumenti Big Data:

- 🧩 Apache NiFi per l'ingestione, trasformazione e movimentazione dei dati
- 🗃️ HDFS come data store
- ⚡ Apache Spark per l'elaborazione batch
- 🧠 Redis come store a bassa latenza dei risultati
- 🐳 Docker per il deployment containerizzato

## 📁 Dataset

I dati provengono dal portale [Electricity Maps](https://www.electricitymaps.com/) e sono composti da CSV orari contenenti:

- Intensità di carbonio (`Carbon intensity gCO2eq/kWh`)
- Percentuale di energia rinnovabile (`Carbon-free energy percentage`)

## 🏗️ Architettura

```mermaid
flowchart TD
    NiFi -->|Fetch & Transform| HDFS
    HDFS -->|Read| Spark
    Spark -->|Query Output| HDFS
    HDFS -->|Export| NiFi
    NiFi -->|Push| Redis
