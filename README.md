```
# ECDC Data Analysis with Hadoop MapReduce

This project implements a **two-stage MapReduce workflow** in Java to process epidemiological data (inspired by the **European Centre for Disease Prevention and Control – ECDC** dataset).  

It computes:
1. The **average number of cases per country** (First MapReduce job).
2. The **number of days each country exceeded its own average** (Second MapReduce job).

---

## ⚙️ Features
- Reads raw CSV data with epidemiological case counts.
- Skips header rows automatically.
- First MapReduce job:
  - Aggregates daily case counts per country.
  - Computes the average number of cases per country.
- Second MapReduce job:
  - Uses **DistributedCache** to load the averages.
  - Counts how many times a country’s daily cases exceeded its own average.
- Outputs clean results per country.

---
```
## 🏗️ Project Structure
src/
└── ecdc/
└── ecdc.java # Main class containing driver, mappers, and reducers

```

---

## 🚀 How It Works
### First MapReduce Job
- **Mapper**: Emits `(country, cases)` pairs.
- **Reducer**: Computes average cases per country and outputs `(country, average)`.

### Second MapReduce Job
- **Mapper**:
  - Reads the averages from the DistributedCache.
  - Emits `(country, 1)` when a country’s daily cases exceed its average.
- **Reducer**: Aggregates counts per country.

---

## ▶️ Running the Program
### Prerequisites
- Java 8+
- Hadoop installed and configured
- Input dataset in CSV format stored in HDFS

### Command
```bash
hadoop jar ecdc.jar ecdc \
    /input/path /output1/path /output2/path

```
- /input/path → Path to CSV dataset in HDFS

- /output1/path → Output of first MapReduce job (average cases per country)

- /output2/path → Final output of second MapReduce job (days exceeding average)

📂 Example Input (CSV)
```
dateRep,day,month,year,cases,deaths,countriesAndTerritories,...
01/01/2020,1,1,2020,10,0,Greece,...
02/01/2020,2,1,2020,20,0,Greece,...
03/01/2020,3,1,2020,5,0,Greece,...

```
📊 Example Output
After First Job (Averages)
```
Greece    11.66
Italy     25.30
Spain     18.75
```
After Second Job (Days Exceeding Average)
```
Greece    1
Italy     5
Spain     3

```

📝 Notes

- DistributedCache is used to pass the output of the first job into the second.

- /output1/path must exist before second job starts, since it is added to the cache.

- /output1/path and /output2/path must not already exist before execution (Hadoop requirement).

- You can adjust logic to work with different CSV schemas if needed.
