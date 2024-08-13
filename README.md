# KPI Calculation
Application that loads data from facts based on full or delta load and calculates OTIF KPI with full and incremental records.

## Project Structure
![image](https://github.com/user-attachments/assets/7dd10347-3b33-4307-9845-2dc5c3567a28)

## Prerequisites
1. Spark should be installed on your system and path must be set in environment variables.

2. Change the paths in config.json from src/main/resources folder as per you pc file path.

3. Change the paths in all bat files inside scripts folder as per you pc file path.

## Deployment

To start a full load of facts

```bash
  run spark submit script full_data_load.bat in scripts folder
```

To start a delta load of facts from past n days before reference date

```bash
  run spark submit script increment_data_load.bat in scripts folder
```

To calculate OTIF kpi from all data in facts

```bash
  run spark submit script otif_full_load.bat in scripts folder
```

To calculate OTIF kpi from past n days before reference date

```bash
  run spark submit script otif_delta_load.bat in scripts folder
```
