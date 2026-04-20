# Steam Explorer CS 457 Final Project

## Setup

### 1. Install Dependencies
```bash
pip install psycopg2-binary pandas streamlit plotly
```
### 2. Create Database
```bash
createdb steam_explorer
psql steam_explorer < db/schema.sql
```
### 3. Download the dataset
Download **'steam.csv'** from:
https://www.kaggle.com/datasets/nikdavis/steam-store-games

Place it in the **'etl/'** folder

### 4. Run the ETL
```bash
cd etl
python etl.py --csv steam.csv
```
### 5. Launch the Dashboard
```bash
cd app
streamlit run app.py
```
