import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

URL = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
CSV_FILE = "Countries_by_GDP.csv"
DB_FILE = "World_Economies.db"
TABLE_NAME = "Countries_by_GDP"
LOG_FILE = "etl_project_log.txt"


def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as file:
        file.write(f"{timestamp} : {message}\n")


def extract_gdp_data(url):
    log_message("Starting data extraction")

    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", {"class": "wikitable"})
    rows = table.find_all("tr")

    countries = []
    gdps = []

    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) >= 3:
            country = cols[0].text.strip()
            gdp_text = cols[2].text.strip().replace(",", "")

            if gdp_text.isdigit():
                countries.append(country)
                gdps.append(float(gdp_text))

    df = pd.DataFrame({
        "Country": countries,
        "GDP_USD_million": gdps
    })

    log_message("Data extraction completed")
    return df


def transform_data(df):
    log_message("Starting data transformation")

    df["GDP_USD_billion"] = (df["GDP_USD_million"] / 1000).round(2)
    df = df[["Country", "GDP_USD_billion"]]

    log_message("Data transformation completed")
    return df


def load_data(df):
    log_message("Starting data load")

    df.to_csv(CSV_FILE, index=False)

    conn = sqlite3.connect(DB_FILE)
    df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)

    log_message("Data loaded into CSV and database")
    conn.close()


def run_query():
    log_message("Running database query")

    conn = sqlite3.connect(DB_FILE)

    query = f"""
    SELECT *
    FROM {TABLE_NAME}
    WHERE GDP_USD_billion > 100
    """

    result = pd.read_sql(query, conn)
    conn.close()

    print("\nCountries with GDP > 100 Billion USD:\n")
    print(result)

    log_message("Database query completed")


def main():
    log_message("ETL process started")

    df_extracted = extract_gdp_data(URL)
    df_transformed = transform_data(df_extracted)
    load_data(df_transformed)
    run_query()

    log_message("ETL process completed successfully")


if __name__ == "__main__":
    main()