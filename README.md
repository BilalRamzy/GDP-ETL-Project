# Countries by GDP – ETL Project

## About the Project
This is a simple **ETL (Extract, Transform, Load)** project written in Python.

The script collects GDP data of countries from Wikipedia, cleans and processes it, then saves it into a CSV file and an SQLite database.

---

## What the Project Does

- **Extract**
  - Scrapes country GDP data from Wikipedia using BeautifulSoup

- **Transform**
  - Converts GDP from **million USD to billion USD**
  - Rounds values to 2 decimal places

- **Load**
  - Saves the data into:
    - A CSV file
    - An SQLite database

- **Query**
  - Displays countries with GDP greater than **100 Billion USD**

- **Logging**
  - Logs each step of the ETL process with timestamps

---

## Tools & Libraries Used

- Python
- requests
- BeautifulSoup (bs4)
- pandas
- sqlite3

---

## Project Files
