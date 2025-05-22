"""
stream_csv_to_mariadb.py

Reads a large CSV file row-by-row and inserts into a MariaDB table
in configurable batch sizes to avoid excessive memory use. Supports
specifying situation_date and sampling a percentage of rows. Clears
the target table before inserting.
"""

import sys
import csv
import random
import argparse
import mysql.connector as mariadb
from mysql.connector import errorcode

def read_csv_generator(filepath):
  """Yields the header first, then each data row as a list."""
  f = open(filepath, newline='', encoding='utf-8')
  reader = csv.reader(f)
  header = next(reader)
  yield header
  for row in reader:
    yield row
  f.close()

def connect_mariadb(host, user, password, database, port=3306):
  """Open a connection to MariaDB."""
  try:
    conn = mariadb.connect(
      host=host,
      port=port,
      user=user,
      password=password,
      database=database,
      autocommit=False    # we’ll commit manually
    )
    return conn
  except mariadb.Error as e:
    print(f"Error connecting to MariaDB: {e}")
    sys.exit(1)

def ensure_table(conn, table_name):
  """Create the table if it doesn't exist, with the given columns."""
  ddl = f"""
  CREATE TABLE IF NOT EXISTS `{table_name}` (
    `id` BIGINT,
    `date` DATE,
    `client_id` INT,
    `card_id` INT,
    `amount` VARCHAR(25),
    `use_chip` VARCHAR(255),
    `merchant_id` INT,
    `merchant_city` VARCHAR(255),
    `merchant_state` VARCHAR(255),
    `zip` VARCHAR(10),
    `mcc` INT,
    `errors` TEXT,
    `situation_date` DATE NOT NULL DEFAULT (CURRENT_DATE)
  )
  """
  cursor = conn.cursor()
  cursor.execute(ddl)
  conn.commit()
  cursor.close()

def clear_table(conn, table_name):
  """Truncate the target table to clear existing data."""
  cursor = conn.cursor()
  cursor.execute(f"TRUNCATE TABLE `{table_name}`")
  conn.commit()
  cursor.close()

def stream_insert(filepath, conn, table_name, batch_size=1000, situation_date=None, percentage=100):
  """
  Read CSV via generator, capture header, and insert rows in batches.
  Optionally sample rows and set situation_date.
  """
  gen = read_csv_generator(filepath)
  header = next(gen)

  # base schema columns
  base_cols = [
    'id', 'date', 'client_id', 'card_id', 'amount',
    'use_chip', 'merchant_id', 'merchant_city',
    'merchant_state', 'zip', 'mcc', 'errors'
  ]

  # include situation_date if provided
  if situation_date:
    cols = base_cols + ['situation_date']
  else:
    cols = base_cols

  columns = ",".join(f"`{col}`" for col in cols)
  placeholders = ",".join(["%s"] * len(cols))
  insert_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

  cursor = conn.cursor()
  batch = []
  row_count = 0
  sample_fraction = percentage / 100.0

  for row in gen:
    # sampling
    if percentage < 100 and random.random() > sample_fraction:
      continue

    # append situation_date if provided
    row_data = row + [situation_date] if situation_date else row

    batch.append(row_data)
    if len(batch) >= batch_size:
      cursor.executemany(insert_sql, batch)
      conn.commit()
      row_count += len(batch)
      print(f"Inserted {row_count} rows...", end="\r")
      batch.clear()

  if batch:
    cursor.executemany(insert_sql, batch)
    conn.commit()
    row_count += len(batch)
    print(f"Inserted {row_count} rows.", end="\r")

  cursor.close()
  print("\nDone.")

def parse_args():
  parser = argparse.ArgumentParser(
    description="Stream CSV to MariaDB with optional date, sampling, and clearing."
  )
  parser.add_argument(
    'csv_path',
    help="Path to the CSV file"
  )
  parser.add_argument(
    '--situation-date',
    dest='situation_date',
    help="Date to set for situation_date (YYYY-MM-DD). Defaults to current date.",
    default=None
  )
  parser.add_argument(
    '--percentage',
    type=int,
    default=100,
    help="Percentage of rows to insert (1-100). Default is 100."
  )
  parser.add_argument(
    '--batch-size',
    type=int,
    default=2000,
    help="Number of rows per batch insert."
  )
  parser.add_argument(
    '--clear',
    action='store_true',
    help="Clear the target table before inserting data."
  )
  return parser.parse_args()

if __name__ == "__main__":
  args = parse_args()
  csv_path = args.csv_path
  situation_date = args.situation_date
  percentage = args.percentage
  batch_size = args.batch_size
  do_clear = args.clear

  if not (0 < percentage <= 100):
    print("Error: percentage must be between 1 and 100")
    sys.exit(1)

  # --- CONFIGURE THESE ---
  DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "bdd",
    "port": 3306,
  }
  TARGET_TABLE = "transactions"

  conn = connect_mariadb(**DB_CONFIG)
  try:
    ensure_table(conn, TARGET_TABLE)
    if do_clear:
      clear_table(conn, TARGET_TABLE)
    stream_insert(
      csv_path, conn, TARGET_TABLE,
      batch_size=batch_size,
      situation_date=situation_date,
      percentage=percentage
    )
  finally:
    conn.close()
