python .\seeders\transactions.py .\data\transactions_data.csv --situation-date 2025-05-22 --percentage 70 --batch-size 10000 --clear
spark-submit --master yarn --deploy-mode client --driver-memory 3g --executor-memory 3g --jars /app/bin/mysql-connector-j-9.2.0.jar /app/jobs/get_transactions_from_bd.py
python .\seeders\transactions.py .\data\transactions_data.csv --situation-date 2025-05-23 --percentage 100 --batch-size 10000 --clear
spark-submit --master yarn --deploy-mode client --driver-memory 3g --executor-memory 3g --jars /app/bin/mysql-connector-j-9.2.0.jar /app/jobs/get_transactions_from_bd.py




# ------------------------
spark-submit --driver-memory 4g --executor-memory 7g  /app/jobs/hive/load_transactions_to_hive.py
spark-submit --driver-memory 3g --executor-memory 7g --jars /app/bin/mysql-connector-j-9.2.0.jar /app/jobs/hdfs/load_transactions_to_hdfs.py
