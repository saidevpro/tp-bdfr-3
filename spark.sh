python .\seeders\transactions.py .\data\transactions_data.csv --situation-date 2025-05-22 --percentage 70 --batch-size 10000 --clear
spark-submit --master yarn --deploy-mode client --driver-memory 3g --executor-memory 3g --jars /app/bin/mysql-connector-j-9.2.0.jar /app/jobs/get_transactions_from_bd.py
python .\seeders\transactions.py .\data\transactions_data.csv --situation-date 2025-05-23 --percentage 100 --batch-size 10000 --clear
spark-submit --master yarn --deploy-mode client --driver-memory 3g --executor-memory 3g --jars /app/bin/mysql-connector-j-9.2.0.jar /app/jobs/get_transactions_from_bd.py

docker exec -it spark-master bash

spark-submit --master yarn --deploy-mode client --driver-memory 4g --executor-memory 4g  /app/jobs/load_files_to_hdfs.py

spark-submit \
  --deploy-mode client \
  --driver-memory 4g \
  --executor-memory 7g \
  --conf spark.executor.memoryOverhead=4000 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.instances=5 \
  --conf spark.sql.files.maxPartitionBytes=64m \
  --conf spark.sql.shuffle.partitions=200 \
  /app/jobs/load_fraud_labels_to_hdfs.py
