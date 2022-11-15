First set up the spark containers 
run these commands first to set up the containers

docker run --name spark-master -h spark-master -e ENABLE_INIT_DAEMON=false -d bde2020/spark-master:3.0.0-hadoop3.2-java11
docker run --name spark-worker-1 --link spark-master:spark-master -e ENABLE_INIT_DAEMON=false -d bde2020/spark-worker:3.0.0-hadoop3.2-java11

Inside spark master CLI, execute the commands from "spark.txt" in order