# STEPS TO RUN THE TEST
## Influxdb
Download the dockerized InfluxDB
### Command to run the container
$ sudo docker run --rm -e INFLUXDB_HTTP_AUTH_ENABLED=true -e INFLUXDB_HTTP_BIND_ADDRESS=":8086" -e INFLUXDB_ADMIN_USER=admin -e INFLUXDB_ADMIN_PASSWORD=admin -p 8086:8086 -v (HOME)/influxdb/:/var/lib/influxdb influxdb

### Install local flink
## Start the cluster
$ (PATH_FLINK_INSTALLATION)/flink-1.12.1/bin$ ./start-cluster.sh
### Build the JAR containing the Job (add the artifact to create the jar with the dependencies)
(PATH_TO_THE_PROJECT)/out/artifacts/sensors_anomaly_detection_jar/sensors-anomaly-detection.jar
## Run the Job
$ flink run -c io.intellisense.testproject.eng.jobs.AnomalyDetectionJob out/artifacts/sensors_anomaly_detection_jar/sensors-anomaly-detection.jar --configFile config.local.yaml --sensorData ./src/main/resources/TestFile.csv
