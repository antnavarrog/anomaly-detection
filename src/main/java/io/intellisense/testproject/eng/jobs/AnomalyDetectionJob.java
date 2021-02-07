package io.intellisense.testproject.eng.jobs;

import io.intellisense.testproject.eng.datasource.CsvDatasource;
import io.intellisense.testproject.eng.function.AnomalyDetectionFunction;
import io.intellisense.testproject.eng.model.RowWithScore;
import io.intellisense.testproject.eng.sink.influxdb.InfluxDBSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.time.Instant;
import java.util.List;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

@Slf4j
public class AnomalyDetectionJob {

    public static void main(String[] args) throws Exception {

        // Pass configFile location as a program argument:
        // --configFile config.local.yaml
        final ParameterTool programArgs = ParameterTool.fromArgs(args);
        final String configFile = programArgs.getRequired("configFile");
        final InputStream resourceStream = AnomalyDetectionJob.class.getClassLoader().getResourceAsStream(configFile);
        final ParameterTool configProperties = ParameterTool.fromPropertiesFile(resourceStream);

        // Stream execution environment
        // ...you can add here whatever you consider necessary for robustness
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(configProperties.getInt("flink.parallelism", 1));
        env.getConfig().setGlobalJobParameters(configProperties);

        // Simple CSV-table datasource
        final String dataset = programArgs.get("sensorData", "sensor-data.csv");
        final CsvTableSource csvDataSource = CsvDatasource.of(dataset).getCsvSource();
        final WatermarkStrategy<Row> watermarkStrategy = WatermarkStrategy.<Row>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> timestampExtract(event));
        final DataStream<Row> sourceStream = csvDataSource.getDataStream(env)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("datasource-operator");
        sourceStream.print();

        final DataStream<RowWithScore> rowWithScoreDataStream = sourceStream
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .windowAll(SlidingEventTimeWindows.of(seconds(2), seconds(1)))
                .process(new AnomalyDetectionFunction())
                .flatMap(new FlatMapFunction<List<RowWithScore>, RowWithScore>() {
                    @Override
                    public void flatMap(List<RowWithScore> value, Collector<RowWithScore> out) throws Exception {
                        value.forEach(out::collect);
                    }
                })
                .name("processing-operator");

        rowWithScoreDataStream.print();
        rowWithScoreDataStream.addSink(new InfluxDBSink(configProperties)).name("sink-operator");
        rowWithScoreDataStream.print();

        final JobExecutionResult jobResult = env.execute("Anomaly Detection Job");
    }

    private static long timestampExtract(Row event) {
        final String timestampField = (String) event.getField(0);
        if (timestampField != null) {
            return Instant.parse(timestampField).toEpochMilli();
        }
        return -1;
    }
}
