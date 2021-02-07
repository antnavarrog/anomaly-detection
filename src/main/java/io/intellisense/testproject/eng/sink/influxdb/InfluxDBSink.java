package io.intellisense.testproject.eng.sink.influxdb;

import io.intellisense.testproject.eng.model.RowWithScore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
public class InfluxDBSink extends RichSinkFunction<RowWithScore> {

    transient InfluxDB influxDB;

    final ParameterTool configProperties;

    // cluster metrics
    final Accumulator recordsIn = new IntCounter(0);
    final Accumulator recordsOut = new IntCounter(0);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator(getClass().getSimpleName() + "-recordsIn", recordsIn);
        getRuntimeContext().addAccumulator(getClass().getSimpleName() + "-recordsOut", recordsOut);
        influxDB = InfluxDBFactory.connect(
                configProperties.getRequired("influxdb.url"),
                configProperties.getRequired("influxdb.username"),
                configProperties.getRequired("influxdb.password"));
        final String dbname = configProperties.getRequired("influxdb.dbName");
        influxDB.setDatabase(dbname);
        influxDB.enableBatch(BatchOptions.DEFAULTS);
    }

    @Override
    public void invoke(RowWithScore rowWithScore, Context context) {
        recordsIn.add(1);
        try {
            // Map model entity to InfluxDB Point via InfluxDB annotations
            final Point point = Point.measurement("RowWithSource")
                    .time(rowWithScore.getTime(), TimeUnit.SECONDS)
                    .addField("sensor1", rowWithScore.getSensor1())
                    .addField("sensor2", rowWithScore.getSensor2())
                    .addField("sensor3", rowWithScore.getSensor3())
                    .addField("sensor4", rowWithScore.getSensor4())
                    .addField("sensor5", rowWithScore.getSensor5())
                    .addField("sensor6", rowWithScore.getSensor6())
                    .addField("sensor7", rowWithScore.getSensor7())
                    .addField("sensor8", rowWithScore.getSensor8())
                    .addField("sensor9", rowWithScore.getSensor9())
                    .addField("sensor10", rowWithScore.getSensor10())
                    .addField("scoreSensor1", rowWithScore.getScoreSensor1())
                    .addField("scoreSensor2", rowWithScore.getScoreSensor2())
                    .addField("scoreSensor3", rowWithScore.getScoreSensor3())
                    .addField("scoreSensor4", rowWithScore.getScoreSensor4())
                    .addField("scoreSensor5", rowWithScore.getScoreSensor5())
                    .addField("scoreSensor6", rowWithScore.getScoreSensor6())
                    .addField("scoreSensor7", rowWithScore.getScoreSensor7())
                    .addField("scoreSensor8", rowWithScore.getScoreSensor8())
                    .addField("scoreSensor9", rowWithScore.getScoreSensor9())
                    .addField("scoreSensor10", rowWithScore.getScoreSensor10())
                    .build();
            influxDB.write(point);
            recordsOut.add(1);
        } catch(Exception e) {
            log.error(e.getMessage());
        }
    }
}