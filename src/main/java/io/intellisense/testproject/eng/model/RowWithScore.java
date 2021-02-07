package io.intellisense.testproject.eng.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.Row;
import org.influxdb.annotation.Measurement;

import java.time.Instant;

@AllArgsConstructor
@Data
@Slf4j
@Measurement(name = "memory")
public class RowWithScore {
    private Long time;
    private double sensor1;
    private double sensor2;
    private double sensor3;
    private double sensor4;
    private double sensor5;
    private double sensor6;
    private double sensor7;
    private double sensor8;
    private double sensor9;
    private double sensor10;
    private double scoreSensor1;
    private double scoreSensor2;
    private double scoreSensor3;
    private double scoreSensor4;
    private double scoreSensor5;
    private double scoreSensor6;
    private double scoreSensor7;
    private double scoreSensor8;
    private double scoreSensor9;
    private double scoreSensor10;

    public RowWithScore(Row row) {
        if (row.getField(0) != null) {
            time = Instant.parse(String.valueOf(row.getField(0))).getEpochSecond();
        }

        sensor1 = parseStringToDouble(row.getField(1));
        sensor2 = parseStringToDouble(row.getField(2));
        sensor3 = parseStringToDouble(row.getField(3));
        sensor4 = parseStringToDouble(row.getField(4));
        sensor5 = parseStringToDouble(row.getField(5));
        sensor6 = parseStringToDouble(row.getField(6));
        sensor7 = parseStringToDouble(row.getField(7));
        sensor8 = parseStringToDouble(row.getField(8));
        sensor9 = parseStringToDouble(row.getField(9));
        sensor10 = parseStringToDouble(row.getField(10));

    }

    private static double parseStringToDouble(Object value) {
        return value == null || (String.valueOf(value)).trim().isEmpty() ? 0 : Double.parseDouble(String.valueOf(value));
    }
}
