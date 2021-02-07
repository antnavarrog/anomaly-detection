package io.intellisense.testproject.eng.function;

import io.intellisense.testproject.eng.model.RowWithScore;
import io.intellisense.testproject.eng.utils.IQR;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class AnomalyDetectionFunction extends ProcessAllWindowFunction<Row, List<RowWithScore>, TimeWindow> {
    private transient ListState<Row> rowListState;
    private transient ValueState<Long> counter;

    @Override
    public void process(Context context, Iterable<Row> elements, Collector<List<RowWithScore>> out) throws Exception {

        Long currentCount = counter.value();

        for (Row element : elements) {
            rowListState.add(element);
            currentCount += 1;
            counter.update(currentCount);
        }

        if (currentCount == 100) {
            List<RowWithScore> rowWithScores = new ArrayList<>();

            for (Row row : rowListState.get()) {
                rowWithScores.add(new RowWithScore(row));
            }

            List<Double> scoresForSensor = generateIQR(rowWithScores);
            for (RowWithScore rowWithScore : rowWithScores) {
                rowWithScore.setScoreSensor1(getScore(rowWithScore.getSensor1(), scoresForSensor.get(0)));
                rowWithScore.setScoreSensor2(getScore(rowWithScore.getSensor2(), scoresForSensor.get(1)));
                rowWithScore.setScoreSensor3(getScore(rowWithScore.getSensor3(), scoresForSensor.get(2)));
                rowWithScore.setScoreSensor4(getScore(rowWithScore.getSensor4(), scoresForSensor.get(3)));
                rowWithScore.setScoreSensor5(getScore(rowWithScore.getSensor5(), scoresForSensor.get(4)));
                rowWithScore.setScoreSensor6(getScore(rowWithScore.getSensor6(), scoresForSensor.get(5)));
                rowWithScore.setScoreSensor7(getScore(rowWithScore.getSensor7(), scoresForSensor.get(6)));
                rowWithScore.setScoreSensor8(getScore(rowWithScore.getSensor8(), scoresForSensor.get(7)));
                rowWithScore.setScoreSensor9(getScore(rowWithScore.getSensor9(), scoresForSensor.get(8)));
                rowWithScore.setScoreSensor10(getScore(rowWithScore.getSensor10(), scoresForSensor.get(9)));
            }

            out.collect(rowWithScores);
            rowListState.clear();
            counter.clear();
        }
    }

    public void open(Configuration conf) {
        ListStateDescriptor<Row> listDesc = new ListStateDescriptor<Row>("rowListState", Row.class);
        rowListState = getRuntimeContext().getListState(listDesc);

        ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>("counter", Long.class, 0L);
        counter = getRuntimeContext().getState(descriptor2);
    }

    public Double getScore(double sensorValue, double iqrValue) {
        if (sensorValue < (1.5 * iqrValue)){
            return (double) 0;
        }else if (sensorValue >= (1.5 * iqrValue)){
            return 0.5;
        }else {
            return (double) 1;
        }
    }

    public List<Double> generateIQR(List<RowWithScore> rowWithScores) {
        List<Double> result = new ArrayList<>();
        List<RowWithScore> tempList = new ArrayList<>(rowWithScores);

        double[] arraySensor1 = tempList.stream().map(RowWithScore::getSensor1).mapToDouble(Double::doubleValue).sorted().toArray();
        double[] arraySensor2 = tempList.stream().map(RowWithScore::getSensor2).mapToDouble(Double::doubleValue).sorted().toArray();
        double[] arraySensor3 = tempList.stream().map(RowWithScore::getSensor3).mapToDouble(Double::doubleValue).sorted().toArray();
        double[] arraySensor4 = tempList.stream().map(RowWithScore::getSensor4).mapToDouble(Double::doubleValue).sorted().toArray();
        double[] arraySensor5 = tempList.stream().map(RowWithScore::getSensor5).mapToDouble(Double::doubleValue).sorted().toArray();
        double[] arraySensor6 = tempList.stream().map(RowWithScore::getSensor6).mapToDouble(Double::doubleValue).sorted().toArray();
        double[] arraySensor7 = tempList.stream().map(RowWithScore::getSensor7).mapToDouble(Double::doubleValue).sorted().toArray();
        double[] arraySensor8 = tempList.stream().map(RowWithScore::getSensor8).mapToDouble(Double::doubleValue).sorted().toArray();
        double[] arraySensor9 = tempList.stream().map(RowWithScore::getSensor9).mapToDouble(Double::doubleValue).sorted().toArray();
        double[] arraySensor10 = tempList.stream().map(RowWithScore::getSensor10).mapToDouble(Double::doubleValue).sorted().toArray();


        //with the list ordered calculate the IQR for each sensor
        result.add(IQR.calculateIQR(arraySensor1, arraySensor1.length));
        result.add(IQR.calculateIQR(arraySensor2, arraySensor2.length));
        result.add(IQR.calculateIQR(arraySensor3, arraySensor3.length));
        result.add(IQR.calculateIQR(arraySensor4, arraySensor4.length));
        result.add(IQR.calculateIQR(arraySensor5, arraySensor5.length));
        result.add(IQR.calculateIQR(arraySensor6, arraySensor6.length));
        result.add(IQR.calculateIQR(arraySensor7, arraySensor7.length));
        result.add(IQR.calculateIQR(arraySensor8, arraySensor8.length));
        result.add(IQR.calculateIQR(arraySensor9, arraySensor9.length));
        result.add(IQR.calculateIQR(arraySensor10, arraySensor10.length));

        return result;
    }
}
