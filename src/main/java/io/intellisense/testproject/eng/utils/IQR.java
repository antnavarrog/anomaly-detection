package io.intellisense.testproject.eng.utils;
import java.util.Arrays;

public class IQR{
    public static double calculateIQR(double [] data, int n) {
        double [] dataSorted = Arrays.stream(data).sorted().toArray();
        double q1 = findMedian(dataSorted, 0, dataSorted.length / 2 - 1);
        double q3 = findMedian(dataSorted, (dataSorted.length + 1) / 2, dataSorted.length - 1);
        return (q3 - q1);
    }

    private static double findMedian(double [] array, int start, int end) {
        if ((end - start) % 2 == 0) { // odd number of elements
            return (array[(end + start) / 2]);
        } else {
            double value1 = array[(end + start) / 2];
            double value2 = array[(end + start) / 2 + 1];
            return (value1 + value2) / 2.0;
        }
    }
}