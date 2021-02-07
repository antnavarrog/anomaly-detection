package io.intellisense.testproject.eng.function;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class AnomalyDetectionFunctionTest {

    private AnomalyDetectionFunction underTest;

    @Before
    public void setUp() {
        underTest = new AnomalyDetectionFunction();
    }

    @Test
    public void givenASensorValueAndIQRValue_ThenReturnsTheScore() {
        double sensorValue = 0;
        double iqrValue = 0;

        double expectedResult = 0.5;
        double result = underTest.getScore(sensorValue, iqrValue);
        assertEquals(expectedResult, result);

    }
}
