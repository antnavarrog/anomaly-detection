package io.intellisense.testproject.eng.utils;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IQRTest {
    @Test
    public void givenAnArrayOfDecimals_ThenReturnsTheIRQValue() {
        double []arrayToTest = {29.7, 36.2, 29.1, 11.7, 45.3, 19.6, 42.8, 57.9, 51.9, 42.9, 51.2, 5.4, 29.2, 15.4,11.6};
        double expectedValue = 29.9;

        double result = IQR.calculateIQR(arrayToTest, arrayToTest.length);
        assertEquals(expectedValue, result);
    }
}
