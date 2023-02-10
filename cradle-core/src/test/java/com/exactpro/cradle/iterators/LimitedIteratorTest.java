package com.exactpro.cradle.iterators;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

public class LimitedIteratorTest {

    @Test(description = "Test case when negative limit throws exception")
    public void testLimitNegative() {
        Assertions.assertThatIllegalArgumentException().isThrownBy(() -> new LimitedIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator(), -1));
    }

    @Test(description = "Test case when zero limit results in empty iterator")
    public void testLimitZero() {
        var iterator = new LimitedIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator(), 0);
        Iterable<String> actualIterable = () -> iterator;
        Iterable<String> expectedIterable = Collections::emptyIterator;

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Test case when limit is greater than number of elements results in iterator with same elements")
    public void testLimitGreaterThanSize() {
        var iterator = new LimitedIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator(), 10);
        Iterable<String> actualIterable = () -> iterator;
        Iterable<String> expectedIterable = () -> List.of("1", "2", "3", "4", "5", "6").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Test case when limit is smaller than number of elements results in smaller iterator")
    public void testLimitSmallerThanSize() {
        var iterator = new LimitedIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator(), 3);
        Iterable<String> actualIterable = () -> iterator;
        Iterable<String> expectedIterable = () -> List.of("1", "2", "3").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }
}
