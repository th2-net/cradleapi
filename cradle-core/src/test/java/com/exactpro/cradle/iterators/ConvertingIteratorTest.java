package com.exactpro.cradle.iterators;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.List;

public class ConvertingIteratorTest {

    @Test(description = "Test case when simple transformation is being done")
    public void testTransform() {
        ConvertingIterator<String, Integer> iterator = new ConvertingIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator(), (el) -> Integer.parseInt(el));
        Iterable<Integer> actualIterable = () -> iterator;
        Iterable<Integer> expectedIterable = List.of(1, 2, 3, 4, 5, 6);

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }
}
