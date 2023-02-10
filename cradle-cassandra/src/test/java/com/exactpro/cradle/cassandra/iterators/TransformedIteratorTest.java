package com.exactpro.cradle.cassandra.iterators;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.List;

public class TransformedIteratorTest {

    @Test(description = "Test case when simple transformation is being done")
    public void testTransform() {
        TransformedIterator<String, Integer> iterator = new TransformedIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator(), (el) -> Integer.parseInt(el));
        Iterable<Integer> actualIterable = () -> iterator;
        Iterable<Integer> expectedIterable = List.of(1, 2, 3, 4, 5, 6);

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }
}
