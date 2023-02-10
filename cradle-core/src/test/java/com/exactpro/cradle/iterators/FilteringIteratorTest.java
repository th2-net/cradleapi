package com.exactpro.cradle.iterators;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

public class FilteringIteratorTest {

    @Test(description = "Test case when all of the elements are being filtered out, resulting in empty iterator")
    public void testFilterAll() {
        FilteringIterator<String> iterator = new FilteringIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator(), (el) -> el.equals("0"));
        Iterable<String> actualIterable = () -> iterator;
        Iterable<String> expectedIterable = Collections::emptyIterator;

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Test case when none of the elements are being filtered out, resulting in same elements")
    public void testFilterNone() {
        FilteringIterator<String> iterator = new FilteringIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator(), (el) -> !el.equals("0"));
        Iterable<String> actualIterable = () -> iterator;
        Iterable<String> expectedIterable = () -> List.of("1", "2", "3", "4", "5", "6").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Test case when iterator contains strings of various lengths and only elements of length 1 are being left")
    public void testFilterSome() {
        var iterator = new FilteringIterator<>(List.of("1", "23", "45", "6", "7", "890").iterator(), (el) -> el.length() == 1);
        Iterable<String> actualIterable = () -> iterator;
        Iterable<String> expectedIterable = () -> List.of("1", "6", "7").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }
}
