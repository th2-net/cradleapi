package com.exactpro.cradle.iterators;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.List;

public class UniqueIteratorTest {

    @Test(description = "Tests case when all of the elements are unique, resulting in no skipping")
    public void TestUniqueNoDuplicates() {

        Iterable<String> actualIterable = () -> new UniqueIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator());
        Iterable<String> expectedIterable = () -> List.of("1", "2", "3", "4", "5", "6").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Tests case when iterator contains only one unique element")
    public void TestUniqueForSameElements() {
        Iterable<String> actualIterable = () -> new UniqueIterator<>(List.of("1", "1", "1", "1", "1").iterator());
        Iterable<String> expectedIterable = () -> List.of("1").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Tests case when iterator contains duplicates")
    public void TestUniqueForMix() {
        Iterable<String> actualIterable = () -> new UniqueIterator<>(List.of("5", "1", "3", "2", "3", "1", "5", "4").iterator());
        Iterable<String> expectedIterable = () -> List.of("5", "1", "3", "2", "4").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }
}
