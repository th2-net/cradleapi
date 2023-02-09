package com.exactpro.cradle.cassandra.iterators;

import com.exactpro.cradle.cassandra.iterators.CradleIterators;
import org.assertj.core.api.Assertions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

public class CradleIteratorsTest {

    List<String> basic;

    @BeforeMethod
    public void before () {
        basic = List.of("1", "2", "3", "4", "5", "6");
    }

    @Test(description = "Tests case when predicate becomes true for first element, resulting in empty iterator")
    public void testUnlessFirstElement() {
        Iterable<String> actualIterable = () -> CradleIterators.unless(basic.iterator(), (el) -> el.equals("1"));
        Iterable<String> expectedIterable = Collections::emptyIterator;

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Test case when predicate becomes true for some element in the middle")
    public void testUnlessMiddleElement() {
        Iterable<String> actualIterable = () -> CradleIterators.unless(basic.iterator(), (el) -> el.equals("4"));
        Iterable<String> expectedIterable = () -> List.of("1", "2", "3").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Tests case when predicate does not come true for any element, resulting in no change")
    public void testUnlessAll() {
        Iterable<String> actualIterable = () -> CradleIterators.unless(basic.iterator(), (el) -> el.equals("non existant"));
        Iterable<String> expectedIterable = () -> List.of("1", "2", "3", "4", "5", "6").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Tests case when original iterator is empty")
    public void testUnlessForEmptyIterator () {

        Iterable<String> actualIterable = () -> CradleIterators.unless(Collections.emptyIterator(), (el) -> el.equals("non existant"));
        Iterable<String> expectedIterable = Collections::emptyIterator;

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Tests case when all of the elements are unique, resulting in no skipping")
    public void TestUniqueNoDuplicates() {

        Iterable<String> actualIterable = () -> CradleIterators.unique(basic.iterator());
        Iterable<String> expectedIterable = () -> List.of("1", "2", "3", "4", "5", "6").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Tests case when iterator contains only one unique element")
    public void TestUniqueForSameElements() {
        Iterable<String> actualIterable = () -> CradleIterators.unique(List.of("1", "1", "1", "1", "1").iterator());
        Iterable<String> expectedIterable = () -> List.of("1").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Tests case when iterator contains duplicates")
    public void TestUniqueForMix() {
        Iterable<String> actualIterable = () -> CradleIterators.unique(List.of("5", "1", "3", "2", "3", "1", "5", "4").iterator());
        Iterable<String> expectedIterable = () -> List.of("5", "1", "3", "2", "4").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Tests Composed iterator: at first transformed, then filtered, then uniqunified and then limited")
    public void TestComposedTransformedFilteredUniqueLimited() {

        Iterator<String> original = List.of("-5", "1", "3", "2", "3", "1", "5", "-5", "4").iterator();
        Iterator<Integer> transformed = CradleIterators.transform(original, s -> (s != null) ? Integer.parseInt(s) : 0);
        Iterator<Integer> filtered = CradleIterators.filter(transformed, (el) -> el > 0);
        Iterator<Integer> uniquified = CradleIterators.unique(filtered);
        Iterator<Integer> limited = CradleIterators.limit(uniquified, 5);

        Iterable<Integer> actualIterable = () -> limited;
        Iterable<Integer> expectedIterable = () -> List.of(1, 3, 2, 5, 4).iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }

    @Test(description = "Tests Composed iterator: at first limited, then transformed, then filtered, and then uniqunified")
    public void TestComposedLimitedTransformedFilteredUnique() {

        Iterator<String> original = List.of("-5", "1", "3", "2", "3", "1", "5", "-5", "4").iterator();
        Iterator<String> limited = CradleIterators.limit(original, 5);
        Iterator<Integer> transformed = CradleIterators.transform(limited, s -> (s != null) ? Integer.parseInt(s) : 0);
        Iterator<Integer> filtered = CradleIterators.filter(transformed, (el) -> el > 0);
        Iterator<Integer> uniquified = CradleIterators.unique(filtered);

        Iterable<Integer> actualIterable = () -> uniquified;
        Iterable<Integer> expectedIterable = () -> List.of(1, 3, 2).iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }
}
