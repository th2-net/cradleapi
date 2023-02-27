/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.cradle.iterators;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

public class TakeWhileIteratorTest {
    @Test(description = "Tests case when is false for first element, resulting in empty iterator")
    public void testTakeWhileFirstElement() {
        TakeWhileIterator<String> iterator = new TakeWhileIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator(), (el) -> !el.equals("1"));
        Iterable<String> actualIterable = () -> iterator;
        Iterable<String> expectedIterable = Collections::emptyIterator;

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
        Assertions.assertThat(iterator.isHalted()).isTrue();
    }

    @Test(description = "Test case when predicate fails for some element in the middle")
    public void testTakeWhileMiddleElement() {
        TakeWhileIterator<String> iterator = new TakeWhileIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator(), (el) -> !el.equals("4"));
        Iterable<String> actualIterable = () -> iterator;
        Iterable<String> expectedIterable = () -> List.of("1", "2", "3").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
        Assertions.assertThat(iterator.isHalted()).isTrue();
    }

    @Test(description = "Tests case when predicate is true for all elements, resulting in no change")
    public void testTakeWhileAll() {
        TakeWhileIterator<String> iterator = new TakeWhileIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator(), (el) -> !el.equals("non existant"));
        Iterable<String> actualIterable = () -> iterator;
        Iterable<String> expectedIterable = () -> List.of("1", "2", "3", "4", "5", "6").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
        Assertions.assertThat(iterator.isHalted()).isFalse();
    }

    @Test(description = "Tests case when original iterator is empty")
    public void testTakeWhileForEmpty() {
        TakeWhileIterator<String> iterator = new TakeWhileIterator<>(Collections.emptyIterator(), (el) -> el.equals("non existant"));
        Iterable<String> actualIterable = () -> iterator;
        Iterable<String> expectedIterable = Collections::emptyIterator;

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
        Assertions.assertThat(iterator.isHalted()).isFalse();
    }

    @Test(description = "Tests case when predicate is false for last element, resulting in iterator if same elements except the last")
    public void testTakeWhileForAllButLast() {
        TakeWhileIterator<String> iterator = new TakeWhileIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator(), (el) -> !el.equals("6"));
        Iterable<String> actualIterable = () -> iterator;
        Iterable<String> expectedIterable = () -> List.of("1", "2", "3", "4", "5").iterator();

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
        Assertions.assertThat(iterator.isHalted()).isTrue();
    }
}
