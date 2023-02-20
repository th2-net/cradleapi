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
