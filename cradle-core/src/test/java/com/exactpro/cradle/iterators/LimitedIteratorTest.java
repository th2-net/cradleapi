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
