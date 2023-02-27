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

import java.util.List;

public class ConvertingIteratorTest {

    @Test(description = "Test case when simple transformation is being done")
    public void testConvert() {
        ConvertingIterator<String, Integer> iterator = new ConvertingIterator<>(List.of("1", "2", "3", "4", "5", "6").iterator(), (el) -> Integer.parseInt(el));
        Iterable<Integer> actualIterable = () -> iterator;
        Iterable<Integer> expectedIterable = List.of(1, 2, 3, 4, 5, 6);

        Assertions.assertThat(actualIterable).containsExactlyElementsOf(expectedIterable);
    }
}
