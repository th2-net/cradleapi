/*
 * Copyright 2021-2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.testevents;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;

import static org.assertj.core.api.Assertions.assertThat;


public class EventTestUtils {

    public static void assertEvents(StoredTestEvent storedEvent,
                                    TestEventToStore toStoreEvent,
                                    RecursiveComparisonConfiguration batchConfig) {
        assertThat(storedEvent).describedAs("StoredTestEvent").satisfies(stored -> {
            assertTestEvent(stored, toStoreEvent);
            assertThat(stored.isSingle()).describedAs("isSingle").isEqualTo(toStoreEvent.isSingle());
            assertThat(stored.isBatch()).describedAs("isBatch").isEqualTo(toStoreEvent.isBatch());
            if (stored.isSingle()) {
                assertThat(stored.asSingle()).describedAs("TestEventSingle")
                        .satisfies(single -> assertTestEventSingle(single, toStoreEvent.asSingle()));
            }
            if (stored.isBatch()) {
                assertThat(stored.asBatch()).describedAs("TestEventBatch")
                        .satisfies(batch -> {
                            if (batchConfig == null) {
                                assertTestEventBatch(batch, toStoreEvent.asBatch());
                            } else {
                                assertTestEventBatch(batch, toStoreEvent.asBatch(), batchConfig);
                            }
                        });
            }
        });
    }

    public static void assertEvents(StoredTestEvent storedEvent, TestEventToStore toStoreEvent) {
        assertEvents(storedEvent, toStoreEvent, null);
    }

    private static void assertTestEventBatch(TestEventBatch actual,
                                             TestEventBatch expected,
                                             RecursiveComparisonConfiguration batchConfig) {
        assertTestEvent(actual, expected);
        assertThat(actual.getTestEventsCount()).describedAs("getTestEventsCount")
                .isEqualTo(expected.getTestEventsCount());

        assertThat(actual.getTestEvents()).describedAs("getTestEvents")
                .usingRecursiveFieldByFieldElementComparator(batchConfig)
                .containsExactlyElementsOf(expected.getTestEvents());

        assertThat(actual.getRootTestEvents()).describedAs("getRootTestEvents")
                .usingRecursiveFieldByFieldElementComparator(batchConfig)
                .containsExactlyElementsOf(expected.getRootTestEvents());

        assertThat(actual.getBatchMessages()).describedAs("getBatchMessages")
                .isEqualTo(expected.getBatchMessages());
    }

    private static void assertTestEventBatch(TestEventBatch actual, TestEventBatch expected) {
        assertTestEvent(actual, expected);
        assertThat(actual.getTestEventsCount()).describedAs("getTestEventsCount")
                .isEqualTo(expected.getTestEventsCount());
        assertThat(actual.getTestEvents()).describedAs("getTestEvents")
                .containsExactlyElementsOf(expected.getTestEvents());
        assertThat(actual.getRootTestEvents()).describedAs("getRootTestEvents")
                .containsExactlyElementsOf(expected.getRootTestEvents());
        assertThat(actual.getBatchMessages()).describedAs("getBatchMessages")
                .isEqualTo(expected.getBatchMessages());
    }

    private static void assertTestEventSingle(TestEventSingle actual, TestEventSingle expected) {
        assertTestEvent(actual, expected);
        //noinspection deprecation
        assertThat(actual.getContent()).describedAs("getContent")
                .isEqualTo(expected.getContent());
        assertThat(actual.getContentBuffer()).describedAs("getContentBuffer")
                .isEqualTo(expected.getContentBuffer());
    }

    private static void assertTestEvent(TestEvent actual, TestEvent expected) {
        assertThat(actual.getBookId()).describedAs("getBookId")
                .isEqualTo(expected.getBookId());
        assertThat(actual.getScope()).describedAs("getScope")
                .isEqualTo(expected.getScope());
        assertThat(actual.getId()).describedAs("getId")
                .isEqualTo(expected.getId());
        assertThat(actual.getParentId()).describedAs("getParentId")
                .isEqualTo(expected.getParentId());
        assertThat(actual.getName()).describedAs("getName")
                .isEqualTo(expected.getName());
        assertThat(actual.getType()).describedAs("getType")
                .isEqualTo(expected.getType());
        assertThat(actual.getEndTimestamp()).describedAs("getEndTimestamp")
                .isEqualTo(expected.getEndTimestamp());
        assertThat(actual.getStartTimestamp()).describedAs("getStartTimestamp")
                .isEqualTo(expected.getStartTimestamp());
        assertThat(actual.getMessages()).describedAs("getMessages")
                .isEqualTo(expected.getMessages());
        assertThat(actual.isSuccess()).describedAs("isSuccess")
                .isEqualTo(expected.isSuccess());
    }
}
