/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.integration.testevents;

import com.exactpro.cradle.cassandra.integration.BaseCradleCassandraTest;
import com.exactpro.cradle.counters.Interval;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;

import static org.assertj.core.util.Lists.newArrayList;

public class PageScopesApiTest extends BaseCradleCassandraTest {
    private static final Logger logger = LoggerFactory.getLogger(PageScopesApiTest.class);

    private static final String SCOPE1 = "test_scope1";

    private static final String SCOPE2 = "test_scope2";

    private static final String SCOPE3 = "test_scope3";

    private static final String SCOPE4 = "test_scope4";

    private static final String SCOPE5 = "test_scope5";

    private static final String SCOPE6 = "test_scope6";

    private static final Set<String> allScopes = Set.of(SCOPE1, SCOPE2, SCOPE3, SCOPE4, SCOPE5, SCOPE6);

    @BeforeClass
    public void startUp() throws IOException, InterruptedException, CradleStorageException {
        super.startUp(true);
        generateData();
    }

    @Override
    protected void generateData() throws CradleStorageException, IOException {
        try {

            long EVENT_BATCH_DURATION = 24000L;
            long EVENTS_IN_BATCH = 4;
            //page 1
            TestEventToStore b1 = generateTestEvent(SCOPE1, dataStart.plus(2, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);
            TestEventToStore b2 = generateTestEvent(SCOPE2, dataStart.plus(3, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);
            TestEventToStore b3 = generateTestEvent(SCOPE2, dataStart.plus(4, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);

            //page 2
            TestEventToStore b4 = generateTestEvent(SCOPE2, dataStart.plus(11, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);
            TestEventToStore b5 = generateTestEvent(SCOPE2, dataStart.plus(12, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);
            TestEventToStore b6 = generateTestEvent(SCOPE3, dataStart.plus(14, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);

            //page 3
            TestEventToStore b7 = generateTestEvent(SCOPE4, dataStart.plus(24, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);
            TestEventToStore b8 = generateTestEvent(SCOPE5, dataStart.plus(27, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);

            //page 4
            TestEventToStore b9 = generateTestEvent(SCOPE6, dataStart.plus(34, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);
            TestEventToStore b10 = generateTestEvent(SCOPE5, dataStart.plus(36, ChronoUnit.MINUTES), EVENT_BATCH_DURATION, EVENT_BATCH_DURATION / EVENTS_IN_BATCH);

            List<TestEventToStore> data = List.of(b1, b2, b3, b4, b5, b6, b7, b8, b9, b10);

            for (TestEventToStore eventToStore : data) {
                storage.storeTestEvent(eventToStore);
            }

        } catch (CradleStorageException | IOException e) {
            logger.error("Error while generating data:", e);
            throw e;
        }
    }

    @Test(description = "Simply gets all scopes for a given book from database")
    public void getAllScopesTest() throws CradleStorageException, IOException {
        try {
            var actual = storage.getScopes(bookId);
            Assertions.assertThat(actual.size()).isEqualTo(6);
            Assertions.assertThat(actual).hasSameElementsAs(allScopes);
        } catch (IOException | CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Get scopes fora a given book and a time interval that covers only one page")
    public void getScopesOnePageNoDuplicatesTest() throws CradleStorageException {
        try {
            var resultSet = storage.getScopes(bookId, new Interval(dataStart, dataStart.plus(9, ChronoUnit.MINUTES)));
            var actual = newArrayList(resultSet.asIterable());
            Assertions.assertThat(actual.size()).isEqualTo(2);
            var expected = Set.of(SCOPE1, SCOPE2);
            Assertions.assertThat(actual).hasSameElementsAs(expected);
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Get session aliases fora a given book and a time interval that covers several pages")
    public void getScopesMultiPageNoDuplicatesTest() throws CradleStorageException {
        try {
            var resultSet = storage.getScopes(bookId, new Interval(dataStart, dataStart.plus(13, ChronoUnit.MINUTES)));
            var actual = newArrayList(resultSet.asIterable());
            Assertions.assertThat(actual.size()).isEqualTo(3);
            var expected = Set.of(SCOPE1, SCOPE2, SCOPE3);
            Assertions.assertThat(actual).hasSameElementsAs(expected);
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Test(description = "Get session aliases fora a given book and a time interval that covers several pages and matches partially")
    public void getScopesMultiPagePartialNoDuplicatesTest() throws CradleStorageException {
        try {
            var resultSet = storage.getScopes(
                    bookId,
                    new Interval(dataStart.plus(7, ChronoUnit.MINUTES), dataStart.plus(33, ChronoUnit.MINUTES))
            );
            var actual = newArrayList(resultSet.asIterable());
            Assertions.assertThat(actual.size()).isEqualTo(6);
            Assertions.assertThat(actual).hasSameElementsAs(allScopes);
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }
}
