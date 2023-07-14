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

package com.exactpro.cradle.cassandra.integration.messages;

import com.exactpro.cradle.counters.Interval;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Set;

import static org.assertj.core.util.Lists.newArrayList;

public class PageGroupsApiTest extends BaseSessionsApiTest {
    @BeforeClass
    public void startUp() throws IOException, InterruptedException, CradleStorageException {
        super.startUp(true);
        generateData();
    }

    @Test(description = "Simply gets all group aliases for a given book from database")
    public void getAllGroupAliasesTest() throws CradleStorageException, IOException {
            var actual = storage.getGroups(bookId);
            Assertions.assertThat(actual.size()).isEqualTo(4);
            Assertions.assertThat(actual).hasSameElementsAs(allGroups);
    }

    @Test(description = "Get group aliases fora a given book and a time interval that covers only one page")
    public void getGroupAliasesOnePageNoDuplicatesTest() throws CradleStorageException {
            var resultSet = storage.getSessionGroups(bookId, new Interval(dataStart, dataStart.plus(10, ChronoUnit.MINUTES)));
            var actual = newArrayList(resultSet.asIterable());
            Assertions.assertThat(actual.size()).isEqualTo(2);
            var expected = Set.of(GROUP1_NAME, GROUP2_NAME);
            Assertions.assertThat(actual).hasSameElementsAs(expected);
    }

    @Test(description = "Get group aliases fora a given book and a time interval that covers several pages")
    public void getGroupAliasesMultiPageNoDuplicatesTest() throws CradleStorageException {
            var resultSet = storage.getSessionGroups(bookId, new Interval(dataStart, dataStart.plus(23, ChronoUnit.MINUTES)));
            var actual = newArrayList(resultSet.asIterable());
            Assertions.assertThat(actual.size()).isEqualTo(3);
            var expected = Set.of(GROUP1_NAME, GROUP2_NAME, GROUP3_NAME);
            Assertions.assertThat(actual).hasSameElementsAs(expected);
    }

    @Test(description = "Get group aliases fora a given book and a time interval that covers several pages and matches partially")
    public void getGroupAliasesMultiPagePartialNoDuplicatesTest() throws CradleStorageException {
            var resultSet = storage.getSessionGroups(
                    bookId,
                    new Interval(dataStart.plus(7, ChronoUnit.MINUTES), dataStart.plus(33, ChronoUnit.MINUTES))
            );
            var actual = newArrayList(resultSet.asIterable());
            Assertions.assertThat(actual.size()).isEqualTo(4);
            Assertions.assertThat(actual).hasSameElementsAs(allGroups);
    }
}
