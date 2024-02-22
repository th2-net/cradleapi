/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.integration.pages;

import com.exactpro.cradle.cassandra.integration.BaseCradleCassandraTest;
import com.exactpro.cradle.utils.CradleStorageException;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.stream.Collectors;

public class PagesApiTest extends BaseCradleCassandraTest {
    private static final Logger logger = LoggerFactory.getLogger(PagesApiTest.class);

    @BeforeClass
    public void startUp() throws IOException, InterruptedException, CradleStorageException {
        super.startUp(true);
        generateData();
    }

    @Override
    protected void generateData() throws CradleStorageException, IOException {
        try {
            for (long i = 3; i < 7; i++) {
                Instant start = Instant.now().plus(i, ChronoUnit.MINUTES);
                storage.addPage(
                        bookId,
                        "autoPageNotNull-" + (i - 2),
                        start,
                        "auto page"
                );
            }
        } catch (CradleStorageException | IOException e) {
            logger.error("Error while generating data:", e);
            throw e;
        }
    }

    @Test(description = "Simply gets all pages, filters them using name and their default values are not null")
    public void testNonNullPages() throws CradleStorageException {
        try {
            var result = storage.getAllPages(bookId);
            var autoPagesNonNull = result.stream()
                    .filter(pageInfo -> pageInfo.getName().startsWith("autoPageNotNull-"))
                    .collect(Collectors.toList());
            Assertions.assertThat(autoPagesNonNull.size()).isEqualTo(4);
            autoPagesNonNull.forEach(pageInfo -> {
                Assertions.assertThat(pageInfo.getComment()).isNotNull();
                Assertions.assertThat(pageInfo.getUpdated()).isNotNull();
                Assertions.assertThat(pageInfo.getRemoved()).isNotNull();
            });
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }
}
