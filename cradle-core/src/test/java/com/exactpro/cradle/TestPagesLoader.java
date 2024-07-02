/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.cradle;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class TestPagesLoader implements PagesLoader {

    private final List<PageInfo> pages;

    public TestPagesLoader(List<PageInfo> pages) {
        this.pages = pages;
    }

    @Nonnull
    @Override
    public Collection<PageInfo> load(@Nonnull BookId bookId, @Nullable Instant start, @Nullable Instant end) {
        return pages.stream().map(TestPagesLoader::copy).collect(Collectors.toList());
    }

    public static PageInfo copy(PageInfo pageInfo) {
        return new PageInfo(
                pageInfo.getId(),
                pageInfo.getEnded(),
                pageInfo.getComment(),
                pageInfo.getUpdated(),
                pageInfo.getRemoved()
        );
    }
}
