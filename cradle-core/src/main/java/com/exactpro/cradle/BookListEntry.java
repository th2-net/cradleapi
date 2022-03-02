/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

import java.io.Serializable;
import java.util.Objects;

/*
    Entries of listBooks method return value
 */

public class BookListEntry implements Serializable {

    private static final long serialVersionUID = 682919942870363336L;
    private final String name;
    private final String schemaVersion;

    public BookListEntry (String name, String schemaVersion) {
        this.name = name;
        this.schemaVersion = schemaVersion;
    }

    public String getName() {
        return name;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BookListEntry that = (BookListEntry) o;
        return Objects.equals(name, that.name) && Objects.equals(schemaVersion, that.schemaVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, schemaVersion);
    }

    @Override
    public String toString() {
        return "BookListEntry [" +
                "name='" + name + '\'' +
                ", schemaVersion='" + schemaVersion + '\'' +
                ']';
    }
}
