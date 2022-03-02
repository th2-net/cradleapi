package com.exactpro.cradle;

import java.io.Serializable;
import java.util.Objects;

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
