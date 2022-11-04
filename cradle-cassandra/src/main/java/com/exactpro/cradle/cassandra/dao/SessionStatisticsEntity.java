package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.mapper.annotations.*;

import java.time.Instant;
import java.util.Objects;

@Entity
@CqlName(SessionStatisticsEntity.TABLE_NAME)
@PropertyStrategy(mutable = false)
public class SessionStatisticsEntity {
    public static final String TABLE_NAME = "session_statistics";

    public static final String FIELD_BOOK = "book";
    public static final String FIELD_PAGE = "page";
    public static final String FIELD_RECORD_TYPE = "record_type";
    public static final String FIELD_FRAME_TYPE = "frame_type";
    public static final String FIELD_FRAME_START = "frame_start";
    public static final String FIELD_SESSION = "session";

    @PartitionKey(1)
    @CqlName(FIELD_BOOK)
    private final String book;

    @PartitionKey(2)
    @CqlName(FIELD_PAGE)
    private final String page;

    @PartitionKey(3)
    @CqlName(FIELD_RECORD_TYPE)
    private final Byte recordType;

    @PartitionKey(4)
    @CqlName(FIELD_FRAME_TYPE)
    private final Byte frameType;

    @ClusteringColumn(1)
    @CqlName(FIELD_FRAME_START)
    private final Instant frameStart;

    @ClusteringColumn(2)
    @CqlName(FIELD_SESSION)
    private final String session;

    public SessionStatisticsEntity(String book, String page, Byte recordType, Byte frameType, Instant frameStart, String session) {
        this.book = book;
        this.page = page;
        this.recordType = recordType;
        this.frameType = frameType;
        this.frameStart = frameStart;
        this.session = session;
    }

    public String getBook() {
        return book;
    }
    public String getPage() {
        return page;
    }
    public Byte getRecordType() {
        return recordType;
    }
    public Byte getFrameType() {
        return frameType;
    }
    public Instant getFrameStart() {
        return frameStart;
    }
    public String getSession() {
        return session;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SessionStatisticsEntity that = (SessionStatisticsEntity) o;

        return Objects.equals(getBook(), that.getBook()) &&
                Objects.equals(getPage(), that.getPage()) &&
                Objects.equals(getRecordType(), that.getRecordType()) &&
                Objects.equals(getFrameType(), that.getFrameType()) &&
                Objects.equals(getFrameStart(), that.getFrameStart()) &&
                Objects.equals(getSession(), that.getSession());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBook(),
                getPage(),
                getRecordType(),
                getFrameType(),
                getFrameStart(),
                getSession());
    }

    @Override
    public String toString() {
        return String.format("%s [%s=\"%s\", %s=\"%s\", %s=%d, %s=%d, %s=%s, %s=\"%s\"]"
                , getClass().getSimpleName()
                , FIELD_BOOK, book
                , FIELD_PAGE, page
                , FIELD_RECORD_TYPE, recordType
                , FIELD_FRAME_TYPE, frameType
                , FIELD_FRAME_START, frameStart.toString()
                , FIELD_SESSION, session);
    }
}
