package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

import java.time.Instant;
import java.util.Objects;

@Entity
@CqlName(SessionStatisticsEntity.TABLE_NAME)
public class SessionStatisticsEntity {
    public static final String TABLE_NAME = "session_statistics";

    public static final String FIELD_BOOK = "book";
    public static final String FIELD_PAGE = "page";
    public static final String FIELD_RECORD_TYPE = "record_type";
    public static final String FIELD_FRAME_TYPE = "frame_type";
    public static final String FIELD_FRAME_START = "frame_start";
    public static final String FIELD_SESSION = "session";

    private String book;
    private String page;
    private Byte recordType;
    private Byte frameType;
    private Instant frameStart;
    private String session;

    public SessionStatisticsEntity() {
    }

    public SessionStatisticsEntity(String book, String page, Byte recordType, Byte frameType, Instant frameStart, String session) {
        this.book = book;
        this.page = page;
        this.recordType = recordType;
        this.frameType = frameType;
        this.frameStart = frameStart;
        this.session = session;
    }

    @PartitionKey(1)
    @CqlName(FIELD_BOOK)
    public String getBook() {
        return book;
    }

    public void setBook(String book) {
        this.book = book;
    }

    @PartitionKey(2)
    @CqlName(FIELD_PAGE)
    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    @PartitionKey(3)
    @CqlName(FIELD_RECORD_TYPE)
    public Byte getRecordType() {
        return recordType;
    }

    public void setRecordType(Byte recordType) {
        this.recordType = recordType;
    }

    @PartitionKey(4)
    @CqlName(FIELD_FRAME_TYPE)
    public Byte getFrameType() {
        return frameType;
    }

    public void setFrameType(Byte frameType) {
        this.frameType = frameType;
    }

    @ClusteringColumn(1)
    @CqlName(FIELD_FRAME_START)
    public Instant getFrameStart() {
        return frameStart;
    }

    public void setFrameStart(Instant frameStart) {
        this.frameStart = frameStart;
    }

    @ClusteringColumn(2)
    @CqlName(FIELD_SESSION)
    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SessionStatisticsEntity that = (SessionStatisticsEntity) o;
        return Objects.equals(book, that.book) &&
                Objects.equals(page, that.page) &&
                Objects.equals(recordType, that.recordType) &&
                Objects.equals(frameType, that.frameType) &&
                Objects.equals(frameStart, that.frameStart) &&
                Objects.equals(session, that.session);
    }

    @Override
    public int hashCode() {
        return Objects.hash(book, page, recordType, frameType, frameStart, session);
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
