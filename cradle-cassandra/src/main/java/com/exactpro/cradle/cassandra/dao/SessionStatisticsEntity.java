package com.exactpro.cradle.cassandra.dao;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

import java.time.Instant;

@Entity
public class SessionStatisticsEntity {

    public static final String FIELD_PAGE = "page";
    public static final String FIELD_RECORD_TYPE = "record_type";
    public static final String FIELD_FRAME_TYPE = "frame_type";
    public static final String FIELD_FRAME_START = "frame_start";
    public static final String FIELD_SESSION = "session";

    private String page;
    private Byte recordType;
    private Byte frameType;
    private Instant frameStart;
    private String session;

    public SessionStatisticsEntity() {
    }

    @PartitionKey(1)
    @CqlName(FIELD_PAGE)
    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    @PartitionKey(2)
    @CqlName(FIELD_RECORD_TYPE)
    public Byte getRecordType() {
        return recordType;
    }

    public void setRecordType(Byte recordType) {
        this.recordType = recordType;
    }

    @PartitionKey(3)
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
}
