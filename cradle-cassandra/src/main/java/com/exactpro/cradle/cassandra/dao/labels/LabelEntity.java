package com.exactpro.cradle.cassandra.dao.labels;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.cassandra.dao.messages.MessageBatchEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Entity
public class LabelEntity {
    private static final Logger logger = LoggerFactory.getLogger(LabelEntity.class);

    public static final String FIELD_PAGE = "page";
    public static final String FIELD_NAME = "name";

    @PartitionKey(0)
    @CqlName(FIELD_PAGE)
    private String page;

    @PartitionKey(1)
    @CqlName(FIELD_NAME)
    private String name;

    public LabelEntity(){
    }

    public LabelEntity(String page, String name){
        setPage(page);
        setName(name);
    }

    public String getPage() { return page; }

    public void setPage(String page) { this.page = page; }

    public String getname() { return name; }

    public void setName(String name) { this.name = name; }

}
