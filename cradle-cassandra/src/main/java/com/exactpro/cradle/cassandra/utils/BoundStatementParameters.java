package com.exactpro.cradle.cassandra.utils;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;



public class BoundStatementParameters {
    private Select select;
    private UUID instanceId;
    private LocalDate startDate;
    private LocalTime timeFrom;
    private String idFrom;
    private LocalTime timeTo;
    private String parentId;
    private Function<BoundStatementBuilder, BoundStatementBuilder> attributes;
    public BoundStatementParameters(Select select,
                                    UUID instanceId,
                                    LocalDate startDate,
                                    LocalTime timeFrom,
                                    String idFrom,
                                    LocalTime timeTo,
                                    String parentId,
                                    Function<BoundStatementBuilder, BoundStatementBuilder> attributes){
        this.select = select;
        this.instanceId = instanceId;
        this.startDate = startDate;
        this.timeFrom = timeFrom;
        this.idFrom = idFrom;
        this.timeTo = timeTo;
        this.parentId = parentId;
        this.attributes = attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BoundStatementParameters)) return false;
        BoundStatementParameters that = (BoundStatementParameters) o;
        return Objects.equals(select, that.select) && Objects.equals(instanceId, that.instanceId) && Objects.equals(startDate, that.startDate) && Objects.equals(timeFrom, that.timeFrom) && Objects.equals(idFrom, that.idFrom) && Objects.equals(timeTo, that.timeTo) && Objects.equals(parentId, that.parentId) && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(select, instanceId, startDate, timeFrom, idFrom, timeTo, parentId, attributes);
    }
}
