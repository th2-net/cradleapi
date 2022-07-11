package com.exactpro.cradle.cassandra.utils;

import com.exactpro.cradle.Order;

import java.util.Objects;

public class SelectArguments {
    private boolean includeContent;
    private String idFrom;
    private String parentId;
    private Order order;
    public SelectArguments(boolean includeContent, String idFrom, String parentId, Order order){
        this.includeContent = includeContent;
        this.idFrom = idFrom;
        this.parentId = parentId;
        this.order = order;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SelectArguments)) return false;
        SelectArguments that = (SelectArguments) o;
        return includeContent == that.includeContent && Objects.equals(idFrom, that.idFrom) && Objects.equals(parentId, that.parentId) && order == that.order;
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeContent, idFrom, parentId, order);
    }
}
