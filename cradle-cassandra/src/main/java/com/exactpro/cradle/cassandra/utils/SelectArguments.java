package com.exactpro.cradle.cassandra.utils;

import com.exactpro.cradle.Order;

import java.util.Objects;

public class SelectArguments {
    private boolean includeContent;
    private String idFrom;
    private String parentId;
    private Order order;
    private boolean idFromPresents;
    private boolean parentIdPresents;
    private boolean orderPresents;
    public SelectArguments(boolean includeContent, String idFrom, String parentId, Order order){
        this.includeContent = includeContent;
        this.idFrom = idFrom;
        if(idFrom != null){
            idFromPresents = true;
        }
        this.parentId = parentId;
        if(parentId != null){
            parentIdPresents = true;
        }
        this.order = order;
        if(order != null){
            orderPresents = true;
        }
    }

    public boolean getIncludeContent() { return includeContent; }
    public String getIdFrom() { return idFrom; }
    public String getParentId() { return parentId; }
    public Order getOrder() { return order; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SelectArguments)) return false;
        SelectArguments arguments = (SelectArguments) o;
        return includeContent == arguments.includeContent
                && idFromPresents == arguments.idFromPresents
                && parentIdPresents == arguments.parentIdPresents
                && orderPresents == arguments.orderPresents;
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeContent, idFromPresents, parentIdPresents, orderPresents);
    }
}
