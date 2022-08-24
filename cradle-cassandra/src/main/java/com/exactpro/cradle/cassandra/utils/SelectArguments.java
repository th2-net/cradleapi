/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
                && orderPresents == arguments.orderPresents
                && order == arguments.order;
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeContent, idFromPresents, parentIdPresents, orderPresents);
    }
}
