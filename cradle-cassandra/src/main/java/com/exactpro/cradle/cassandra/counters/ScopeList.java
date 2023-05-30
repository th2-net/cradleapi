/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.counters;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class ScopeList {
    private final HashSet<String> scopeSet;
    private final String scope;

    // ScopeList will store single scope in String field
    // and for multiple scopes will utilize Set
    public ScopeList(String scope) {
        this.scopeSet = null;
        this.scope = scope;
    }

    private ScopeList(HashSet<String> scopeSet) {
        this.scopeSet = scopeSet;
        this.scope = null;
    }

    private static void addScopeList(HashSet<String> set, ScopeList scopeList) {
        if (scopeList.scopeSet != null) {
            set.addAll(scopeList.scopeSet);
        } else {
            set.add(scopeList.scope);
        }
    }

    public Collection<String> getScopes() {
        if (scope != null) {
            return List.of(scope);
        }
        return scopeSet;
    }

    public ScopeList mergedWith(ScopeList scopeList) {
        if (containsAll(scopeList)) {
            return this;
        }

        HashSet<String> set;
        if (this.scopeSet != null) {
            set = (HashSet<String>) scopeSet.clone();
        } else {
            set = new HashSet<>();
            set.add(this.scope);
        }
        addScopeList(set, scopeList);

        return new ScopeList(set);
    }

    public boolean containsAll(ScopeList other) {
        // if we hold single scope
        if (this.scope != null) {
            if (this.scope.equals(other.scope)) {
                return true;
            }
            return other.scopeSet != null && other.scopeSet.size() == 1 && other.scopeSet.contains(this.scope);
        } else {
            // if we hold multiple scope
            if (other.scope != null) {
                return this.scopeSet.contains(other.scope);
            }
            return this.scopeSet.containsAll(other.scopeSet);
        }
    }
}