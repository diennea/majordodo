/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package dodo.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An executor Node
 *
 * @author enrico.olivelli
 */
public final class Node {

    public static final int STATUS_ALIVE = 0;

    private String nodeId;
    private int status;
    private String nodeLocation;
    private Set<String> tags;
    private Map<String, Integer> maximumNumberOfTasks;
    private final Map<String, AtomicInteger> actualNumberOfTasks = new HashMap<>();

    public Map<String, AtomicInteger> getActualNumberOfTasks() {
        return actualNumberOfTasks;
    }

    public Map<String, Integer> getMaximumNumberOfTasks() {
        return maximumNumberOfTasks;
    }

    public void setMaximumNumberOfTasks(Map<String, Integer> maximumNumberOfTasks) {
        this.maximumNumberOfTasks = maximumNumberOfTasks;
    }

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

    public Node() {
    }

    public String getNodeLocation() {
        return nodeLocation;
    }

    public void setNodeLocation(String nodeLocation) {
        this.nodeLocation = nodeLocation;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

}
