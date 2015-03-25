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
package dodo.clustering;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * A queue of tasks
 *
 * @author enrico.olivelli
 */
public class TaskQueue {

    public static final String DEFAULT_TAG = "default";

    private final Queue<Task> tasks = new ArrayDeque<>();
    private final String tag;
    private final String name;

    @Override
    public String toString() {
        return "TaskQueue{" + "tag=" + tag + ", name=" + name + '}';
    }

    public TaskQueue(String tag, String name) {
        this.tag = tag;
        this.name = name;
    }

    public String getTag() {
        return tag;
    }

    void addNewTask(Task task) {
        tasks.add(task);
    }

    public Task peekNext() {
        return tasks.peek();
    }

    public Task removeNext(long expectedTaskId) {
        Task t = tasks.poll();        
        if (t == null || t.getTaskId() != expectedTaskId) {
            throw new RuntimeException();
        }
        return t;
    }

}
