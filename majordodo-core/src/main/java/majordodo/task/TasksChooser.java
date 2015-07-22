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
package majordodo.task;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Chooses tasks
 *
 * @author enrico.olivelli
 */
public class TasksChooser {

    private final List<Integer> groups;
    private final Map<Integer, Integer> availableSpace;
    private final Map<Integer, Integer> priorityByGroup = new HashMap<>();
    private final boolean matchAllGroups;
    private final int max;
    private final Map<Integer, List<Entry>> bestbyTasktype = new HashMap<>();
    private final List<Entry> matchAllTypesQueue;
    private final Integer availableSpaceForAnyTask;
    private int found;
    private final int fastStop;

    public static final class Entry {

        final int position;
        final long taskid;
        final int priorityByGroup;

        public Entry(int position, long taskid, int priorityByGroup) {
            this.position = position;
            this.taskid = taskid;
            this.priorityByGroup = priorityByGroup;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 43 * hash + this.position;
            hash = 43 * hash + (int) (this.taskid ^ (this.taskid >>> 32));
            hash = 43 * hash + this.priorityByGroup;
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Entry other = (Entry) obj;
            if (this.position != other.position) {
                return false;
            }
            if (this.taskid != other.taskid) {
                return false;
            }
            if (this.priorityByGroup != other.priorityByGroup) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "Entry{" + "position=" + position + ", taskid=" + taskid + ", priorityByGroup=" + priorityByGroup + '}';
        }

    }
    private static final Comparator<Entry> PRIORITY_COMPARATOR = new Comparator<Entry>() {

        @Override
        public int compare(Entry o1, Entry o2) {
            // order entires by priority and than by position, a lower position means an "older task", which it is best to take for first
            int diff = o1.priorityByGroup - o2.priorityByGroup;
            if (diff != 0) {
                return diff;
            }
            if (o1.position > o2.position) {
                return 1;
            } else {
                // please not that it is impossible that o1.taskid == o1.task
                return -1;
            }
        }

    };

    TasksChooser(List<Integer> groups, Map<Integer, Integer> availableSpace, int max) {
        this.availableSpace = new HashMap<>(availableSpace);
        this.groups = groups;
        this.max = max;
        availableSpace.keySet().stream().forEach((tasktype) -> {
            if (tasktype > 0) {
                bestbyTasktype.put(tasktype, new ArrayList<>());
            }
        });
        availableSpaceForAnyTask = availableSpace.get(0);
        this.matchAllGroups = groups.contains(Task.GROUP_ANY);
        int priority = groups.size();
        for (int idgroup : groups) {
            this.priorityByGroup.put(idgroup, priority--);
        }
        if (availableSpaceForAnyTask != null) {
            matchAllTypesQueue = new ArrayList<>();
        } else {
            matchAllTypesQueue = null;
        }

        int stopAt = max;
        for (int maxByTaskType : availableSpace.values()) {
            stopAt += maxByTaskType;
        }
        // limit the scan in order not to scan always the full heap
        fastStop = stopAt;
    }

    public List<Entry> getChoosenTasks() {
        List<Entry> result = new ArrayList<>();
        bestbyTasktype.values().forEach(result::addAll);
        if (matchAllTypesQueue != null) {
            result.addAll(matchAllTypesQueue);
        }
        if (result.size() == 1) {
            return result;
        }
        result.sort(PRIORITY_COMPARATOR);
        if (result.size() > max) {
            return result.subList(0, max);
        } else {
            return result;
        }
    }

    boolean accept(int position, TasksHeap.TaskEntry entry) {
        final int idgroup = entry.groupid;
        if (matchAllGroups || groups.contains(idgroup)) {
            int tasktype = entry.tasktype;
            Integer availableSpaceForTaskType = availableSpace.get(tasktype);
            if (availableSpaceForTaskType == null) {
                availableSpaceForTaskType = availableSpaceForAnyTask;
            }

            if (availableSpaceForTaskType != null) {
                List<Entry> queue;
                List<Entry> bytasktype = bestbyTasktype.get(tasktype);
                if (bytasktype != null) {
                    queue = bytasktype;
                } else {
                    queue = matchAllTypesQueue;
                }
                if (queue != null) {
                    Integer priority = priorityByGroup.get(idgroup);
                    if (priority == null) {
                        priority = Integer.MIN_VALUE; // possibile if using "matchAllGroups"
                    }
                    if (queue.size() < availableSpaceForTaskType) {  //TODO: use better implementation of bounded priority queue
                        queue.add(new Entry(position, entry.taskid, priority));
                        found++;
                        if (found == fastStop) {
                            return true;
                        }
                    }
                }
            }

        }
        return false;
    }

}
