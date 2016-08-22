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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

import majordodo.utils.DiscardingBoundedPriorityQueue;
import majordodo.utils.IntCounter;

/**
 * Chooses tasks
 *
 * @author enrico.olivelli
 */
public final class TasksChooser {

    private final List<Integer> groups;
    private final Set<Integer> excludedGroups;
    private final Map<Integer, Integer> availableSpace;
    private final Map<Integer, IntCounter> availableResourcesCounters;
    private final Map<Integer, Integer> priorityByGroup = new HashMap<>();
    private final boolean matchAllGroups;
    private final int max;
    private final Map<Integer, PriorityQueue<Entry>> bestbyTasktype = new HashMap<>();
    private final PriorityQueue<Entry> matchAllTypesQueue;
    private final Integer availableSpaceForAnyTask;

    public static final class Entry implements Comparable<Entry> {

        /**
         * Natural ordering comparator
         */
        public static final Comparator<Entry> STANTARD_COMPARATOR = new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {

                return o1.compareTo(o2);

            }

        };

        /**
         * Inverse ordering comparator
         */
        public static final Comparator<Entry> INVERSE_COMPARATOR = new Comparator<Entry>() {

            @Override
            public int compare(Entry o1, Entry o2) {

                return o2.compareTo(o1);

            }

        };

        final int position;
        final long taskid;
        final int priorityByGroup;
        final int[] resources;

        public Entry(int position, long taskid, int priorityByGroup, int[] resources) {
            this.position = position;
            this.taskid = taskid;
            this.priorityByGroup = priorityByGroup;
            this.resources = resources;
        }

        @Override
        public int hashCode() {
            return position;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            final Entry other = (Entry) obj;
            if (this.position != other.position) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "Entry{" + "position=" + position + ", taskid=" + taskid + ", priorityByGroup=" + priorityByGroup + '}';
        }

        /**
         * {@inheritDoc}
         *
         * Entries with less priority are <i>smaller</i>. On ties newer entries
         * (bigger position) are <i>smaller</i>
         */
        @Override
        public int compareTo(Entry o) {

            int diff = this.priorityByGroup - o.priorityByGroup;
            if (diff != 0) {
                return diff;
            }
            if (this.position < o.position) {
                return 1;
            } else {
                return -1;
            }
        }

    }

    TasksChooser(List<Integer> groups, Set<Integer> excludedGroups, Map<Integer, Integer> availableSpace, Map<Integer, IntCounter> availableResourcesCounters, int max) {
        this.availableSpace = new HashMap<>(availableSpace);
        this.groups = groups;
        this.availableResourcesCounters = availableResourcesCounters;
        this.excludedGroups = excludedGroups;
        this.max = max;

        /*
		 * Bonded priority queues will be used. each add will request log(n)
		 * operations but n represent maximum task number for a type (enough
		 * small) and not all existing tasks (possibly really big).
         */
        availableSpace.entrySet().stream().forEach((entry) -> {
            if (entry.getKey() > 0) {
                bestbyTasktype.put(entry.getKey(), new DiscardingBoundedPriorityQueue<Entry>(entry.getValue()));
            }
        });

        availableSpaceForAnyTask = availableSpace.get(0);
        this.matchAllGroups = groups.contains(Task.GROUP_ANY);
        int priority = groups.size();
        for (int idgroup : groups) {
            this.priorityByGroup.put(idgroup, priority--);
        }
        if (availableSpaceForAnyTask != null) {
            matchAllTypesQueue = new DiscardingBoundedPriorityQueue<Entry>(availableSpaceForAnyTask);
        } else {
            matchAllTypesQueue = null;
        }

    }

    public List<Entry> getChoosenTasks() {

        final List<Entry> result = new ArrayList<>();

        bestbyTasktype.values().forEach(result::addAll);

        if (matchAllTypesQueue != null) {
            result.addAll(matchAllTypesQueue);
        }

        if (result.size() > 1) {
            result.sort(Entry.INVERSE_COMPARATOR);
        }

        if (!availableResourcesCounters.isEmpty()) {
            List<Entry> newResult = new ArrayList<>();
            int acceptedCount = 0;
            for (Entry entry : result) {
                // an entry can be accepted only if there is space for every declared resource                
                if (entry.resources == null) {
                    newResult.add(entry);
                    acceptedCount++;
                } else {
                    boolean allOk = true;
                    for (int idresource : entry.resources) {
                        IntCounter spaceForResource = availableResourcesCounters.get(idresource);
                        if (spaceForResource != null && spaceForResource.count <= 0) {
                            allOk = false;
                            break;
                        }
                    }
                    if (allOk) {
                        for (int idresource : entry.resources) {
                            IntCounter spaceForResource = availableResourcesCounters.get(idresource);
                            if (spaceForResource != null) {
                                spaceForResource.count--;
                            }
                        }
                        newResult.add(entry);
                        acceptedCount++;
                    }
                }
                if (acceptedCount >= max) {
                    break;
                }
            }
            return newResult;
        } else {

            return result.size() > max ? result.subList(0, max) : result;
        }

    }

    private static final Logger LOGGER = Logger.getLogger(TasksChooser.class.getName());

    void accept(int position, TasksHeap.TaskEntry entry) {

        final int idgroup = entry.groupid;

        if ((matchAllGroups && !excludedGroups.contains(idgroup)) || groups.contains(idgroup)) {

            int tasktype = entry.tasktype;

            Integer availableSpaceForTaskType = availableSpace.get(tasktype);

            if (availableSpaceForTaskType == null) {
                availableSpaceForTaskType = availableSpaceForAnyTask;
            }

            if (availableSpaceForTaskType != null) {

                Queue<Entry> queue;
                Queue<Entry> bytasktype = bestbyTasktype.get(tasktype);

                /*
				 * If availableSpaceForTaskType is not null bytasktype or
				 * matchAllTypesQueue aren't null... so queue is not null
                 */
                queue = bytasktype != null ? bytasktype : matchAllTypesQueue;

                Integer priority = priorityByGroup.get(idgroup);

                // possibile if using "matchAllGroups"
                if (priority == null) {
                    priority = Integer.MIN_VALUE;
                }

                queue.add(new Entry(position, entry.taskid, priority, entry.resources));

            }
        }
    }

}
