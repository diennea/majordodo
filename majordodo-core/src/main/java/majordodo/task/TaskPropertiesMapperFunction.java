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

/**
 * Function which maps a task to a group of user and a set of resources which will be used by the task. This function must be very
 * fast, usually the implementation will be a HashMap lookup. This function must
 * be trehead safe must it should never impose any lock.
 * Beware that the set of resources used by a task is dynamic and can change within the interval of time from the submission of the task, the first execution and further executions.
 *
 * @author enrico.olivelli
 */
@FunctionalInterface
public interface TaskPropertiesMapperFunction {

    public TaskProperties getTaskProperties(long taskid, String taskType, String userid);
}
