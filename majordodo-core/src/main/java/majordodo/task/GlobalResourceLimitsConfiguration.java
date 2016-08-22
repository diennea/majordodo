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

import java.util.Map;

/**
 * Defines a global resource usage limits, to be applied to the whole system. As
 * resource is for instance a DBMS and you want to limit the overall number of
 * active connections to it
 *
 * @author enrico.olivelli
 */
public interface GlobalResourceLimitsConfiguration {

    /**
     * Define for each resource the actual maximunm number of task which use
     * that resource and is running (ASSIGNED to a worker)
     *
     * @return
     */
    public Map<String, Integer> getGlobalResourceLimits();
}
