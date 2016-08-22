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

import java.util.Collections;
import java.util.Map;

/**
 * Simple configuration based on a Map
 *
 * @author enrico.olivelli
 */
public class MapGlobalResourceLimitsConfiguration implements GlobalResourceLimitsConfiguration {

    private Map<String, Integer> map;

    public MapGlobalResourceLimitsConfiguration(Map<String, Integer> map) {
        this.map = map != null ? Collections.unmodifiableMap(map) : Collections.emptyMap();
    }

    public MapGlobalResourceLimitsConfiguration() {
        map = Collections.emptyMap();
    }

    public Map<String, Integer> getConfiguration() {
        return map;
    }

    public void setConfiguration(Map<String, Integer> map) {
        this.map = map != null ? Collections.unmodifiableMap(map) : Collections.emptyMap();
    }

    @Override
    public Map<String, Integer> getGlobalResourceLimits() {
        return map;
    }

}
