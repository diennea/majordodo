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
package majordodo.worker;

import majordodo.executors.TaskExecutor;
import java.util.Map;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

/**
 * Executors Groovy scripts
 *
 * @author enrico.olivelli
 */
class GroovyScriptTaskExecutor extends TaskExecutor {

    public GroovyScriptTaskExecutor(Map<String, Object> parameters) {
    }

    @Override
    public String executeTask(Map<String, Object> parameters) throws Exception {
        System.out.println("executeTask: parameters=" + parameters);
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine groovy = manager.getEngineByName("groovy");
        groovy.put("parameters", parameters);
        String code = parameters.get("code") + "";
        Object res = groovy.eval(code);
        String result = res != null ? res.toString() : "";
        System.out.println("executeTask finished: parameters=" + parameters + " result=" + result);
        return result;
    }

}
