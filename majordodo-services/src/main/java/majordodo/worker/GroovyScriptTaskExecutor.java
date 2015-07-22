/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
