/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dodo.worker;

import dodo.executors.TaskExecutor;
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
    public void executeTask(Map<String, Object> parameters, Map<String, Object> results) throws Exception {
        System.out.println("executeTask: parameters=" + parameters + " results=" + results);
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine groovy = manager.getEngineByName("groovy");
        groovy.put("parameters", parameters);
        groovy.put("results", results);
        String code = parameters.get("code") + "";
        groovy.eval(code);
        System.out.println("executeTask finished: parameters=" + parameters + " results=" + results);

    }

}
