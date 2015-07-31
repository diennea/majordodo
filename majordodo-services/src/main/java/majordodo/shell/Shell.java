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
package majordodo.shell;

import java.util.List;
import static org.codehaus.groovy.runtime.DefaultGroovyMethods.println;
import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Groovysh;

/**
 * Main shell
 *
 * @author enrico.olivelli
 */
public class Shell {

    public static class BarCommand extends CommandSupport {

        protected BarCommand(Groovysh shell) {
            super(shell, ":bar", ":b");
        }

        @Override
        public Object execute(List<String> args) {
            println("running bar command");
            return "test";
        }
    }

    public static void main(String... args) {
        Groovysh groovysh = new Groovysh();
        groovysh.register(new BarCommand(groovysh));
        groovysh.run((String) null);
    }
}
