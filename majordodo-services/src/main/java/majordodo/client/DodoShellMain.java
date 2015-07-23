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
package majordodo.client;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

/**
 * Main
 *
 * @author enrico
 */
public class DodoShellMain {

    public static void main(String... args) {
        try {
            Options options = new Options();
            options.addOption("h", "broker.host", true, "broker host (default localhost)");
            options.addOption("p", "broker.port", true, "broker port (default 7364)");
            CommandLineParser parser = new BasicParser();
            CommandLine cmd = parser.parse(options, args);
            int port = Integer.parseInt(cmd.getOptionValue('p', "7364"));
            String host = cmd.getOptionValue("h", "localhost");
            DodoShell shell = new DodoShell(host, port);
            shell.run();

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
