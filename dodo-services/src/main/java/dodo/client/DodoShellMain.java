/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dodo.client;

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
