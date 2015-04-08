/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dodo.client;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import jline.console.ConsoleReader;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Main shell
 *
 * @author enrico
 */
public class DodoShell {

    private final String host;
    private final int port;
    private ConsoleReader reader;

    public DodoShell(String host, int port) {
        this.host = host;
        this.port = port;

    }

    private void runCommand(String command) throws Exception {
        String[] split = command.split(" ");
        if (split.length == 0 || command.trim().isEmpty()) {
            return;
        }
        String cmd = split[0];
        if (cmd.equals("tasks")) {
            executeTaskList();
        } else if (cmd.equals("workers")) {
            executeWorkersList();
        } else if (cmd.equals("version")) {
            executeVersion();
        } else if (cmd.equals("task-run-script")) {
            if (split.length != 4) {
                reader.println("usage task-run-script script.groovy QUEUENAME TAG");
                return;
            }
            executeTaskRunScript(split[1], split[2], split[3]);
        } else {
            write("error: no such command " + cmd);
        }
    }

    private void executeTaskList() throws Exception {
        try {
            String result = request("GET", null, "http://" + host + ":" + port + "/client/tasks");
            write("result:" + result);
        } catch (Exception err) {
            err.printStackTrace();
            write("err:" + err);
        }
    }

    private void executeWorkersList() throws Exception {
        try {
            String result = request("GET", null, "http://" + host + ":" + port + "/client/workers");
            write("result:" + result);
        } catch (Exception err) {
            err.printStackTrace();
            write("err:" + err);
        }
    }

    private void executeVersion() throws Exception {
        try {
            String result = request("GET", null, "http://" + host + ":" + port + "/client/version");
            write("result:" + result);
        } catch (Exception err) {
            err.printStackTrace();
            write("err:" + err);
        }
    }

    private void write(Object msg) {
        try {
            reader.getOutput().write(msg + "\r\n");
        } catch (Throwable t) {
        }
    }

    private String request(String method, Map<String, Object> data, String url) throws Exception {
        URL _url = new URL(url);
        HttpURLConnection con = (HttpURLConnection) _url.openConnection();
        try {
            //System.out.println("reqeust:" + method + " " + data + ", to " + url);
            con.setRequestMethod(method);
            if (method.equals("POST")) {
                con.setDoOutput(true);
                con.setRequestProperty("Content-Type", "application/json;charset=utf-8");;
                ObjectMapper mapper = new ObjectMapper();
                String s = mapper.writeValueAsString(data);
                //System.out.println("data:" + s);
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                con.setRequestProperty("Content-Length", bytes.length + "");
                con.getOutputStream().write(bytes);
            }
            return IOUtils.toString(con.getInputStream(), StandardCharsets.UTF_8);
        } catch (Exception err) {
            write("ERROR: " + err + "\r\n");
            if (con.getErrorStream() != null) {
                return IOUtils.toString(con.getErrorStream(), StandardCharsets.UTF_8);
            } else {
                return err + "";
            }
        } finally {
            con.disconnect();
        }
    }

    public void run() throws Exception {
        reader = new ConsoleReader();
        reader.setPrompt("admin@" + ANSIUtils.color(host + ":" + port, ANSIUtils.ANSI_GREEN) + ">");

        String line = reader.readLine();

        while (line != null) {
            write("line: " + line + "\r\n");
            if (line.equals("quit") || line.equals("q") || line.equals("exit")) {
                break;
            }
            runCommand(line);
            line = reader.readLine();
        }
    }

    private void executeTaskRunScript(String scriptname, String queuename, String tag) {
        try {
            Map<String, Object> taskParams = new HashMap<>();
            taskParams.put("type", "script");
            taskParams.put("queueName", queuename);
            taskParams.put("tag", tag);
            Map<String, Object> pp = new HashMap<>();
            File codefile = new File(scriptname);
            byte[] content = Files.readAllBytes(codefile.toPath());
            pp.put("code", new String(content, StandardCharsets.UTF_8));
            taskParams.put("parameters", pp);
            String result = request("POST", taskParams, "http://" + host + ":" + port + "/client/tasks");
            write("result:" + result);
        } catch (Exception err) {
            err.printStackTrace();
            write("err:" + err);
        }
    }

}
