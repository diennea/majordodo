/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dodo.broker;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

public class BrokerClientTest {

    @Test
    public void test() throws Exception {       
        try (BrokerMain main = new BrokerMain(new Properties());) {
            main.start();
            {
                Map<String, Object> taskParams = new HashMap<>();
                taskParams.put("type","test");
                taskParams.put("queueName","testQueue");
                taskParams.put("tag","testTag");
                Map<String,Object> pp = new HashMap<>();
                pp.put("param1","test1");
                pp.put("param2",20);
                taskParams.put("parameters",pp);
                String result = request("POST", taskParams, "http://localhost:7364/client/tasks");
                System.out.println("result2:" + result);
            }
            {
                String result = request("GET", null, "http://localhost:7364/client/tasks");
                System.out.println("result1:" + result);
            }
        }

    }

    private static String request(String method, Map<String, Object> data, String url) throws Exception {
        URL _url = new URL(url);
        HttpURLConnection con = (HttpURLConnection) _url.openConnection();
        try {
            System.out.println("reqeust:" + method + " " + data + ", to " + url);
            con.setRequestMethod(method);
            if (method.equals("POST")) {
                con.setDoOutput(true);
                con.setRequestProperty("Content-Type", "application/json;charset=utf-8");;
                ObjectMapper mapper = new ObjectMapper();
                String s = mapper.writeValueAsString(data);
                System.out.println("data:" + s);
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                con.setRequestProperty("Content-Length", bytes.length + "");
                con.getOutputStream().write(bytes);
            }
            return IOUtils.toString(con.getInputStream(), StandardCharsets.UTF_8);
        } catch (Exception err) {
            err.printStackTrace();
            return IOUtils.toString(con.getErrorStream(), StandardCharsets.UTF_8);
        } finally {
            con.disconnect();
        }
    }
}
