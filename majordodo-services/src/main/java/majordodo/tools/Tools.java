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
package majordodo.tools;

import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import majordodo.network.BrokerHostData;
import majordodo.replication.LedgersInfo;
import majordodo.replication.ReplicatedCommitLog;
import majordodo.task.Broker;
import majordodo.task.FileCommitLog;
import majordodo.task.LogSequenceNumber;
import majordodo.task.StatusChangesLog;
import majordodo.task.StatusEdit;

/**
 * Utility for dumping a Replicated Commit Log to disk for debugging purposes
 *
 * @author enrico.olivelli
 */
public class Tools {

    public static void main(String... args) {
        try {
            Properties configuration = new Properties();
            if (args.length < 2) {
                System.out.println("Usage: tools.sh dumplogs|clear [LEDGERID] [OUTPUTFILE]");
                return;
            }
            File configFile = new File(args[0]);
            try (FileReader reader = new FileReader(configFile)) {
                configuration.load(reader);
            }
            String command = args[1];
            long ledgerId = -1;
            String outputfilename = "";
            switch (command) {
                case "dumplogs":
                    if (args.length > 2) {
                        ledgerId = Long.parseLong(args[2]);
                        if (args.length > 3) {
                            outputfilename = args[3];
                        }
                    }
                    break;
                case "clear":
                    break;
                default:
                    throw new RuntimeException("bad command " + command + ", only dumplogs|clear");
            }

            String sharedsecret = configuration.getProperty("sharedsecret", "dodo");
            String clusteringmode = configuration.getProperty("clustering.mode", "singleserver");
            String host = configuration.getProperty("broker.host", "127.0.0.1");
            int port = Integer.parseInt(configuration.getProperty("broker.port", "7363"));
            boolean ssl = Boolean.parseBoolean(configuration.getProperty("broker.ssl", "true"));
            StatusChangesLog log;
            switch (clusteringmode) {
                case "singleserver": {
                    String logsdir = configuration.getProperty("logs.dir", "txlog");
                    String snapdir = configuration.getProperty("data.dir", "data");
                    long maxFileSize = Long.parseLong(configuration.getProperty("logs.maxfilesize", (1024 * 1024) + ""));
                    log = new FileCommitLog(Paths.get(snapdir), Paths.get(logsdir), maxFileSize);
                    break;
                }
                case "clustered": {
                    String zkAddress = configuration.getProperty("zk.address", "localhost:1281");
                    int zkSessionTimeout = Integer.parseInt(configuration.getProperty("zk.sessiontimeout", "40000"));
                    String zkPath = configuration.getProperty("zk.path", "/majordodo");
                    String snapdir = configuration.getProperty("data.dir", "data");

                    Map<String, String> additional = new HashMap<>();
                    additional.put("tools-jvmid", ManagementFactory.getRuntimeMXBean().getName());
                    additional.put("tools-command", command);
                    ReplicatedCommitLog _log = new ReplicatedCommitLog(zkAddress, zkSessionTimeout, zkPath, Paths.get(snapdir),
                            BrokerHostData.formatHostdata(new BrokerHostData(host, port, Broker.VERSION(), ssl, additional))
                    );
                    log = _log;
                    int ensemble = Integer.parseInt(configuration.getProperty("bookkeeper.ensemblesize", _log.getEnsemble() + ""));
                    int writeQuorumSize = Integer.parseInt(configuration.getProperty("bookkeeper.writequorumsize", _log.getWriteQuorumSize() + ""));
                    int ackQuorumSize = Integer.parseInt(configuration.getProperty("bookkeeper.ackquorumsize", _log.getAckQuorumSize() + ""));
                    long ledgersRetentionPeriod = Long.parseLong(configuration.getProperty("bookkeeper.ledgersretentionperiod", _log.getLedgersRetentionPeriod() + ""));
                    _log.setAckQuorumSize(ackQuorumSize);
                    _log.setEnsemble(ensemble);
                    _log.setLedgersRetentionPeriod(ledgersRetentionPeriod);
                    _log.setWriteQuorumSize(writeQuorumSize);
                    _log.setSharedSecret(sharedsecret);
                    break;
                }
                default:
                    throw new RuntimeException("bad value for clustering.mode property, only valid values are singleserver|clustered");
            }
            try {
                if (command.equals("dumplogs")) {
                    if (log instanceof ReplicatedCommitLog) {
                        ReplicatedCommitLog _log = (ReplicatedCommitLog) log;
                        LedgersInfo actualLedgersList = _log.getClusterManager().getActualLedgersList();
                        System.out.println("Actual ledgers list:" + actualLedgersList.getActiveLedgers());
                    }
                    if (ledgerId >= 0) {
                        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss.SSS");
                        if (!outputfilename.isEmpty()) {
                            System.out.println("Dumping contents of ledger " + ledgerId + " to file " + outputfilename);
                            try (PrintWriter writer = new PrintWriter(new File(outputfilename))) {
                                log.recovery(new LogSequenceNumber(ledgerId, 0), (LogSequenceNumber t, StatusEdit u) -> {
                                    writer.println(t.ledgerId + "," + t.sequenceNumber + ',' + u.toFormattedString(formatter));
                                }, false);
                            }
                        } else {
                            System.out.println("Dumping contents of ledger " + ledgerId + " to stdout");
                            log.recovery(new LogSequenceNumber(ledgerId, 0), (LogSequenceNumber t, StatusEdit u) -> {
                                System.out.println(t.ledgerId + "," + t.sequenceNumber + ',' + u.toFormattedString(formatter));
                            }, false);
                        }
                    }
                } else if (command.equals("clear")) {
                    log.clear();
                }
            } finally {
                log.close();
            }

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
