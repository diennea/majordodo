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

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import majordodo.testclients.SimpleExecutor;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for CodePoolUtils
 *
 * @author enrico.olivelli
 */
public class CodePoolUtilsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void packMeTest() throws Exception {
        byte[] zip = CodePoolUtils.createCodePoolDataFromClass(CodePoolUtilsTest.class);        
        Set<String> entries = new HashSet<>();
        try (ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(zip));) {
            ZipEntry nextEntry;
            while ((nextEntry = in.getNextEntry()) != null) {
                if (nextEntry.isDirectory()) {
                    System.out.println("dir  " + nextEntry.getName());
                } else {
                    System.out.println("file  " + nextEntry.getName());
                    entries.add(nextEntry.getName());
                }
            }
        }
        String entry = entries.stream().filter(s -> s.startsWith("generated_") && s.endsWith(".jar")).findFirst().orElse(null);
        assertTrue(entry != null);
        
//        assertTrue(entries.contains("majordodo/client/CodePoolUtilsTest.class"));

        Path directory = folder.newFolder().toPath();
        CodePoolUtils.unzipCodePoolData(directory, zip);
        assertTrue(Files.isRegularFile(directory.resolve(entry)));
    }

    @Test
    public void packFromJarTest() throws Exception {
        byte[] zip = CodePoolUtils.createCodePoolDataFromClass(SimpleExecutor.class);
        Set<String> entries = new HashSet<>();
        try (ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(zip));) {
            ZipEntry nextEntry;
            while ((nextEntry = in.getNextEntry()) != null) {
                if (nextEntry.isDirectory()) {
                    System.out.println("dir  " + nextEntry.getName());
                } else {
                    System.out.println("file  " + nextEntry.getName());
                    entries.add(nextEntry.getName());
                }
            }
        }
        
        // invoking the test from Maven in single project mode the class is loaded from the local repository
        boolean ok1 = entries.stream().filter(s -> s.startsWith("majordodo-test-clients-") && s.endsWith(".jar")).findFirst().isPresent();
        // invoking the test from Maven Reactor puts into the classpath the "target/classes" directory of majordodo-test-clients
        boolean ok2 = entries.stream().filter(s -> s.startsWith("generated-") && s.endsWith(".jar")).findFirst().isPresent();
        assertTrue(ok1 || ok2);
    }
}
