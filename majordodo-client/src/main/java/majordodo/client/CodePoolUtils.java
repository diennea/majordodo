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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Utility for managing CodePools
 *
 * @author enrico.olivelli
 */
public class CodePoolUtils {

    /**
     * Encodes to base64 the given CodePool Data
     *
     * @param input
     * @return
     * @throws IOException
     */
    public static String encodeCodePoolData(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        copyStream(input, output);
        return Base64.getEncoder().encodeToString(output.toByteArray());
    }

    private static void copyStream(InputStream input, OutputStream output) throws IOException {
        int n = 0;
        byte[] buffer = new byte[64 * 1024];
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
    }

    /**
     * Serializes an Executor for execution with
     * {@link  TaskModeAwareExecutorFactory}
     *
     * @param executor
     * @return
     * @throws Exception
     */
    public static String serializeExecutor(Object executor) throws Exception {
        if (executor instanceof Class) {
            Class c = (Class) executor;
            return "newinstance:" + c.getName();
        } else {
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(oo);
            os.writeUnshared(executor);
            os.close();
            return "base64:" + Base64.getEncoder().encodeToString(oo.toByteArray());
        }
    }

    public static byte[] createZipWithOneEntry(String entryfilename, byte[] filedata) throws IOException {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try (ZipOutputStream zipper = new ZipOutputStream(oo, StandardCharsets.UTF_8);) {
            int posslash = entryfilename.indexOf('/');
            if (posslash >= 0) { // simple case for directory
                String dire = entryfilename.substring(0, posslash);
                ZipEntry entry = new ZipEntry(dire);
                zipper.putNextEntry(entry);
                zipper.closeEntry();
            }
            ZipEntry entry = new ZipEntry(entryfilename);
            zipper.putNextEntry(entry);
            zipper.write(filedata);
            zipper.closeEntry();
        }
        return oo.toByteArray();
    }

    /**
     * Packs bytecode in a CodePool
     *
     * @param klass
     * @return
     * @throws IOException
     */
    public static byte[] createCodePoolDataFromClass(Class klass) throws IOException {

        String res = '/' + klass.getName().replace('.', '/') + ".class";
        URL location = klass.getResource(res);
        System.out.println("createCodePoolDataFromClass "+klass+" -> "+location);
        String slocation = location.toString();        
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try (ZipOutputStream zoo = new ZipOutputStream(oo)) {
            if (slocation.startsWith("jar:file:")) {
                URL locationbase = klass.getResource("/");
                int end = slocation.lastIndexOf('!');
                int start = slocation.lastIndexOf("/", end);
                String jarName = slocation.substring(start + 1, end);
                ZipEntry entry = new JarEntry(jarName);
                zoo.putNextEntry(entry);
                try (InputStream in = locationbase.openStream()) {
                    copyStream(in, zoo);
                }
                zoo.closeEntry();
            } else if (slocation.startsWith("file:/")) {
                URL locationbase = klass.getResource("/");
                // package all the classes in the directory  
                String slocationbase = locationbase.toString();
                Path directory = Paths.get(slocationbase.substring("file:".length()));                
                int skip = slocationbase.length() - 6;
                addFileToZip(skip, directory.toFile(), zoo);
            }
        }
        byte[] resb = oo.toByteArray();
//        Files.write(Paths.get("debug.jar"), resb);
        return resb;
    }

    private static void addFileToZip(int skipprefix, File file, ZipOutputStream zipper) throws IOException {
        String raw = file.getAbsolutePath().replace("\\", "/");
        if (raw.length() == skipprefix) {
            if (file.isDirectory()) {
                for (File child : file.listFiles()) {
                    addFileToZip(skipprefix, child, zipper);
                }
            }
        } else {
            String path = raw.substring(skipprefix + 1);
            if (file.isDirectory()) {
                for (File child : file.listFiles()) {
                    addFileToZip(skipprefix, child, zipper);
                }
            } else {                
                ZipEntry entry = new ZipEntry(path);
                zipper.putNextEntry(entry);
                try (FileInputStream in = new FileInputStream(file)) {
                    copyStream(in, zipper);
                }
                zipper.closeEntry();
            }
        }

    }

    private static long copyStreams(InputStream input, OutputStream output) throws IOException {
        long count = 0;
        int n = 0;
        byte[] buffer = new byte[60 * 1024];
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    public static List<URL> unzipCodePoolData(Path directory, byte[] data) throws IOException {
        List<URL> urls = new ArrayList<>();
        ZipInputStream zip = new ZipInputStream(new ByteArrayInputStream(data));
        ZipEntry nextEntry = zip.getNextEntry();
        while (nextEntry != null) {
            if (!nextEntry.isDirectory()) {
                String filename = nextEntry.getName();                
                Path file = directory.resolve(filename);                
                Files.createDirectories(file.getParent());

                try (OutputStream out = Files.newOutputStream(file)) {
                    copyStreams(zip, out);
                }
                urls.add(file.toUri().toURL());
            }
            nextEntry = zip.getNextEntry();
        }
        return urls;
    }

}
