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
package majordodo.codepools;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import majordodo.client.CodePoolUtils;

/**
 * Classloader with loads classes from a given CodePool
 *
 * @author enrico.olivelli
 */
public final class CodePoolClassloader extends URLClassLoader {

    private static final Logger LOGGER = Logger.getLogger(CodePoolClassloader.class.getName());
    private final Path directory;

    public CodePoolClassloader(ClassLoader parent, String codePoolId, byte[] data, Path tmpDirectory) throws IOException {
        super(new URL[0], parent);
        directory = tmpDirectory.resolve(codePoolId);
        deleteDirectory(directory);
        buildCodePoolTmpDirectory(codePoolId, data);
    }

    private void buildCodePoolTmpDirectory(String codePoolId, byte[] data) throws IOException {
        LOGGER.log(Level.SEVERE, "Unzipping codepool data for pool " + codePoolId + " to " + directory.toAbsolutePath());
        List<URL> unzipCodePoolData = CodePoolUtils.unzipCodePoolData(directory, data);
        unzipCodePoolData.forEach(this::addURL);
        LOGGER.log(Level.SEVERE, "Classpath for " + codePoolId + ": " + Arrays.toString(this.getURLs()));

    }

    @Override
    public void close() throws IOException {
        deleteDirectory(directory);
    }

    private static class FileDeleter extends SimpleFileVisitor<Path> {

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
//            println("delete directory " + dir);
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
        }
    }

    public static void deleteDirectory(Path f) throws IOException {
        if (Files.isDirectory(f)) {
            Files.walkFileTree(f, new FileDeleter());
            Files.deleteIfExists(f);
        } else if (Files.isRegularFile(f)) {
            throw new IOException("name " + f.toAbsolutePath() + " is not a directory");
        }
    }

}
