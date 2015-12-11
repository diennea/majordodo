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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Classloader with loads classes from a given CodePool
 *
 * @author enrico.olivelli
 */
public final class CodePoolClassloader extends URLClassLoader {

    private final Path directory;

    public CodePoolClassloader(ClassLoader parent, CodePool codePool, Path tmpDirectory) throws IOException {
        super(new URL[0], parent);
        directory = tmpDirectory.resolve(codePool.getId());
        buildCodePoolTmpDirectory(codePool);
    }

    private void buildCodePoolTmpDirectory(CodePool codePool) throws IOException {
        ZipInputStream zip = new ZipInputStream(new ByteArrayInputStream(codePool.getCodePoolData()));
        ZipEntry nextEntry = zip.getNextEntry();
        while (nextEntry != null) {
            if (!nextEntry.isDirectory()) {
                String filename = nextEntry.getName();
                Path file = directory.resolve(filename);
                try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.TRUNCATE_EXISTING)) {
                    long size = nextEntry.getSize();
                    long written = copyStreams(zip, out);
                    if (size != written) {
                        throw new IOException("error while unzipping " + file.toAbsolutePath());
                    }
                }
                addURL(file.toUri().toURL());
            }
            nextEntry = zip.getNextEntry();
        }
    }

    @Override
    public void close() throws IOException {
        deleteDirectory(directory);
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
