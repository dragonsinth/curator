/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.DirectoryUtils;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.PathUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;

/**
 * An example of using TreeCache.  Mirrors the given ZK subtree onto the file system.
 */
public final class TreeFS
{

    private static void printHelp()
    {
        System.err.println("TreeFS -server host:port [-path path] [-out directory]\n");
        System.err.println("Mirrors a ZK tree or subtree onto the local filesystem (read only)\n");
        System.err.println("Options:");
        System.err.println("  -server: the zk server to connect to");
        System.err.println("  -path:   the zk path of the root node to mirror; default '/'");
        System.err.println("  -out:    the local directory to mirror into; default '.'");
        System.err.println("  -f:      recursively delete the output directory if not empty on startup");
        System.err.println("  -v:      verbose (log all events)");
        System.err.println("  -x:      exit after initial mirror is built");
        System.err.println();
        System.exit(1);
    }

    public static void main(String[] args) throws Exception
    {
        if ( args.length < 2 )
        {
            printHelp();
        }

        String server = null;
        String rootPath = "/";
        File outDir = new File(".");
        boolean forceClean = false;
        boolean verbose = false;
        boolean exit = false;
        Iterator<String> argIt = Arrays.asList(args).iterator();
        while ( argIt.hasNext() )
        {
            String arg = argIt.next();
            if ( "-server".equals(arg) )
            {
                if ( !argIt.hasNext() )
                {
                    System.err.println("Missing operand for argument: " + arg);
                    printHelp();
                }
                server = argIt.next();
            }
            else if ( "-path".equals(arg) )
            {
                if ( !argIt.hasNext() )
                {
                    System.err.println("Missing operand for argument: " + arg);
                    printHelp();
                }
                rootPath = argIt.next();
                PathUtils.validatePath(rootPath);
            }
            else if ( "-out".equals(arg) )
            {
                if ( !argIt.hasNext() )
                {
                    System.err.println("Missing operand for argument: " + arg);
                    printHelp();
                }
                outDir = new File(argIt.next());
            }
            else if ( "-f".equals(arg) )
            {
                forceClean = true;
            }
            else if ( "-v".equals(arg) )
            {
                verbose = true;
            }
            else if ( "-x".equals(arg) )
            {
                exit = true;
            }
            else
            {
                System.err.println("Unknown argument: " + arg);
                printHelp();
            }
        }

        if ( server == null )
        {
            System.err.println("Missing -server specification");
            printHelp();
        }

        if ( outDir.isFile() )
        {
            if ( forceClean )
            {
                if ( !outDir.delete() )
                {
                    throw new IOException("Could remove file obstructing: " + outDir);
                }
            }
            else
            {
                throw new IOException("File obstructing (use -f to delete?): " + outDir);
            }
        }

        if ( !outDir.exists() )
        {
            if ( !outDir.mkdirs() )
            {
                throw new IOException("Could not create out directory: " + outDir);
            }
        }

        if ( outDir.listFiles().length > 0 )
        {
            if ( forceClean )
            {
                DirectoryUtils.deleteDirectoryContents(outDir);
            }
            else
            {
                throw new IOException("Directory not empty (use -f to delete?): " + outDir);
            }
        }

        CuratorFramework client = null;
        TreeCache cache = null;
        try
        {
            client = CuratorFrameworkFactory.newClient(server, new ExponentialBackoffRetry(1000, 3));
            client.getUnhandledErrorListenable().addListener(new UnhandledErrorListener()
            {
                @Override
                public void unhandledError(String message, Throwable e)
                {
                    System.err.println(message);
                    e.printStackTrace(System.err);
                }
            });
            client.start();

            final File rootDir = outDir;

            // TODO(scottb): use namespaces when the bug is fixed.
//            CuratorFramework usingNamespace;
//            if ( rootPath.length() > 1 )
//            {
//                usingNamespace = client.usingNamespace(rootPath.substring(1));
//            }
//            else
//            {
//                usingNamespace = client;
//
//            }
//            cache = new TreeCache(usingNamespace, "", false);

            cache = TreeCache.newBuilder(client, rootPath).setCacheData(false).build();
            final boolean isVerbose = verbose;
            final boolean shouldExit = exit;
            cache.getListenable().addListener(new TreeCacheListener()
            {
                @Override
                public void childEvent(CuratorFramework client, TreeCacheEvent event) {
                    try {
                        processChildEvent(event);
                    } catch (Exception e) {
                        e.printStackTrace(System.err);
                    }
                }

                private void processChildEvent(TreeCacheEvent event) throws Exception
                {
                    if ( event.getType() == TreeCacheEvent.Type.INITIALIZED && shouldExit )
                    {
                        if ( isVerbose )
                        {
                            System.out.println(String.format("%s, exiting", event.getType()));
                        }
                        System.exit(0);
                    }

                    ChildData data = event.getData();
                    if ( data == null )
                    {
                        if ( isVerbose )
                        {
                            System.out.println(String.format("%s", event.getType()));
                        }
                        return;
                    }

                    if ( isVerbose )
                    {
                        System.out.println(String.format("%s %s", event.getType(), event.getData().getPath()));
                    }

                    checkState(data.getPath().startsWith("/"));
                    String path = data.getPath().substring(1);
                    boolean isRoot = path.isEmpty();
                    File file = isRoot ? rootDir : new File(rootDir, path);
                    File dataFile = new File(file, "zookeeper");

                    // Disjoint add/update logic
                    switch ( event.getType() )
                    {
                    case NODE_ADDED:
                        checkState(!file.exists() || isRoot, file);
                        break;

                    case NODE_UPDATED:
                        boolean isDirectory = data.getStat().getNumChildren() > 0;
                        if ( file.isDirectory() && !isDirectory )
                        {
                            // Convert directory to normal file
                            if ( dataFile.exists() )
                            {
                                Files.delete(dataFile.toPath());
                            }
                            Files.delete(file.toPath());
                        }

                        if ( !file.isDirectory() && isDirectory )
                        {
                            // Convert normal file to directory
                            Files.delete(file.toPath());
                        }
                        break;
                    }

                    // Common add/update logic
                    switch ( event.getType() )
                    {
                    case NODE_ADDED:
                    case NODE_UPDATED:
                        if (!isRoot) {
                            // Walk from the root down to the immediate parent, forcing directories.
                            ensureDirectories(file.getParentFile(), rootDir);
                        }

                        boolean isDirectory = data.getStat().getNumChildren() > 0;
                        if ( isDirectory && !isRoot )
                        {
                            Files.createDirectory(file.toPath());
                        }

                        if ( nonEmptyData(data) || !isDirectory )
                        {
                            File outFile = isDirectory ? dataFile : file;
                            FileOutputStream fileOutputStream = new FileOutputStream(outFile);
                            try
                            {
                                if (nonEmptyData(data)) {
                                    fileOutputStream.write(data.getData());
                                }
                            }
                            finally
                            {
                                CloseableUtils.closeQuietly(fileOutputStream);
                            }
                            Files.setLastModifiedTime(outFile.toPath(), FileTime.fromMillis(data.getStat().getMtime()));
                        }
                        break;

                    case NODE_REMOVED:
                        if ( dataFile.exists() )
                        {
                            Files.delete(dataFile.toPath());
                        }
                        Files.delete(file.toPath());
                        break;
                    }
                }
            });

            cache.start();

            while ( true )
            {
                Thread.sleep(10000);
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
            DirectoryUtils.deleteDirectoryContents(outDir);
        }
    }

    private static void ensureDirectories(File parent, File rootDir) throws IOException {
        if (!parent.equals(rootDir)) {
            ensureDirectories(parent.getParentFile(), rootDir);
            Path parentPath = parent.toPath();
            if (parent.isFile()) {
                if (parent.length() == 0) {
                    Files.delete(parentPath);
                } else {
                    Files.move(parentPath, parentPath.getParent().resolve("zookeeper"));
                }
            }
            if (!parent.exists()) {
                Files.createDirectory(parentPath);
            }
        }
        checkState(parent.isDirectory());
    }

    private static boolean nonEmptyData(ChildData data) {
        return data.getData() != null && data.getData().length > 0;
    }
}
