/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.srvutil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 文件变更监听服务，内部维护了需要监听的文件列表文件列表的hash摘要和监听器。
 * 当调用线程的start函数之后，会启动一个线程，每隔500ms检查一次文件列表中的文件是否发生了变化，如果发生了变化，则调用监听器中的onChanged回调函数。
 */
public class FileWatchService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    // 需要监听的文件列表
    private final List<String> watchFiles;

    // 文件列表中每个文件的hash摘要
    private final List<String> fileCurrentHash;

    // 监听器
    private final Listener listener;

    // 监听间隔
    private static final int WATCH_INTERVAL = 500;
    private MessageDigest md = MessageDigest.getInstance("MD5");

    /**
     *
     * @param watchFiles 需要监听的文件列表
     * @param listener 监听器
     * @throws Exception exception
     */
    public FileWatchService(final String[] watchFiles,
        final Listener listener) throws Exception {
        this.listener = listener;
        this.watchFiles = new ArrayList<>();
        this.fileCurrentHash = new ArrayList<>();

        // 遍历需要监听的文件列表，如果文件存在，则将其添加到需要监听的文件列表中，并计算出该文件的hash摘要
        for (int i = 0; i < watchFiles.length; i++) {
            if (StringUtils.isNotEmpty(watchFiles[i]) && new File(watchFiles[i]).exists()) {
                this.watchFiles.add(watchFiles[i]);
                this.fileCurrentHash.add(hash(watchFiles[i]));
            }
        }
    }

    @Override
    public String getServiceName() {
        return "FileWatchService";
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(WATCH_INTERVAL);

                for (int i = 0; i < watchFiles.size(); i++) {
                    String newHash;
                    try {
                        newHash = hash(watchFiles.get(i));
                    } catch (Exception ignored) {
                        log.warn(this.getServiceName() + " service has exception when calculate the file hash. ", ignored);
                        continue;
                    }
                    if (!newHash.equals(fileCurrentHash.get(i))) {
                        fileCurrentHash.set(i, newHash);
                        listener.onChanged(watchFiles.get(i));
                    }
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }

    // 计算文件的hash摘要
    private String hash(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        md.update(Files.readAllBytes(path));
        byte[] hash = md.digest();
        return UtilAll.bytes2string(hash);
    }

    public interface Listener {
        /**
         * Will be called when the target files are changed
         * @param path the changed file path
         */
        void onChanged(String path);
    }
}
