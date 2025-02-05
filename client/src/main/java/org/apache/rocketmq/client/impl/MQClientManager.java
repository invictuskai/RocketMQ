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
package org.apache.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

/**
 * client管理对象
 */
public class MQClientManager {
    private final static InternalLogger log = ClientLogger.getLog();

    // client管理者对象，static修饰，只有在初次类加载过程中被初始化，整个JVM只有一个，保证唯一性
    private static MQClientManager instance = new MQClientManager();

    //
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();

    // MQClientInstance缓存表，key-clientId，value-客户端实例
    private ConcurrentMap<String/* clientId */, MQClientInstance> factoryTable =
        new ConcurrentHashMap<String, MQClientInstance>();

    private MQClientManager() {

    }

    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig) {
        return getOrCreateMQClientInstance(clientConfig, null);
    }

    // 获取客户端实例
    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {

        // 生成客户端clientId，clientId= 当前client的IP@当前client的instanceName@unitName
        // 此处可以通过制定instanceName或者unitName来生成不同的MQClientInstance，进而对producer或者consumer单独的MQClientInstance对象
        // 但是要注意每一个MQClientInstance对象都有自己的
        String clientId = clientConfig.buildMQClientId();

        // 通过client获取对于其的MQClientInstance
        MQClientInstance instance = this.factoryTable.get(clientId);

        // 如果客户端实例为null
        if (null == instance) {

            // 创建一个MQClientInstance
            instance =
                new MQClientInstance(clientConfig.cloneClientConfig(),
                    this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);

            // 获取上次放入的MQClientInstance
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }

        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
