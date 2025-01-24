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
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.common.RemotingUtil;
/**
 * 管理心跳信息以及路由管理，主要是映射表
 * RocketMQ基于发布订阅机制，一个Topic拥有多个消息队列，一个Broker为每一个主题默认创建4个读队列4个写队列。
 * 多个Broker组成一个集群，BrokerName由相同的多台Broker组成Master-Slave架构，brokerId为0代表Master，大于0表示Slave。BrokerLiveInfo中的lastUpdateTimestamp存储上次收到Broker心跳包的时间
 * 假如一个2主2从的broker集群
 * cluster:c1                              cluster:c1
 * brokerName:broker-a   主<------>从       brokerName:broker-a
 * brokerId:0                              brokerId:1
 * cluster:c1                              cluster:c1
 * brokerName:broker-b   主<------>从       brokerName:broker-b
 * brokerId:0                           brokerId:1
 *
 * 对应的topicQueueTable数据如下
 *   "topicQueueTable": {
 *     "topic1": [
 *       {
 *         "brokerName": "broker-a",
 *         "readQueueNums": 4,
 *         "writeQueueNums": 4,
 *         "perm": 6, // 读写权限
 *         "topicSynFlag": 0 // topic同步标记
 *       },
 *       {
 *         "brokerName": "broker-b",
 *         "readQueueNums": 4,
 *         "writeQueueNums": 4,
 *         "perm": 6, // 读写权限
 *         "topicSynFlag": 0 // topic同步标记
 *       }
 *     ],
 *     "topic other": []
 *   }
 *   对应的brokerAddrTable数据如下
 *     "brokerAddrTable": {
 *     "broker-a": {
 *       "cluster": "c1",
 *       "brokerName": "broker-a",
 *       "brokerAddrs": {
 *         "0": "192.168.56.1:10000",
 *         "1": "192.168.56.2:10000"
 *       }
 *     },
 *     "broker-b": {
 *       "cluster": "c1",
 *       "brokerName": "broker-b",
 *       "brokerAddrs": {
 *         "0": "192.168.56.3:10000",
 *         "1": "192.168.56.4:10000"
 *       }
 *     }
 *   }
 *   对应的brokerLiveTable数据如下
 *     "brokerLiveTable": {
 *     "192.168.56.1:10000": {
 *       "lastUpdateTimeStamp": 1518270318980,
 *       "dataVersion": "versionObj",
 *       "channel": "channelObj",
 *       "hasServerAddr": "192.168.56.2:10000"
 *     },
 *     "192.168.56.2:10000": {
 *       "lastUpdateTimeStamp": 1518270318980,
 *       "dataVersion": "versionObj",
 *       "channel": "channelObj",
 *       "hasServerAddr": "192.168.56.3:10000"
 *     },
 *     "192.168.56.3:10000": {
 *       "lastUpdateTimeStamp": 1518270318980,
 *       "dataVersion": "versionObj",
 *       "channel": "channelObj",
 *       "hasServerAddr": "192.168.56.4:10000"
 *     },
 *     "192.168.56.4:10000": {
 *       "lastUpdateTimeStamp": 1518270318980,
 *       "dataVersion": "versionObj",
 *       "channel": "channelObj",
 *       "hasServerAddr": ""
 *     }
 *   }
 *   对应的clusterAddrTable数据如下
 *   "clusterAddrTable": {
 *     "c1": ["broker-a", "broker-b"]
 *   }
 */
public class RouteInfoManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    // broker连接的Channel有效时间 2分钟
    private final static long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;

    // 读写锁，用于并发控制
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // key为topic，value为队列在broker上的分布情况，可以理解为每一个BrokerName对应一个QueueData。topic消息队列路由信息，消息发送时根据该映射表进行负载均衡
    private final HashMap<String/* topic */, Map<String /* brokerName */ , QueueData>> topicQueueTable;

    // key为broker名称，value为Broker基础信息。包含 brokerName所属集群名称，brokerName下的master节点的id和ip地址，slave节点的id和ip地址。
    private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;

    // Broker集群信息，key为集群名称，value为存储当前集群名称下的所有Broker名称
    private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;

    // Broker状态信息。NameServer每次收到心跳包会替换该信息
    private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;

    // Broker上的FilterServer列表，用于类模式消息过滤用的，类模式过滤在4.4版本之后被废弃
    private final HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    public RouteInfoManager() {
        this.topicQueueTable = new HashMap<>(1024);
        this.brokerAddrTable = new HashMap<>(128);
        this.clusterAddrTable = new HashMap<>(32);
        this.brokerLiveTable = new HashMap<>(256);
        this.filterServerTable = new HashMap<>(256);
    }

    public ClusterInfo getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper;
    }

    public void deleteTopic(final String topic) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                this.topicQueueTable.remove(topic);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        }
    }

    // 刪除topic
    public void deleteTopic(final String topic, final String clusterName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
                if (brokerNames != null
                    && !brokerNames.isEmpty()) {
                    Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
                    if (queueDataMap != null) {
                        for (String brokerName : brokerNames) {
                            final QueueData removedQD = queueDataMap.remove(brokerName);
                            if (removedQD != null) {
                                log.info("deleteTopic, remove one broker's topic {} {} {}", brokerName, topic,
                                    removedQD);
                            }
                        }
                        if (queueDataMap.isEmpty()) {
                            log.info("deleteTopic, remove the topic all queue {} {}", clusterName, topic);
                            this.topicQueueTable.remove(topic);
                        }
                    }

                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        }
    }

    public TopicList getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                topicList.getTopicList().addAll(this.topicQueueTable.keySet());
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList;
    }

    // 注册broker到namesrv
    public RegisterBrokerResult registerBroker(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId,
            final String haServerAddr,
            final TopicConfigSerializeWrapper topicConfigWrapper,
            final List<String> filterServerList,
            final Channel channel) {
        // 返回结果封装对象
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            try {
                // 获取锁，防止并发修改
                this.lock.writeLock().lockInterruptibly();

                //将当前集群放入到clusterAddrTable中，并获取集群对应的brokerName列表
                Set<String> brokerNames = this.clusterAddrTable.computeIfAbsent(clusterName, k -> new HashSet<>());
                brokerNames.add(brokerName);

                // 首次注册标志
                boolean registerFirst = false;

                //从brokerAddrTable中获取brokerName对应的brokerDate
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                // 条件满足，首次注册
                if (null == brokerData) {
                    registerFirst = true;
                    // 创建brokerData
                    brokerData = new BrokerData(clusterName, brokerName, new HashMap<>());
                    // 将当前brokerData添加到brokerAddrTable中
                    this.brokerAddrTable.put(brokerName, brokerData);
                }

                // 获取当前broker的物理节点数据，每个broker对应的ip地址
                Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();
                //Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
                //The same IP:PORT must only have one record in brokerAddrTable
                // 遍历这个broker物理节点列表
                Iterator<Entry<Long, String>> it = brokerAddrsMap.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<Long, String> item = it.next();
                    // 条件成立，说明新上线的broker名字相同，brokerId确不同，说明broker信息发生变化
                    if (null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey()) {
                        log.debug("remove entry {} from brokerData", item);
                        // 将当前brokerAddrsMap中对应的旧的broker物理地址信息移除
                        it.remove();
                    }
                }

                // 重新将当前broker放入
                String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
                //条件成立，说明当前是master节点，
                if (MixAll.MASTER_ID == brokerId) {
                    log.info("cluster [{}] brokerName [{}] master address change from {} to {}",
                            brokerData.getCluster(), brokerData.getBrokerName(), oldAddr, brokerAddr);
                }

                // 判断为首次注册
                registerFirst = registerFirst || (null == oldAddr);

                // 条件成立：当前broker上的topic不为空，当前broker为master节点
                if (null != topicConfigWrapper
                        && MixAll.MASTER_ID == brokerId) {
                    /**
                     * 条件成立，说明当前broker的topic信息发生变化或者当前broker为首次注册，此时需要创建或者更新topic的路由元数据
                     * 同时更新topicQueueTable数据。其实是为默认主题自动注册路由信息，其中包含MixAll.DEFAULT_TOPIC的路由信息。当消息生产者发送主题时，
                     * 如果该主题未创建，并且BrokerConfig的autoCreateTopicEnable为true，则返回MixAll.DEFAULT_TOPIC的路由信息，
                     */
                    if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.getDataVersion())
                            || registerFirst) {
                        // 获取当前broker上的topic配置信息
                        ConcurrentMap<String, TopicConfig> tcTable =
                                topicConfigWrapper.getTopicConfigTable();
                        if (tcTable != null) {
                            // 遍历当前topic，更新topicQueueTable数据
                            for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                                this.createAndUpdateQueueData(brokerName, entry.getValue());
                            }
                        }
                    }
                }

                // 创建BrokerLiveInfo保存当前活跃的Broker信息，并返回上一次心跳时当前broker节点的BrokerLiveInfo信息
                BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr,
                        new BrokerLiveInfo(
                                System.currentTimeMillis(),
                                topicConfigWrapper.getDataVersion(),
                                channel,
                                haServerAddr));
                // 如果上一次是null，说明是新注册的broker
                if (null == prevBrokerLiveInfo) {
                    log.info("new broker registered, {} HAServer: {}", brokerAddr, haServerAddr);
                }

                // 注册Broker的过滤器Server地址列表，一个Broker上会关联多个FilterServer消息过滤服务器
                if (filterServerList != null) {
                    if (filterServerList.isEmpty()) {
                        this.filterServerTable.remove(brokerAddr);
                    } else {
                        this.filterServerTable.put(brokerAddr, filterServerList);
                    }
                }

                // 当前broker不是master节点
                if (MixAll.MASTER_ID != brokerId) {
                    // 获取当前broker的master节点信息
                    String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (masterAddr != null) {
                        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(masterAddr);
                        if (brokerLiveInfo != null) {
                            result.setHaServerAddr(brokerLiveInfo.getHaServerAddr());
                            result.setMasterAddr(masterAddr);
                        }
                    }
                }
            } finally {
                // 释放写锁
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        }

        // 返回结果
        return result;
    }

    // 判断当前broker上的topic信息发生变化
    public boolean isBrokerTopicConfigChanged(final String brokerAddr, final DataVersion dataVersion) {
        DataVersion prev = queryBrokerTopicConfig(brokerAddr);
        return null == prev || !prev.equals(dataVersion);
    }

    public DataVersion queryBrokerTopicConfig(final String brokerAddr) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            return prev.getDataVersion();
        }
        return null;
    }

    public void updateBrokerInfoUpdateTimestamp(final String brokerAddr, long timeStamp) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            prev.setLastUpdateTimestamp(timeStamp);
        }
    }

    // 创建或更新topicQueueTable数据
    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        QueueData queueData = new QueueData();
        // 设置当前队列所在的brokerName
        queueData.setBrokerName(brokerName);
        // 设置当前队列的读写队列数量
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        // 设置当前队列的权限
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());

        // 创建或更新topicQueueTable数据
        Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topicConfig.getTopicName());
        if (null == queueDataMap) {
            queueDataMap = new HashMap<>();
            queueDataMap.put(queueData.getBrokerName(), queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataMap);
            log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);
        } else {
            QueueData old = queueDataMap.put(queueData.getBrokerName(), queueData);
            if (old != null && !old.equals(queueData)) {
                log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), old,
                        queueData);
            }
        }
    }

    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        return operateWritePermOfBrokerByLock(brokerName, RequestCode.WIPE_WRITE_PERM_OF_BROKER);
    }

    public int addWritePermOfBrokerByLock(final String brokerName) {
        return operateWritePermOfBrokerByLock(brokerName, RequestCode.ADD_WRITE_PERM_OF_BROKER);
    }

    private int operateWritePermOfBrokerByLock(final String brokerName, final int requestCode) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return operateWritePermOfBroker(brokerName, requestCode);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("operateWritePermOfBrokerByLock Exception", e);
        }

        return 0;
    }


    private int operateWritePermOfBroker(final String brokerName, final int requestCode) {
        int topicCnt = 0;

        for (Map.Entry<String, Map<String, QueueData>> entry : topicQueueTable.entrySet()) {
            String topic = entry.getKey();
            Map<String, QueueData> queueDataMap = entry.getValue();

            if (queueDataMap != null) {
                QueueData qd = queueDataMap.get(brokerName);
                if (qd != null) {
                    int perm = qd.getPerm();
                    switch (requestCode) {
                        case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                            perm &= ~PermName.PERM_WRITE;
                            break;
                        case RequestCode.ADD_WRITE_PERM_OF_BROKER:
                            perm = PermName.PERM_READ | PermName.PERM_WRITE;
                            break;
                    }
                    qd.setPerm(perm);

                    topicCnt++;
                }
            }
        }

        return topicCnt;
    }

    public void unregisterBroker(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddr);
                log.info("unregisterBroker, remove from brokerLiveTable {}, {}",
                        brokerLiveInfo != null ? "OK" : "Failed",
                        brokerAddr
                );

                this.filterServerTable.remove(brokerAddr);

                boolean removeBrokerName = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    String addr = brokerData.getBrokerAddrs().remove(brokerId);
                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}",
                            addr != null ? "OK" : "Failed",
                            brokerAddr
                    );

                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}",
                                brokerName
                        );

                        removeBrokerName = true;
                    }
                }

                if (removeBrokerName) {
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}",
                                removed ? "OK" : "Failed",
                                brokerName);

                        if (nameSet.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}",
                                    clusterName
                            );
                        }
                    }
                    this.removeTopicByBrokerName(brokerName);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("unregisterBroker Exception", e);
        }
    }

    private void removeTopicByBrokerName(final String brokerName) {
        Set<String> noBrokerRegisterTopic = new HashSet<>();

        this.topicQueueTable.forEach((topic, queueDataMap) -> {
            QueueData old = queueDataMap.remove(brokerName);
            if (old != null) {
                log.info("removeTopicByBrokerName, remove one broker's topic {} {}", topic, old);
            }

            if (queueDataMap.size() == 0) {
                noBrokerRegisterTopic.add(topic);
                log.info("removeTopicByBrokerName, remove the topic all queue {}", topic);
            }
        });

        noBrokerRegisterTopic.forEach(topicQueueTable::remove);
    }

    // 读取映射表中的数据
    public TopicRouteData pickupTopicRouteData(final String topic) {

        // 创建TopicRouteData
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundQueueData = false;
        boolean foundBrokerData = false;

        // 存放brokerName集合
        Set<String> brokerNameSet = new HashSet<>();

        // 存放broker数据集合
        List<BrokerData> brokerDataList = new LinkedList<>();
        topicRouteData.setBrokerDatas(brokerDataList);

        HashMap<String, List<String>> filterServerMap = new HashMap<>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            try {

                // 加读锁
                this.lock.readLock().lockInterruptibly();

                // 获取当前topic的队列元数据信息
                Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);

                // 如果已经有对应topic的队列数据
                if (queueDataMap != null) {

                    topicRouteData.setQueueDatas(new ArrayList<>(queueDataMap.values()));
                    foundQueueData = true;

                    brokerNameSet.addAll(queueDataMap.keySet());

                    for (String brokerName : brokerNameSet) {
                        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                        if (null != brokerData) {
                            BrokerData brokerDataClone = new BrokerData(brokerData.getCluster(), brokerData.getBrokerName(), (HashMap<Long, String>) brokerData
                                    .getBrokerAddrs().clone());
                            brokerDataList.add(brokerDataClone);
                            foundBrokerData = true;

                            // skip if filter server table is empty
                            if (!filterServerTable.isEmpty()) {
                                for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                                    List<String> filterServerList = this.filterServerTable.get(brokerAddr);

                                    // only add filter server list when not null
                                    if (filterServerList != null) {
                                        filterServerMap.put(brokerAddr, filterServerList);
                                    }
                                }
                            }
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        }

        log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);

        if (foundBrokerData && foundQueueData) {
            return topicRouteData;
        }

        return null;
    }

    // 遍历心跳失败的broker并移除，每10s执行一次
    public int scanNotActiveBroker() {
        // 统计本次移除的broker数量
        int removeCount = 0;

        // 获取当前活跃的broker列表
        Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();

        // 开始遍历
        while (it.hasNext()) {
            Entry<String, BrokerLiveInfo> next = it.next();

            // 当前broker上次上报心跳的时间
            long last = next.getValue().getLastUpdateTimestamp();
            if ((last + BROKER_CHANNEL_EXPIRED_TIME) < System.currentTimeMillis()) {
                // 关闭channel连接
                RemotingUtil.closeChannel(next.getValue().getChannel());
                it.remove();
                log.warn("The broker channel expired, {} {}ms", next.getKey(), BROKER_CHANNEL_EXPIRED_TIME);
                this.onChannelDestroy(next.getKey(), next.getValue().getChannel());

                removeCount++;
            }
        }

        return removeCount;
    }

    /**
     * broker与NameSrv连接的Channel关闭，同时更新RouterInfoManager中的各种映射表
     * @param remoteAddr broker的地址
     * @param channel broker和NameSrv的连接Channel
     */
    public void onChannelDestroy(String remoteAddr, Channel channel) {
        // 记录要移除broker的地址
        String brokerAddrFound = null;
        if (channel != null) {
            try {
                try {
                    // 获取锁
                    this.lock.readLock().lockInterruptibly();
                    Iterator<Entry<String, BrokerLiveInfo>> itBrokerLiveTable =
                            this.brokerLiveTable.entrySet().iterator();

                    // 遍历brokerLiveTable
                    while (itBrokerLiveTable.hasNext()) {
                        Entry<String, BrokerLiveInfo> entry = itBrokerLiveTable.next();
                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (null == brokerAddrFound) {
            brokerAddrFound = remoteAddr;
        } else {
            log.info("the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
        }

        if (brokerAddrFound != null && brokerAddrFound.length() > 0) {

            try {
                try {
                    // 申请写锁。根据brokerAddress从brokerLiveTable、filterServerTable中移除Broker相关的信息
                    this.lock.writeLock().lockInterruptibly();
                    this.brokerLiveTable.remove(brokerAddrFound);
                    this.filterServerTable.remove(brokerAddrFound);
                    String brokerNameFound = null;
                    boolean removeBrokerName = false;

                    // 更新brokerAddrTable中的数据，遍历brokerAddrTable，从brokerData的brokerAddrs中找到具体的broker从brokerData中移除
                    Iterator<Entry<String, BrokerData>> itBrokerAddrTable =
                            this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerAddrTable.hasNext() && (null == brokerNameFound)) {
                        BrokerData brokerData = itBrokerAddrTable.next().getValue();

                        Iterator<Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> entry = it.next();
                            Long brokerId = entry.getKey();
                            String brokerAddr = entry.getValue();
                            if (brokerAddr.equals(brokerAddrFound)) {
                                brokerNameFound = brokerData.getBrokerName();
                                it.remove();
                                log.info("remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed",
                                        brokerId, brokerAddr);
                                break;
                            }
                        }
                        // 如果移除后的brokerAddrTable为空，则从brokerAddrTable中移除该brokerName
                        if (brokerData.getBrokerAddrs().isEmpty()) {
                            removeBrokerName = true;
                            itBrokerAddrTable.remove();
                            log.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed",
                                    brokerData.getBrokerName());
                        }
                    }

                    // 根据BrokerName，从clusterAddrTable中找到Broker并将其从集群中移除。如果移除后，集群中不包含任何Broker，则将该
                    //集群从clusterAddrTable中移除，
                    if (brokerNameFound != null && removeBrokerName) {
                        Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<String, Set<String>> entry = it.next();
                            String clusterName = entry.getKey();
                            Set<String> brokerNames = entry.getValue();
                            boolean removed = brokerNames.remove(brokerNameFound);
                            if (removed) {
                                log.info("remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed",
                                        brokerNameFound, clusterName);

                                if (brokerNames.isEmpty()) {
                                    log.info("remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster",
                                            clusterName);
                                    it.remove();
                                }

                                break;
                            }
                        }
                    }
                    // 根据BrokerName，遍历所有主题的队列，如果队列中包含当前Broker的队列，则移除，如果topic只包含待移除Broker的队列，从路由表中删除该topic
                    if (removeBrokerName) {
                        String finalBrokerNameFound = brokerNameFound;
                        Set<String> needRemoveTopic = new HashSet<>();

                        topicQueueTable.forEach((topic, queueDataMap) -> {
                            QueueData old = queueDataMap.remove(finalBrokerNameFound);
                            log.info("remove topic[{} {}], from topicQueueTable, because channel destroyed",
                                    topic, old);

                            if (queueDataMap.size() == 0) {
                                log.info("remove topic[{}] all queue, from topicQueueTable, because channel destroyed",
                                        topic);
                                needRemoveTopic.add(topic);
                            }
                        });

                        needRemoveTopic.forEach(topicQueueTable::remove);
                    }
                } finally {
                    this.lock.writeLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }
    }

    public void printAllPeriodically() {
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                log.info("--------------------------------------------------------");
                {
                    log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                    Iterator<Entry<String, Map<String, QueueData>>> it = this.topicQueueTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Map<String, QueueData>> next = it.next();
                        log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                    Iterator<Entry<String, BrokerData>> it = this.brokerAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerData> next = it.next();
                        log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                    Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerLiveInfo> next = it.next();
                        log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
                    Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Set<String>> next = it.next();
                        log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("printAllPeriodically Exception", e);
        }
    }

    public TopicList getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
                    topicList.getTopicList().add(entry.getKey());
                    topicList.getTopicList().addAll(entry.getValue());
                }

                if (brokerAddrTable != null && !brokerAddrTable.isEmpty()) {
                    Iterator<String> it = brokerAddrTable.keySet().iterator();
                    while (it.hasNext()) {
                        BrokerData bd = brokerAddrTable.get(it.next());
                        HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
                        if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
                            Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                            topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
                            break;
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList;
    }

    public TopicList getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();

                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
                for (String brokerName : brokerNameSet) {
                    this.topicQueueTable.forEach((topic, queueDataMap) -> {
                        if (queueDataMap.containsKey(brokerName)) {
                            topicList.getTopicList().add(topic);
                        }
                    });
                }

            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList;
    }

    public TopicList getUnitTopics() {
        return topicQueueTableIter(qd -> TopicSysFlag.hasUnitFlag(qd.getTopicSysFlag()));
    }

    public TopicList getHasUnitSubTopicList() {
        return topicQueueTableIter(qd -> TopicSysFlag.hasUnitSubFlag(qd.getTopicSysFlag()));
    }

    public TopicList getHasUnitSubUnUnitTopicList() {
        return topicQueueTableIter(qd -> !TopicSysFlag.hasUnitFlag(qd.getTopicSysFlag())
                && TopicSysFlag.hasUnitSubFlag(qd.getTopicSysFlag()));
    }

    private TopicList topicQueueTableIter(Predicate<QueueData> pickCondition) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();

                topicQueueTable.forEach((topic, queueDataMap) -> {
                    for (QueueData qd : queueDataMap.values()) {
                        if (pickCondition.test(qd)) {
                            topicList.getTopicList().add(topic);
                        }

                        // we need only one queue data here
                        break;
                    }
                });

            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList;
    }
}

class BrokerLiveInfo {

    // 上次更新时间
    private long lastUpdateTimestamp;

    // 数据版本
    private DataVersion dataVersion;

    // 网络Channel信息
    private Channel channel;

    // 这个broker是否有NameSrv信息
    private String haServerAddr;

    public BrokerLiveInfo(long lastUpdateTimestamp, DataVersion dataVersion, Channel channel,
                          String haServerAddr) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.dataVersion = dataVersion;
        this.channel = channel;
        this.haServerAddr = haServerAddr;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    @Override
    public String toString() {
        return "BrokerLiveInfo [lastUpdateTimestamp=" + lastUpdateTimestamp + ", dataVersion=" + dataVersion
                + ", channel=" + channel + ", haServerAddr=" + haServerAddr + "]";
    }
}
