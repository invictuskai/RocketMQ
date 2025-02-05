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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 处理发送消息延时策略
 * 在RocketMq集群中，queue分布在各个不同的broker服务器中时，当尝试向其中一个queue发送消息时，如果出现耗时过长或者发送失败的情况，RocketMQ则会尝试重试发送。
 * 同样的消息第一次发送失败或耗时过长，可能是网络波动或者相关broker停止导致，如果短时间再次重试投递到当前broker极有可能还是同样的情况。所以RocketMQ提供了延迟故障自动切换Queue的功能
 * 并且会根据故障次数和失败等级来预判故障时间并自动恢复。
 * 如果指定了Queue该功能不生效
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    // 发送消息耗时时长数组  单位毫秒。使用时会反向遍历这个数组来计算broker不可用时长
    // 比如消息推送时长是600ms，那么对应的latencyMax数组下标是2，那么此时notAvailableDuration中下标为2的值是30000ms，表明此时broker不可用时长为30000ms。
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    // broker不可用时长数组 单位毫秒 和latencyMax数组一一对应
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {

        // 是否开启broker故障延迟机制
        if (this.sendLatencyFaultEnable) {
            try {
                // 使用threadLocal维护索引位置，做到线程隔离
                int index = tpInfo.getSendWhichQueue().incrementAndGet();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    // 索引位置对queue数量进行取模，保证尽量分布均匀。
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 检查对应的broker是否可用，可用则返回当前messageQueue
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }

                // 说明没有找到可用的broker，则从故障列表中选择一个broker并返回
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        return new MessageQueue(mq.getTopic(), notBestBroker, tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    } else {
                        return mq;
                    }
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }

        /**
         * 没有开启broker故障机制走到这里，直接排除上一次发送失败的broker
         * 开启broker走到这里说明队列所在的broker延迟很高无法使用。
         */
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        // 发送延迟故障自动切换Queue功能开启
        if (this.sendLatencyFaultEnable) {
            // isolation表示是否开启延迟隔离，默认不开启；开启之后默认当前消息发送时长为30s
            // 根据发送消息时长计算broker不可用时长
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    // 从latencyMax数组尾部开始寻找，找到第一个比currentLatency小的下标，然后从notAvailableDuration数组中获取对应下标的值就是当前broker需要规避的时长。
    // 在这个时间内，broker除非必要一般不会参与消息发送队列负载
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
