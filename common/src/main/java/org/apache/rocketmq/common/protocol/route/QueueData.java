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

/**
 * $Id: QueueData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.route;

// 队列数据，这个记录的是一个broker中某个topic的所有队列信息
// queue有两种选择算法，分别是默认的轮询算法和最小投递延迟算法

/**
 * 最小延迟投算法
 * 会统计每次消息投递的时间延迟，然后根据统计出的结果将消息投递到时间延迟最小的Queue。 如果延迟相同，则采用轮询算法投递。该算法可以有效提升消息的投递性能。
 */
public class QueueData implements Comparable<QueueData> {

    // broker名称
    private String brokerName;

    // 读队列数量
    private int readQueueNums;

    // 写队列数量
    private int writeQueueNums;

    // 权限
    private int perm;

    //同步的位置标识
    private int topicSysFlag;

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public int getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(int topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + perm;
        result = prime * result + readQueueNums;
        result = prime * result + writeQueueNums;
        result = prime * result + topicSysFlag;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueueData other = (QueueData) obj;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        } else if (!brokerName.equals(other.brokerName))
            return false;
        if (perm != other.perm)
            return false;
        if (readQueueNums != other.readQueueNums)
            return false;
        if (writeQueueNums != other.writeQueueNums)
            return false;
        if (topicSysFlag != other.topicSysFlag)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "QueueData [brokerName=" + brokerName + ", readQueueNums=" + readQueueNums
            + ", writeQueueNums=" + writeQueueNums + ", perm=" + perm + ", topicSysFlag=" + topicSysFlag
            + "]";
    }

    @Override
    public int compareTo(QueueData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
