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
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {

    // 引用数量，当refCount<=0 说明没有任何程序使用，可以释放资源了
    protected final AtomicLong refCount = new AtomicLong(1);

    // 表示当前资源是否可用 false表示当前资源属于不可用状态
    protected volatile boolean available = true;

    // 是否清理完毕资源，默认是false，当清理完毕之后为true
    protected volatile boolean cleanupOver = false;

    // 第一次调调用shutdown释放资源时的时间戳，有可能refCount值比较大，第一次调用shutdown有很多其他进程在使用当前资源，此时firstShutdownTimestamp会记录初次请求关闭资源的时间
    // 后续再次关闭当前资源的时候会传递一个时间阈值interval，如果当前系统时间-firstShutdownTimestamp>interval  此时会强制关闭
    private volatile long firstShutdownTimestamp = 0;

    // 增加引用计数
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    // 关闭资源
    public void shutdown(final long intervalForcibly) {
        // 资源可用
        if (this.available) {
            // 标记资源不可用
            this.available = false;
            // 记录首次请求资源关闭的时间
            this.firstShutdownTimestamp = System.currentTimeMillis();
            // 资源关闭
            this.release();
        } else if (this.getRefCount() > 0) {
            // 说明当前资源已经不可用但是还有其他程序持有，判断时间是否超过阈值，超过强制关闭
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    // 释放当前资源
    public void release() {
        // 说明资源仍有其他程序使用
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;
        // 释放当前资源
        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
