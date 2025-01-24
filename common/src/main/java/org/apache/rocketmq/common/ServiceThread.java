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
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    // 等待线程的退出时间 90s
    private static final long JOIN_TIME = 90 * 1000;

    // 处理任务的线程
    private Thread thread;

    // 倒计时锁，用于并发控制
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);

    // 是否已经通知过，用于并发控制，避免重复通知，使用了CAS并发控制，同时volatile 关键字修饰保证可见性
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

    // 是否已经停止，默认为false
    protected volatile boolean stopped = false;

    // 是否为守护线程
    protected boolean isDaemon = false;

    //Make it able to restart the thread
    // 线程是否已经启动
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    public abstract String getServiceName();

    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 通过CAS 并发控制，如果已经启动，则直接返回，避免重复启动
        if (!started.compareAndSet(false, true)) {
            return;
        }
        // 初始化停止标识
        stopped = false;
        // 启动线程，run函数实现交给子类
        this.thread = new Thread(this, getServiceName());
        // 记录是否为守护线程标识
        this.thread.setDaemon(isDaemon);

        // 启动线程
        this.thread.start();
    }

    // 停止线程
    public void shutdown() {
        this.shutdown(false);
    }

    // 停止线程
    // interrupt 是否中断线程
    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);

        // 设置线程的启动标识为false
        if (!started.compareAndSet(true, false)) {
            return;
        }

        // 设置线程停止标识为true
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        // 并发控制，避免重复通知
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            // 如果需要，设置线程中断标记位
            if (interrupt) {
                this.thread.interrupt();
            }

            // 记录开始时间
            long beginTime = System.currentTimeMillis();
            // 如果当前线程不是守护线程，则等待
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJointime());
            }

            // 记录结束时间
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    @Deprecated
    public void stop() {
        this.stop(false);
    }

    @Deprecated
    public void stop(final boolean interrupt) {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    // 停止线程，不进行中断
    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }

    // 唤醒线程
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    // 线程等待指定
    protected void waitForRunning(long interval) {
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        //entry to wait
        waitPoint.reset();

        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
