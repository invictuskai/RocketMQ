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
package org.apache.rocketmq.remoting.netty;

// 服务端netty网络层配置类
public class NettyServerConfig implements Cloneable {

    // 监听端口号，默认为8888
    private int listenPort = 8888;

    // 业务线程池，执行handler的EventLoop线程池组
    private int serverWorkerThreads = 8;

    /**
     * 根据这个值创建一个publicExecutor线程值.netty会根据不同的业务类型创建不同的线程池
     * 比如处理消息发送，消息消费，心跳检测等，如果当前业务类型没有注册线程池，则使用publicExecutor线程池
     */
    private int serverCallbackExecutorThreads = 0;

    //worker工作组线程池线程数量，I/O线程池个数，主要处理网络请求。
    private int serverSelectorThreads = 3;

    //服务端单向访问客户端的并发数限制
    private int serverOnewaySemaphoreValue = 256;

    //服务端异步访问客户端并发数限制
    private int serverAsyncSemaphoreValue = 64;

    //服务端Channel最大空闲存活时间， 默认为120s
    private int serverChannelMaxIdleTimeSeconds = 120;


    //网络socket发送缓存区大小 默认64KB
    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;

    //网络socket接收缓存区大小 默认64KB
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;

    //写缓冲区最高水位标记
    private int writeBufferHighWaterMark = NettySystemConfig.writeBufferHighWaterMark;

    //写缓冲区最低水位标记
    private int writeBufferLowWaterMark = NettySystemConfig.writeBufferLowWaterMark;

    // TCP连接队列大小，默认1024
    private int serverSocketBacklog = NettySystemConfig.socketBacklog;

    // 是否开启服务端netty内存池优化
    private boolean serverPooledByteBufAllocatorEnable = true;

    /**
     * make install
     *
     *
     * ../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \
     * --host=x86_64-linux-gnu \ --build=x86_64-pc-linux-gnu \ --without-gd
     */
    // 是否开启epoll，Linux下建议开启
    private boolean useEpollNativeSelector = false;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }

    public void setServerWorkerThreads(int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
    }

    public int getServerSelectorThreads() {
        return serverSelectorThreads;
    }

    public void setServerSelectorThreads(int serverSelectorThreads) {
        this.serverSelectorThreads = serverSelectorThreads;
    }

    public int getServerOnewaySemaphoreValue() {
        return serverOnewaySemaphoreValue;
    }

    public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
        this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
    }

    public int getServerCallbackExecutorThreads() {
        return serverCallbackExecutorThreads;
    }

    public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
        this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
    }

    public int getServerAsyncSemaphoreValue() {
        return serverAsyncSemaphoreValue;
    }

    public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
        this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
    }

    public int getServerChannelMaxIdleTimeSeconds() {
        return serverChannelMaxIdleTimeSeconds;
    }

    public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
        this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
    }

    public int getServerSocketSndBufSize() {
        return serverSocketSndBufSize;
    }

    public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
        this.serverSocketSndBufSize = serverSocketSndBufSize;
    }

    public int getServerSocketRcvBufSize() {
        return serverSocketRcvBufSize;
    }

    public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
        this.serverSocketRcvBufSize = serverSocketRcvBufSize;
    }

    public int getServerSocketBacklog() {
        return serverSocketBacklog;
    }

    public void setServerSocketBacklog(int serverSocketBacklog) {
        this.serverSocketBacklog = serverSocketBacklog;
    }

    public boolean isServerPooledByteBufAllocatorEnable() {
        return serverPooledByteBufAllocatorEnable;
    }

    public void setServerPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
        this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
    }

    public boolean isUseEpollNativeSelector() {
        return useEpollNativeSelector;
    }

    public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
        this.useEpollNativeSelector = useEpollNativeSelector;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return (NettyServerConfig) super.clone();
    }

    public int getWriteBufferLowWaterMark() {
        return writeBufferLowWaterMark;
    }

    public void setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
        this.writeBufferLowWaterMark = writeBufferLowWaterMark;
    }

    public int getWriteBufferHighWaterMark() {
        return writeBufferHighWaterMark;
    }

    public void setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
        this.writeBufferHighWaterMark = writeBufferHighWaterMark;
    }
}
