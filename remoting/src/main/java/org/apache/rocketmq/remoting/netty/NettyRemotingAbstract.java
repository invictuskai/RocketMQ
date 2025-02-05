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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.common.ServiceThread;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

public abstract class NettyRemotingAbstract {

    /**
     * Remoting logger instance.
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * Semaphore to limit maximum number of on-going one-way requests, which protects system memory footprint.
     */

    //单向请求并发度控制
    protected final Semaphore semaphoreOneway;

    /**
     * Semaphore to limit maximum number of on-going asynchronous requests, which protects system memory footprint.
     */

    // 异步请求并发度控制
    protected final Semaphore semaphoreAsync;

    /**
     * This map caches all on-going requests.
     */

    // ResponseFuture映射表
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable =
        new ConcurrentHashMap<Integer, ResponseFuture>(256);

    /**
     * This container holds all processors per request code, aka, for each incoming request, we may look up the
     * responding processor in this map to handle the request.
     */

    // 请求码和processor映射表，key-请求码  value-对应的处理器Processor和执行这个Processor的线程池
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
        new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);

    /**
     * Executor to feed netty events to user defined {@link ChannelEventListener}.
     */

    // netty事件执行器
    protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    /**
     * The default request processor to use in case there is no exact match in {@link #processorTable} per request code.
     */

    // 默认处理器和执行器映射表
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

    /**
     * SSL context via which to create {@link SslHandler}.
     */
    protected volatile SslContext sslContext;

    /**
     * custom rpc hooks
     */
    protected List<RPCHook> rpcHooks = new ArrayList<RPCHook>();


    static {
        NettyLogger.initNettyLogger();
    }

    /**
     * Constructor, specifying capacity of one-way and asynchronous semaphores.
     *
     * @param permitsOneway Number of permits for one-way requests.
     * @param permitsAsync Number of permits for asynchronous requests.
     */
    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    /**
     * Custom channel event listener.
     *
     * @return custom channel event listener if defined; null otherwise.
     */
    public abstract ChannelEventListener getChannelEventListener();

    /**
     * Put a netty event to the executor.
     *
     * @param event Netty event instance.
     */
    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecutor.putNettyEvent(event);
    }

    /**
     * Entry of incoming command processing.
     * 处理消息发送请求
     * <p>
     * <strong>Note:</strong>
     * The incoming remoting command may be
     * <ul>
     * <li>An inquiry request from a remote peer component;</li>
     * <li>A response to a previous request issued by this very participant.</li>
     * </ul>
     * </p>
     *
     * @param ctx Channel handler context.
     * @param msg incoming remoting command.
     * @throws Exception if there were any error while processing the incoming command.
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            // 根据协议传输对象的不同类型做不同的处理
            switch (cmd.getType()) {
                // client主动发起的请求
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, cmd);
                    break;
                // client请求后得到服务端的响应，这个得到的相应消息类型为RESPONSE_COMMAND
                case RESPONSE_COMMAND:
                    // 处理客户端返回结果，同时移除responseTable中的映射信息
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }

    protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook: rpcHooks) {
                rpcHook.doBeforeRequest(addr, request);
            }
        }
    }

    protected void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook: rpcHooks) {
                rpcHook.doAfterResponse(addr, request, response);
            }
        }
    }


    /**
     * Process incoming request command issued by remote peer.
     *
     * @param ctx channel handler context.
     * @param cmd request command.
     */

    // 根据RemotingCommand的code获取对应的process处理器
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        // 根据业务代码的code找到对应的pair
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        // 如果没有找到对应的pair就用默认的 namesrv并未注册code对应的processor，使用默认的
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessor : matched;
        // 获取这个网络传输对象的opaque
        final int opaque = cmd.getOpaque();
        // 找到对应的pair()
        if (pair != null) {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        // 获取当前Channel的地址信息
                        String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                        // 执行BeforeRpcHooks
                        doBeforeRpcHooks(remoteAddr, cmd);
                        //封装响应客户端的逻辑
                        final RemotingResponseCallback callback = new RemotingResponseCallback() {
                            @Override
                            public void callback(RemotingCommand response) {
                                // 执行RPCHook的after方法
                                doAfterRpcHooks(remoteAddr, cmd, response);
                                // 如果请求不是单向的，需要响应客户端发来的请求
                                if (!cmd.isOnewayRPC()) {
                                    if (response != null) {
                                        // 设置响应的opaque
                                        response.setOpaque(opaque);
                                        response.markResponseType();
                                        response.setSerializeTypeCurrentRPC(cmd.getSerializeTypeCurrentRPC());
                                        try {
                                            // 提交给IO线程响应客户端
                                            ctx.writeAndFlush(response);
                                        } catch (Throwable e) {
                                            log.error("process request over, but response failed", e);
                                            log.error(cmd.toString());
                                            log.error(response.toString());
                                        }
                                    } else {
                                    }
                                }
                            }
                        };

                        //获取pair对应的processor并调用，这里AsyncNettyRequestProcessor有一个子类DefaultRequestProcessor
                        if (pair.getObject1() instanceof AsyncNettyRequestProcessor) {
                            AsyncNettyRequestProcessor processor = (AsyncNettyRequestProcessor)pair.getObject1();
                            processor.asyncProcessRequest(ctx, cmd, callback);
                        } else {
                            NettyRequestProcessor processor = pair.getObject1();
                            RemotingCommand response = processor.processRequest(ctx, cmd);
                            callback.callback(response);
                        }
                    } catch (Throwable e) {
                        log.error("process request exception", e);
                        log.error(cmd.toString());

                        if (!cmd.isOnewayRPC()) {
                            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                                RemotingHelper.exceptionSimpleDesc(e));
                            response.setOpaque(opaque);
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            if (pair.getObject1().rejectRequest()) {
                final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                    "[REJECTREQUEST]system busy, start flow control for a while");
                response.setOpaque(opaque);
                ctx.writeAndFlush(response);
                return;
            }

            try {
                // 将run和Channel以及网络传输对象封装为RequestTask对象
                final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
                // 将RequestTask提交给当前pair的线程池,此时从IO线程切换到业务线程
                pair.getObject2().submit(requestTask);
            } catch (RejectedExecutionException e) {
                //
                if ((System.currentTimeMillis() % 10000) == 0) {
                    log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                        + ", too many requests and system thread pool busy, RejectedExecutionException "
                        + pair.getObject2().toString()
                        + " request code: " + cmd.getCode());
                }

                if (!cmd.isOnewayRPC()) {
                    final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                        "[OVERLOAD]system busy, start flow control for a while");
                    response.setOpaque(opaque);
                    ctx.writeAndFlush(response);
                }
            }
        } else {
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response =
                RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setOpaque(opaque);
            ctx.writeAndFlush(response);
            log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }
    }

    /**
     * Process response from remote peer to the previous issued requests.
     * @param ctx channel handler context.
     * @param cmd response command instance.
     *
     * 发送请求时，同步请求会调用ResponseFuture.waitResponse，收到响应消息之后就会这里通知等待线程。
     * 异步请求的话，也会在这里执行发起请求时注册的callback回调。
     * oneway是单向请求，不关注响应结果
     */

    /**
     *  NettyRemotingAbstract的方法
     *  客户端发送消息之后服务端的响应会被processResponseCommand方法处理
     *  方法主要步骤：
     *  1. 先根据请求id找到之前放到responseTable的ResponseFuture，然后从responseTable中移除ResponseFuture
     *  2. 判断如果存在回调函数，即异步请求，那么调用executeInvokeCallback方法，该方法会执行回调函数的方法
     *  3. 如果没有回调函数，则调用putResponse方法。该方法将响应数据设置到responseCommand中，然后调用countDownLatch.countDown，即倒计数减去1，唤醒等待的线程
     * @param ctx
     * @param cmd
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final int opaque = cmd.getOpaque();
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture != null) {
            // 设置响应客户端RemotingCommand
            responseFuture.setResponseCommand(cmd);

            // 将ResponseFuture从映射表中移除
            responseTable.remove(opaque);

            if (responseFuture.getInvokeCallback() != null) {

                // 执行异步回调处理流程
                executeInvokeCallback(responseFuture);
            } else {

                // 唤醒挂起的业务线程
                responseFuture.putResponse(cmd);
                responseFuture.release();
            }
        } else {
            log.warn("receive response, but not matched any request, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            log.warn(cmd.toString());
        }
    }

    /**
     * Execute callback in callback executor. If callback executor is null, run directly in current thread
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;

        // 获取执行回调的线程池
        ExecutorService executor = this.getCallbackExecutor();
        if (executor != null) {
            try {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {

                            // 再次执行请求时注册的回调逻辑
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            log.warn("execute callback in executor exception, and callback throw", e);
                        } finally {
                            responseFuture.release();
                        }
                    }
                });
            } catch (Exception e) {
                runInThisThread = true;
                log.warn("execute callback in executor exception, maybe executor busy", e);
            }
        } else {
            runInThisThread = true;
        }

        // 再次执行请求时注册的回调逻辑
        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                log.warn("executeInvokeCallback Exception", e);
            } finally {
                responseFuture.release();
            }
        }
    }



    /**
     * Custom RPC hook.
     * Just be compatible with the previous version, use getRPCHooks instead.
     */
    @Deprecated
    protected RPCHook getRPCHook() {
        if (rpcHooks.size() > 0) {
            return rpcHooks.get(0);
        }
        return null;
    }

    /**
     * Custom RPC hooks.
     *
     * @return RPC hooks if specified; null otherwise.
     */
    public List<RPCHook> getRPCHooks() {
        return rpcHooks;
    }


    /**
     * This method specifies thread pool to use while invoking callback methods.
     *
     * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
     * netty event-loop thread.
     */
    public abstract ExecutorService getCallbackExecutor();

    /**
     * <p>
     * This method is periodically invoked to scan and expire deprecated request.
     * </p>
     */
    public void scanResponseTable() {
        // 存放所有超时需要被移除的ResponseFuture
        final List<ResponseFuture> rfList = new LinkedList<ResponseFuture>();
        // 遍历当前responseTable中的ResponseFuture
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();
            // 条件符合，说明超时
            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                // 释放信号量
                rep.release();
                // 从responseTable中移除
                it.remove();
                //将需要被移除的加入到rfList
                rfList.add(rep);
                log.warn("remove timeout request, " + rep);
            }
        }
        // 遍历被移除的ResponseFuture
        for (ResponseFuture rf : rfList) {
            try {
                // 执行回调逻辑
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    /**
     *
     /**
     * NettyRemotingAbstract的方法
     * 执行同步调用
     * 主要执行步骤：
     * 1. 首先创建ResponseFuture（ResponseFuture是保存请求响应结果的，opaque是请求id，将请求id与response的对应关系保存在responseTable（map）中，通过请求id就可以找到对应的response了。）
     * 2. 然后利用netty的Channel连接组件，将消息以同步的方式发送出去，并且添加一个监听器，监听消息是否成功发送，消息发送完毕后会执行回调；
     * 3. ChannelFutureListener回调的时候会进行判断，如果消息成功发送，就设置发送成功并返回，否则设置发送失败的标志和失败原因，并且设置响应结果为null，唤醒阻塞的responseFuture
     * 4. responseFuture被唤醒后会进行一系列判断。如果响应结果为null，那么会根据不同的情况抛出不同的异常，如果响应结果不为null，那么返回响应结果。
     * 5. 最后再finally块中从responseTable中移除响应结果缓存
     *
     * @param channel
     * @param request
     * @param timeoutMillis
     * @return
     * @throws InterruptedException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     */
    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
        final long timeoutMillis)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        // 获取请求id
        final int opaque = request.getOpaque();

        try {
            // 参数一：客户端Channel
            // 参数二：请求id
            // 参数三：超时时间
            // 将客户端Channel消息id封装为ResponseFuture
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null, null);
            // 将请求id和ResponseFuture放入到映射表中
            this.responseTable.put(opaque, responseFuture);
            final SocketAddress addr = channel.remoteAddress();
            // 当 IO 操作完成之后，IO线程会回调 ChannelFuture 中 GenericFutureListener 的 operationComplete 方法，
            // 并把 ChannelFuture 对象当作方法的入参。如果用户需要做上下文相关的操作，需要将上下文信息保存到对应的 ChannelFuture 中。
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {

                    // 处理响应结果
                    if (f.isSuccess()) {
                        // 写入成功
                        responseFuture.setSendRequestOK(true);
                        return;
                    } else {
                        // 写入失败
                        responseFuture.setSendRequestOK(false);
                    }
                    // 写入失败，从映射表中移除掉当前请求的ResponseFuture
                    responseTable.remove(opaque);
                    // 保存异常信息
                    responseFuture.setCause(f.cause());

                    responseFuture.putResponse(null);
                    log.warn("send a request command to channel <" + addr + "> failed.");
                }
            });

            // 业务线程通过countDownLatch挂起，等待客户端的返回结果responseCommand
            // responseFuture同步阻塞等待直到得到响应结果或者到达超时时间
            // 其内部调用了countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            // 执行到这里，说明超时或者出现了异常
            if (null == responseCommand) {
                //如果是发送成功，但是没有响应，表示等待响应超时，那么抛出超时异常
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(addr), timeoutMillis,
                        responseFuture.getCause());
                } else {
                    throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(addr), responseFuture.getCause());
                }
            }
            //返回响应结果
            return responseCommand;
        } finally {
            // 移除responseTable映射表信息
            this.responseTable.remove(opaque);
        }
    }

    /**
     * NettyRemotingAbatract的方法
     * 异步调用实现：
     * invokeAsyncImpl，也和单向发送方式一样
     * 1. 基于Semaphore信号量尝试获取异步发送的资源，通过信号量控制异步消息并发发送的消息数，从而保护系统内存占用。
     *    客户端单向发送的Semaphore信号量默认为65535，即异步消息最大并发为65535
     * 2. 在获取到了信号量资源后，构建SemaphoreReleaseOnlyOnce对象，保证信号量本次只被释放一次，防止并发操作引起线程安全问题，然后就通过channel发送请求即可。
     * 3. 然后创建一个ResponseFuture，设置超时时间，回调函数，然后将本次请求id和respone存入responseTable缓存。
     * 4. 随后执行调用，并添加一个ChannelFutureListener，消息发送完毕会进行回调。
     * 5. 当ChannelFutureListener回调的时候会判断如果消息发送成功，那么设置发送成功并返回，否则如果发送失败了，则移除缓存、设置false、并且执行InvokeCallback#operationComplete回调。
     * @param channel
     * @param request
     * @param timeoutMillis
     * @param invokeCallback
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        // 获取开始时间
        long beginStartTime = System.currentTimeMillis();
        // 获取opaque
        final int opaque = request.getOpaque();
        // 服务器申请信号量
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        // 申请成功
        if (acquired) {
            // 创建释放Semaphore信号量的对象
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            // 计算当前耗时，
            long costTime = System.currentTimeMillis() - beginStartTime;
            // 如果超时时间小于耗时，说明请求超时
            if (timeoutMillis < costTime) {
                // 释放信号量
                once.release();
                throw new RemotingTimeoutException("invokeAsyncImpl call timeout");
            }

            // 创建responseFuture，结果回调处理对象
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis - costTime, invokeCallback, once);
            // 将responseFuture放入responseTable中
            this.responseTable.put(opaque, responseFuture);
            try {
                // 业务线程将数据交给netty的IO线程，进行数据发送，同时注册监听器，更新发送结果
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        if (f.isSuccess()) {
                            responseFuture.setSendRequestOK(true);
                            return;
                        }
                        requestFail(opaque);
                        log.warn("send a request command to channel <{}> failed.", RemotingHelper.parseChannelRemoteAddr(channel));
                    }
                });
            } catch (Exception e) {
                responseFuture.release();
                log.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
            // 执行到这里，说明申请信号量Semaphore失败，当前server请求client的并发度过高
        } else {

            // 如果没有设置超时时间，抛出异常
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {

                // 设置了超时时间，但申请信号量失败，说明当前并发过高，日志打印当前等待队列长度，服务器请求并发量
                String info =
                    String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                        timeoutMillis,
                        this.semaphoreAsync.getQueueLength(),
                        this.semaphoreAsync.availablePermits()
                    );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    private void requestFail(final int opaque) {
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                log.warn("execute callback in requestFail, and callback throw", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * mark the request of the specified channel as fail and to invoke fail callback immediately
     * @param channel the channel which is close already
     */
    protected void failFast(final Channel channel) {
        Iterator<Entry<Integer, ResponseFuture>> it = responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> entry = it.next();
            if (entry.getValue().getProcessChannel() == channel) {
                Integer opaque = entry.getKey();
                if (opaque != null) {
                    requestFail(opaque);
                }
            }
        }
    }

    /**
     *
     * 单向发送请求的逻辑:
     *      将消息发送以后，就不管发送结果了，只要将消息发送出去就行，也不会进行重试；
     *      从源码中也可以看出，消息发送出去以后，只要释放信号量，当发送不成功就打印日志，不管消息发送的结果如何。
     *
     * invokeOnewayImpl方法大致步骤：
     * 1. 首先将请求标记为单向发送；
     * 2. 然后获取Semaphore信号量；
     * 3. 获取到信号量资源，就利用netty连接将消息发送给broker服务器，当发送完成后，就释放信号量，如果发送不成功，就打印日志；
     *      发送消息异常就释放信号量，抛出发送消息异常信息。
     *      获取信号量不成功，也抛出发送消息异常的信息。
     *
     * 这里有个重要的知识点：单向发送信息的信号量最大请求为65535（即单向消息最大并发为65535），当超过这个数字就不能进行发送消息了。
     * 同时，只有获取到信号量才能进行发送消息，这么做就是为了避免请求过多，导致RocketMQ的压力过大而出现性能问题，也起到了限流作用，保护RocketMQ
     * @param channel           通道
     * @param request           请求
     * @param timeoutMillis     超时时间
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        // 标记为单向发送
        request.markOnewayRPC();
        //基于Semaphore信号量尝试获取单向发送的资源，通过信号量控制单向消息并发发送的消息数，从而保护系统内存占用。

        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            //构建SemaphoreReleaseOnlyOnce对象，保证信号量本次只被释放一次，防止并发操作引起线程安全问题
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                //基于Netty的Channel组件，将请求发出去
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        // 释放信号量
                        once.release();
                        if (!f.isSuccess()) {
                            log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                        }
                    }
                });
            } catch (Exception e) {
                once.release();
                log.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            //如果没有获取到信号量资源，已经超时，则抛出异常
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String info = String.format(
                    "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreOnewayValue: %d",
                    timeoutMillis,
                    this.semaphoreOneway.getQueueLength(),
                    this.semaphoreOneway.availablePermits()
                );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    // 处理netty网络连接状态变化事件，如果出现变化会封装为event时间放入到队列中进行处理
    class NettyEventExecutor extends ServiceThread {
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        private final int maxSize = 10000;

        public void putNettyEvent(final NettyEvent event) {
            int currentSize = this.eventQueue.size();
            if (currentSize <= maxSize) {
                this.eventQueue.add(event);
            } else {
                log.warn("event queue size [{}] over the limit [{}], so drop this event {}", currentSize, maxSize, event.toString());
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStopped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            // 处理连接超时事件
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }
}
