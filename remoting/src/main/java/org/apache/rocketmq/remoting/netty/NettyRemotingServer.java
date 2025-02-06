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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

// 服务端网络连接层，主要用在broker和nameSrv通信过程中，namesrv通过NettyRemotingServer来充当网络服务器角色处理broker的请求
public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    // netty服务器端启动对象
    private final ServerBootstrap serverBootstrap;

    //netty worker组线程池
    private final EventLoopGroup eventLoopGroupSelector;

    // netty boss组线程池
    private final EventLoopGroup eventLoopGroupBoss;

    //netty 服务端网络配置
    private final NettyServerConfig nettyServerConfig;

    // 公共线程池，注册处理器时未指定线程池使用公共线程池
    private final ExecutorService publicExecutor;

    // ChannelEventListener有两个实现 BrokerHouseKeepingService和ClientHouseKeepingService
    // BrokerHouseKeepingService(namesrv使用)，Channel发生改变之后做出响应处理
    // ClientHouseKeepingService(broker使用)
    private final ChannelEventListener channelEventListener;

    //定时器，执行scanResponseTable任务  用于清理过期的ResponseFuture
    private final Timer timer = new Timer("ServerHouseKeepingService", true);

    //当向ChannelPipeline中添加handler时指定group后，网络事件传播到当前handler时，事件由当前分配给handler的线程进行处理
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    //netty监听端口号
    private int port = 0;

    //
    private static final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";
    private static final String TLS_HANDLER_NAME = "sslHandler";
    private static final String FILE_REGION_ENCODER_NAME = "fileRegionEncoder";

    // sharable handlers

    // 共享处理器，多个Channel使用同一个对象
    private HandshakeHandler handshakeHandler;

    // 编码器
    private NettyEncoder encoder;

    // 连接管理器，用来处理连接创建，连接关闭，连接超时等事件
    private NettyConnectManageHandler connectionManageHandler;

    // server处理器
    private NettyServerHandler serverHandler;

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig,
        final ChannelEventListener channelEventListener) {

        //服务端主动访问客户并发限制，
        // serverOnewaySemaphoreValue 单向请求的并发限制
        // serverAsyncSemaphoreValue  异步请求并发限制
        super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;

        //公共线程池线程数量
        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums   =  4;
        }

        // 创建公共线程池
        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            //定义线程名前缀
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyServerPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });

        // 如果使用epoll，使用epoll创建boss和worker组工作线程
        if (useEpoll()) {
            this.eventLoopGroupBoss = new EpollEventLoopGroup(1, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyEPOLLBoss_%d", this.threadIndex.incrementAndGet()));
                }
            });

            this.eventLoopGroupSelector = new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                private int threadTotal = nettyServerConfig.getServerSelectorThreads();

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerEPOLLSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });

            // 否则使用NioEventLoopGroup创建boss和worker组工作线程
        } else {
            this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyNIOBoss_%d", this.threadIndex.incrementAndGet()));
                }
            });

            this.eventLoopGroupSelector = new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                private int threadTotal = nettyServerConfig.getServerSelectorThreads();

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerNIOSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });
        }

        // 设置当前的SSL上下文，保证连接安全性
        loadSslContext();
    }

    public void loadSslContext() {
        TlsMode tlsMode = TlsSystemConfig.tlsMode;
        log.info("Server is running in TLS {} mode", tlsMode.getName());

        if (tlsMode != TlsMode.DISABLED) {
            try {
                sslContext = TlsHelper.buildSslContext(false);
                log.info("SSLContext created for server");
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext for server", e);
            } catch (IOException e) {
                log.error("Failed to create SSLContext for server", e);
            }
        }
    }

    private boolean useEpoll() {
        return RemotingUtil.isLinuxPlatform()
            && nettyServerConfig.isUseEpollNativeSelector()
            && Epoll.isAvailable();
    }

    @Override
    public void start() {
        //当向ChannelPipeline中添加handler时指定group后，网络事件传播到当前handler时，事件由当前分配给handler的线程进行处理
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
            nettyServerConfig.getServerWorkerThreads(),
            new ThreadFactory() {

                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyServerCodecThread_" + this.threadIndex.incrementAndGet());
                }
            });
        // 创建共享的处理器Handler
        prepareSharableHandlers();

        ServerBootstrap childHandler =
                //配置服务端启动对象
            this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                    // 设置服务端ServerSocketChannel类型
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                    // 设置服务端ChannelOption类型 设置TCP全连接队列的大小。
                .option(ChannelOption.SO_BACKLOG, nettyServerConfig.getServerSocketBacklog())
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                // 数据包会堆积到一定的数量后一起发送，这就可能导致数据的发送存在一定的延时,如果不希望的发送被延时，则需要将该值设置为true
                .childOption(ChannelOption.TCP_NODELAY, true)
                    // 设置服务器端口
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
                    //当client发起请求连接server会创建ServerSocketChannel对象，给客户端的Pipeline加上处理器，每个Handler设置处理线程
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                            .addLast(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, handshakeHandler)
                            .addLast(defaultEventExecutorGroup,

                                // 编码器
                                encoder,

                                // 解码器
                                new NettyDecoder(),

                                // 连接空闲状态管理器
                                new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                // 连接管理器
                                connectionManageHandler,

                                //服务端处理器
                                serverHandler
                            );
                    }
                });
        if (nettyServerConfig.getServerSocketSndBufSize() > 0) {
            log.info("server set SO_SNDBUF to {}", nettyServerConfig.getServerSocketSndBufSize());
            childHandler.childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize());
        }
        if (nettyServerConfig.getServerSocketRcvBufSize() > 0) {
            log.info("server set SO_RCVBUF to {}", nettyServerConfig.getServerSocketRcvBufSize());
            childHandler.childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize());
        }
        if (nettyServerConfig.getWriteBufferLowWaterMark() > 0 && nettyServerConfig.getWriteBufferHighWaterMark() > 0) {
            log.info("server set netty WRITE_BUFFER_WATER_MARK to {},{}",
                    nettyServerConfig.getWriteBufferLowWaterMark(), nettyServerConfig.getWriteBufferHighWaterMark());
            childHandler.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                    nettyServerConfig.getWriteBufferLowWaterMark(), nettyServerConfig.getWriteBufferHighWaterMark()));
        }
        // 开启内存池，使用的内存池是PooledByteBufAllocator.DEFAULT
        if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
            childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        try {
            // 绑定server端口并启动，拿到一个ChannelFuture
            ChannelFuture sync = this.serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            // 拿到端口信息并保存
            this.port = addr.getPort();
        } catch (InterruptedException e1) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
        }

        // 创建netty网络异常事件处理器
        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }

        // 开启定时任务，每一秒执行一次，扫描ResponseTable中事件，将过期ResponseFuture移除
        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    NettyRemotingServer.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }

    @Override
    public void shutdown() {
        try {
            if (this.timer != null) {
                this.timer.cancel();
            }

            this.eventLoopGroupBoss.shutdownGracefully();

            this.eventLoopGroupSelector.shutdownGracefully();

            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingServer shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    // 注册钩子，执行前置或者后置处理
    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        if (rpcHook != null && !rpcHooks.contains(rpcHook)) {
            rpcHooks.add(rpcHook);
        }
    }

    // 注册业务处理器，将处理器和对应的线程池绑定为一个Pair对象，并且将这个pair对象放入processorTable中， 其值就是pair对象，key就是对应的请求编码RequestCode。
    //  每个请求，都会根据自己携带的RequestCode在processorTable中查找对应的处理器以及对应的执行器线程池来处理请求。 RocketMQ通过这样的方式来提升处理请求的性能。

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        // 建立处理器和线程池之间的关系
        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);

        //requestCode: 请求类型
        this.processorTable.put(requestCode, pair);
    }

    // 注册默认处理器
    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair<NettyRequestProcessor, ExecutorService>(processor, executor);
    }

    @Override
    public int localListenPort() {
        return this.port;
    }

    // 根据requestCode获取对应的pair
    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return processorTable.get(requestCode);
    }

    /**
     * 服务器主动向客户端发起请求时使用的接口，当前方法同步调用，server需要等到client返回结果整个调用才算结束，关注客户端返回结果
     * @param channel  客户端Channel
     * @param request  网络请求对象
     * @param timeoutMillis 超时时长
     * @return RemotingCommand
     * @throws InterruptedException InterruptedException
     * @throws RemotingSendRequestException RemotingSendRequestException
     * @throws RemotingTimeoutException RemotingTimeoutException
     */
    @Override
    public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        return this.invokeSyncImpl(channel, request, timeoutMillis);
    }

    /**
     * 服务端通过异步调用的形式向客户端发起请求，关注client返回结果
     * @param channel 客户端Channel
     * @param request 网络请求对象
     * @param timeoutMillis 超时时长
     * @param invokeCallback 回调处理对象
     * @throws InterruptedException InterruptedException
     * @throws RemotingTooMuchRequestException  RemotingTooMuchRequestException
     * @throws RemotingTimeoutException RemotingTimeoutException
     * @throws RemotingSendRequestException RemotingSendRequestException
     */
    @Override
    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
    }

    /**
     * server主动向client发送请求，但是sever调用成功就结束，不关注client返回结果
     * @param channel 客户端Channel
     * @param request 网络请求对象
     * @param timeoutMillis 超时时长
     * @throws InterruptedException InterruptedException
     * @throws RemotingTooMuchRequestException RemotingTooMuchRequestException
     * @throws RemotingTimeoutException RemotingTimeoutException
     * @throws RemotingSendRequestException RemotingSendRequestException
     */
    @Override
    public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeOnewayImpl(channel, request, timeoutMillis);
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }


    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    private void prepareSharableHandlers() {
        handshakeHandler = new HandshakeHandler(TlsSystemConfig.tlsMode);

        // 创建编码器
        encoder = new NettyEncoder();

        //
        connectionManageHandler = new NettyConnectManageHandler();
        serverHandler = new NettyServerHandler();
    }

    @ChannelHandler.Sharable
    class HandshakeHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private final TlsMode tlsMode;

        private static final byte HANDSHAKE_MAGIC_CODE = 0x16;

        HandshakeHandler(TlsMode tlsMode) {
            this.tlsMode = tlsMode;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {

            // mark the current position so that we can peek the first byte to determine if the content is starting with
            // TLS handshake
            msg.markReaderIndex();

            byte b = msg.getByte(0);

            if (b == HANDSHAKE_MAGIC_CODE) {
                switch (tlsMode) {
                    case DISABLED:
                        ctx.close();
                        log.warn("Clients intend to establish an SSL connection while this server is running in SSL disabled mode");
                        break;
                    case PERMISSIVE:
                    case ENFORCING:
                        if (null != sslContext) {
                            ctx.pipeline()
                                .addAfter(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, TLS_HANDLER_NAME, sslContext.newHandler(ctx.channel().alloc()))
                                .addAfter(defaultEventExecutorGroup, TLS_HANDLER_NAME, FILE_REGION_ENCODER_NAME, new FileRegionEncoder());
                            log.info("Handlers prepended to channel pipeline to establish SSL connection");
                        } else {
                            ctx.close();
                            log.error("Trying to establish an SSL connection but sslContext is null");
                        }
                        break;

                    default:
                        log.warn("Unknown TLS mode");
                        break;
                }
            } else if (tlsMode == TlsMode.ENFORCING) {
                ctx.close();
                log.warn("Clients intend to establish an insecure connection while this server is running in SSL enforcing mode");
            }

            // reset the reader index so that handshake negotiation may proceed as normal.
            msg.resetReaderIndex();

            try {
                // Remove this handler
                ctx.pipeline().remove(this);
            } catch (NoSuchElementException e) {
                log.error("Error while removing HandshakeHandler", e);
            }

            // Hand over this message to the next .
            ctx.fireChannelRead(msg.retain());
        }
    }

    /**
     * Nett服务端收到消息的时候，首先会在NettyServerHandler中进行处理
     */
    @ChannelHandler.Sharable
    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            // 处理客户端的请求
            processMessageReceived(ctx, msg);
        }
    }

    @ChannelHandler.Sharable
    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelRegistered {}", remoteAddress);
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddress);
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]", remoteAddress);
            super.channelActive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]", remoteAddress);
            super.channelInactive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        // 通过监控Channel状态事件，关闭连接，发布连接关闭的event事件
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]", remoteAddress);
                    RemotingUtil.closeChannel(ctx.channel());
                    if (NettyRemotingServer.this.channelEventListener != null) {
                        NettyRemotingServer.this
                            .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }

            RemotingUtil.closeChannel(ctx.channel());
        }
    }
}
