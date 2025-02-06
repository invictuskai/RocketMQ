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
package org.apache.rocketmq.broker;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;
// broker启动类
public class BrokerStartup {
    public static Properties properties = null;
    public static CommandLine commandLine = null;
    public static String configFile = null;
    public static InternalLogger log;

    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    public static BrokerController start(BrokerController controller) {
        try {

            controller.start();

            String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                + controller.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    public static BrokerController createBrokerController(String[] args) {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        try {
            //PackageConflictDetect.detectFastjson();
            // 构建命令行操作对象
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options),
                new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
            }

            // broker的配置类，用来封装绝大多数基本配置信息，比如ROCKETMQ_HOME
            final BrokerConfig brokerConfig = new BrokerConfig();
            // NettyServer配置类，broker作为服务端接受来自客户端(producer/consumer)的请求
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            //NettyClient配置类，broker作为客户端和nameSrv交互
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();

            nettyClientConfig.setUseTLS(Boolean.parseBoolean(System.getProperty(TLS_ENABLE,
                String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING))));
            // 设置netty的服务端监听端口号
            nettyServerConfig.setListenPort(10911);
            // 存储配置类，用来封装broker的存储配置信息，比如存储路径、存储大小、存储方式等
            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

            /**
             * Slave-broker的参数配置信息
             * 如果broker的角色是slave(默认broker的角色是异步master)，设置命中消息在内存的最大比利
             * 超过设置的最大内存，消息将被置换出内存。
             */
            if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }

            /**
             * 解析外部配置文件
             * 判断命令行中是否包含-c字符(是否通过命令行执行配置文件)
             */
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);

                    properties2SystemEnv(properties);
                    // 解析broker配置信息
                    MixAll.properties2Object(properties, brokerConfig);
                    // 设置broker的nettyServerConfig和nettyClientConfig的配置信息
                    MixAll.properties2Object(properties, nettyServerConfig);
                    MixAll.properties2Object(properties, nettyClientConfig);
                    // 设置broker的messageStoreConfig的配置信息
                    MixAll.properties2Object(properties, messageStoreConfig);

                    BrokerPathConfigHelper.setBrokerConfigPath(file);
                    in.close();
                }
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

            if (null == brokerConfig.getRocketmqHome()) {
                System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
                System.exit(-2);
            }

            // 获取brokerConfig中的nameSrv地址并进行校验
            String namesrvAddr = brokerConfig.getNamesrvAddr();
            if (null != namesrvAddr) {
                try {
                    String[] addrArray = namesrvAddr.split(";");
                    for (String addr : addrArray) {
                        RemotingUtil.string2SocketAddress(addr);
                    }
                } catch (Exception e) {
                    System.out.printf(
                        "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                        namesrvAddr);
                    System.exit(-3);
                }
            }
            /**
             * 设置 校验brokerId brokerId =0为master，brokerId >0为slave
             */
            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= 0) {
                        System.out.printf("Slave's brokerId must be > 0");
                        System.exit(-3);
                    }

                    break;
                default:
                    break;
            }
            // 是否开启多副本 主从切换集群
            if (messageStoreConfig.isEnableDLegerCommitLog()) {
                brokerConfig.setBrokerId(-1);
            }

            // 设置高可用通信监听关口 默认端口号+1 默认端口是10911。这个端口主要用于主从同步之类的高可用操作
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            System.setProperty("brokerLogDir", "");
            if (brokerConfig.isIsolateLogEnable()) {
                System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + brokerConfig.getBrokerId());
            }
            if (brokerConfig.isIsolateLogEnable() && messageStoreConfig.isEnableDLegerCommitLog()) {
                System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + messageStoreConfig.getdLegerSelfId());
            }
            configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");
            // 判断命令行中是否包含字符'p'（printConfigItem）和'm'，如果存在则打印配置信息并结束jvm运行，没有的话就不用管
            if (commandLine.hasOption('p')) {
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig);
                MixAll.printObjectProperties(console, nettyServerConfig);
                MixAll.printObjectProperties(console, nettyClientConfig);
                MixAll.printObjectProperties(console, messageStoreConfig);
                System.exit(0);
            } else if (commandLine.hasOption('m')) {
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig, true);
                MixAll.printObjectProperties(console, nettyServerConfig, true);
                MixAll.printObjectProperties(console, nettyClientConfig, true);
                MixAll.printObjectProperties(console, messageStoreConfig, true);
                System.exit(0);
            }
            // 打印当前broker的配置日志
            log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
            MixAll.printObjectProperties(log, brokerConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);
            MixAll.printObjectProperties(log, nettyClientConfig);
            MixAll.printObjectProperties(log, messageStoreConfig);
            // 实例化 BrokerController，内部主要初始化了一些配置类、manager类、处理器、线程池等
            final BrokerController controller = new BrokerController(
                brokerConfig,
                nettyServerConfig,
                nettyClientConfig,
                messageStoreConfig);
            // remember all configs to prevent discard
            // 将所有的-c的外部配置信息保存到BrokerController中的Configuration对象属性的allConfigs属性中
            controller.getConfiguration().registerConfig(properties);

            // 初始化BrokerController
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }
            // 添加关闭钩子方法，在Broker关闭之前执行，进行一些内存清理、对象销毁等操作
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);

                @Override
                public void run() {
                    synchronized (this) {
                        log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long beginTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                            log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
