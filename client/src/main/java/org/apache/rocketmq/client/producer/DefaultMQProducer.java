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
package org.apache.rocketmq.client.producer;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.EndTransactionTraceHookImpl;
import org.apache.rocketmq.client.trace.hook.SendMessageTraceHookImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;

/**
 * This class is the entry point for applications intending to send messages. </p>
 *
 * It's fine to tune fields which exposes getter/setter methods, but keep in mind, all of them should work well out of
 * box for most scenarios. </p>
 *
 * This class aggregates various <code>send</code> methods to deliver messages to broker(s). Each of them has pros and
 * cons; you'd better understand strengths and weakness of them before actually coding. </p>
 *
 * <p> <strong>Thread Safety:</strong> After configuring and starting process, this class can be regarded as thread-safe
 * and used among multiple threads context. </p>
 */

/**
 * 1，业务层使用对象，业务使用该对象完成消息发送
 * 2，管理配置，生产者要有对应的broker，queue等各种配置信息
 * 是应用用来投递消息的入口，开箱即用，可通过无参构造方法快速创建一个生产者。主要负责消息的发送，支持同步/异步/oneway的发送方式，这些发送方式均支持批量发送。
 * 可以通过该类提供的getter/setter方法，调整发送者的参数。DefaultMQProducer提供了多个send方法，每个send方法略有不同
 */

public class DefaultMQProducer extends ClientConfig implements MQProducer {

    /**
     * Wrapping internal implementations for virtually all methods presented in this class.
     */

    // 	生产者的内部默认实现
    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;
    private final InternalLogger log = ClientLogger.getLog();

    // 重试的ResponseCodes，发送消息时如果client收到以下ResponseCodes会尝试重试发送
    private final Set<Integer> retryResponseCodes = new CopyOnWriteArraySet<Integer>(Arrays.asList(
            ResponseCode.TOPIC_NOT_EXIST,
            ResponseCode.SERVICE_NOT_AVAILABLE,
            ResponseCode.SYSTEM_ERROR,
            ResponseCode.NO_PERMISSION,
            ResponseCode.NO_BUYER_ID,
            ResponseCode.NOT_IN_CURRENT_UNIT
    ));

    /**
     * Producer group conceptually aggregates all producer instances of exactly same role, which is particularly
     * important when transactional messages are involved. </p>
     *
     * For non-transactional messages, it does not matter as long as it's unique per process. </p>
     *
     * See <a href="http://rocketmq.apache.org/docs/core-concept/">core concepts</a> for more discussion.
     */

    /**
     * 生产者所属组，消息服务器在回查事务状态时会随机选择该组中任何一个生产者发起的事务回查请求。
     */
    private String producerGroup;

    /**
     * Just for testing or demo program
     */

    // broker端写死的主体队列信息，一是可以用来测试。二是当发送消息指定的topic信息在nameSrv没有找到路由信息，可以使用TBW102为模板创建topic
    private String createTopicKey = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;

    /**
     * Number of queues to create per default topic.
     */

    // broker默认每个topic创建的队列数量
    private volatile int defaultTopicQueueNums = 4;

    /**
     * Timeout for sending messages.
     */

    // 发送消息超时时间阈值  3s
    private int sendMsgTimeout = 3000;

    /**
     * Compress message body threshold, namely, message body larger than 4k will be compressed on default.
     */

    // 当发送的消息body大小超过4k之后默认使用压缩
    private int compressMsgBodyOverHowmuch = 1024 * 4;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in synchronous mode. </p>
     *
     * This may potentially cause message duplication which is up to application developers to resolve.
     */

    // 发送失败之后重试发送次数  加上第一次发送，也就说每个消息最多发送三次
    private int retryTimesWhenSendFailed = 2;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in asynchronous mode. </p>
     *  下面是异步发送参数
     * This may potentially cause message duplication which is up to application developers to resolve.
     */

    // 异步消息发送失败之后重试次数, 加上第一次发送，也就说每个消息最多发送三次
    private int retryTimesWhenSendAsyncFailed = 2;

    /**
     * Indicate whether to retry another broker on sending failure internally.
     */

    // 消息重试时选择另外一个Broker，是否不等待存储结果就返回，默认为false。
    private boolean retryAnotherBrokerWhenNotStoreOK = false;

    /**
     * Maximum allowed message body size in bytes.
     */

    // 异步发送的消息body最大阈值  4M
    private int maxMessageSize = 1024 * 1024 * 4; // 4M

    /**
     * Interface of asynchronous transfer data
     */

    // 消息轨迹相关
    private TraceDispatcher traceDispatcher = null;

    /**
     * Default constructor.
     */
    public DefaultMQProducer() {
        this(null, MixAll.DEFAULT_PRODUCER_GROUP, null);
    }

    /**
     * Constructor specifying the RPC hook.
     *
     * @param rpcHook RPC hook to execute per each remoting command execution.
     */
    public DefaultMQProducer(RPCHook rpcHook) {
        this(null, MixAll.DEFAULT_PRODUCER_GROUP, rpcHook);
    }

    /**
     * Constructor specifying producer group.
     *
     * @param producerGroup Producer group, see the name-sake field.
     */
    public DefaultMQProducer(final String producerGroup) {
        this(null, producerGroup, null);
    }

    /**
     * Constructor specifying producer group.
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook RPC hook to execute per each remoting command execution.
     * @param enableMsgTrace Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default
     * trace topic name.
     */
    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook, boolean enableMsgTrace,
        final String customizedTraceTopic) {
        this(null, producerGroup, rpcHook, enableMsgTrace, customizedTraceTopic);
    }

    /**
     * Constructor specifying producer group.
     *
     * @param namespace Namespace for this MQ Producer instance.
     * @param producerGroup Producer group, see the name-sake field.
     */
    public DefaultMQProducer(final String namespace, final String producerGroup) {
        this(namespace, producerGroup, null);
    }

    /**
     * Constructor specifying both producer group and RPC hook.
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook RPC hook to execute per each remoting command execution.
     */
    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook) {
        this(null, producerGroup, rpcHook);
    }

    /**
     * Constructor specifying namespace, producer group and RPC hook.
     *
     * @param namespace Namespace for this MQ Producer instance.
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook RPC hook to execute per each remoting command execution.
     */
    public DefaultMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook) {

        // 生产者namespace
        this.namespace = namespace;

        // 生产者组
        this.producerGroup = producerGroup;

        // 生产者门面对象
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
    }

    /**
     * Constructor specifying producer group and enabled msg trace flag.
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param enableMsgTrace Switch flag instance for message trace.
     */
    public DefaultMQProducer(final String producerGroup, boolean enableMsgTrace) {
        this(null, producerGroup, null, enableMsgTrace, null);
    }

    /**
     * Constructor specifying producer group, enabled msgTrace flag and customized trace topic name.
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param enableMsgTrace Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default
     * trace topic name.
     */
    public DefaultMQProducer(final String producerGroup, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(null, producerGroup, null, enableMsgTrace, customizedTraceTopic);
    }

    /**
     * Constructor specifying namespace, producer group, RPC hook, enabled msgTrace flag and customized trace topic
     * name.
     *
     * @param namespace Namespace for this MQ Producer instance.
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook RPC hook to execute per each remoting command execution.
     * @param enableMsgTrace Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default
     * trace topic name.
     */
    public DefaultMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook,
        boolean enableMsgTrace, final String customizedTraceTopic) {
        this.namespace = namespace;
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
        //if client open the message trace feature
        // 条件成立，说明客户端开启消息轨迹跟踪
        if (enableMsgTrace) {
            try {

                // 创建异步的消息轨迹转发
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(producerGroup, TraceDispatcher.Type.PRODUCE, customizedTraceTopic, rpcHook);

                //
                dispatcher.setHostProducer(this.defaultMQProducerImpl);
                traceDispatcher = dispatcher;
                this.defaultMQProducerImpl.registerSendMessageHook(
                    new SendMessageTraceHookImpl(traceDispatcher));
                this.defaultMQProducerImpl.registerEndTransactionHook(
                    new EndTransactionTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    @Override
    public void setUseTLS(boolean useTLS) {
        super.setUseTLS(useTLS);
        if (traceDispatcher instanceof AsyncTraceDispatcher) {
            ((AsyncTraceDispatcher) traceDispatcher).getTraceProducer().setUseTLS(useTLS);
        }
    }
    
    /**
     * Start this producer instance. </p>
     *
     * <strong> Much internal initializing procedures are carried out to make this instance prepared, thus, it's a must
     * to invoke this method before sending or querying messages. </strong> </p>
     *
     * @throws MQClientException if there is any unexpected error.
     */

    // 生产者实例启动方法
    @Override
    public void start() throws MQClientException {

        // 设置生产者组组名，如果传递了namespace，则设置组名为namespace%group
        this.setProducerGroup(withNamespace(this.producerGroup));

        // 生产者实现对象启动
        this.defaultMQProducerImpl.start();
        if (null != traceDispatcher) {
            try {
                traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    /**
     * This method shuts down this producer instance and releases related resources.
     */
    @Override
    public void shutdown() {
        this.defaultMQProducerImpl.shutdown();
        if (null != traceDispatcher) {
            traceDispatcher.shutdown();
        }
    }

    /**
     * Fetch message queues of topic <code>topic</code>, to which we may send/publish messages.
     *
     * @param topic Topic to fetch.
     * @return List of message queues readily to send messages to
     * @throws MQClientException if there is any client error.
     */
    // 查找该主题下所有的消息队列。
    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        return this.defaultMQProducerImpl.fetchPublishMessageQueues(withNamespace(topic));
    }

    /**
     * Send message in synchronous mode. This method returns only when the sending procedure totally completes. </p>
     *
     * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
     * {@link #retryTimesWhenSendFailed} times before claiming failure. As a result, multiple messages may be potentially
     * delivered to broker(s). It's up to the application developers to resolve potential duplication issue.
     *
     * @param msg Message to send.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    // 同步发送消息，具体发送到主题中的哪个消息队列由负载算法决定。

    /**
     * 发送单条消息时，消息体的内容将保存在body中。发送批量消息时，需要将多条消息体的内容存储在body中。如何存储更便于服务端
     * 正确解析每条消息呢？RocketMQ采取的方式是，对单条消息内容使用固定格式进行存储
     * 总长度4字节--魔数4字节--boyCRC4字节--Flag4字节--body长度4字节--消息体N字节--属性长度2字节--扩展属性N字节
     */
    @Override
    public SendResult send(
        Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg);
    }

    /**
     * Same to {@link #send(Message)} with send timeout specified in addition.
     *
     * @param msg Message to send.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    // 同步发送消息，如果发送超过timeout则抛出超时异常。
    @Override
    public SendResult send(Message msg,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, timeout);
    }

    /**
     * Send message to broker asynchronously. </p>
     *
     * This method returns immediately. On sending completion, <code>sendCallback</code> will be executed. </p>
     *
     * Similar to {@link #send(Message)}, internal implementation would potentially retry up to {@link
     * #retryTimesWhenSendAsyncFailed} times before claiming sending failure, which may yield message duplication and
     * application developers are the one to resolve this potential issue.
     *
     * @param msg Message to send.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    // 异步发送消息，sendCallback参数是消息发送成功后的回调方法。
    @Override
    public void send(Message msg,
        SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, sendCallback);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with send timeout specified in addition.
     *
     * @param msg message to send.
     * @param sendCallback Callback to execute.
     * @param timeout send timeout.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    //异步发送消息，sendCallback参数是消息发送成功后的回调方法。超过timeout抛出异常
    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, sendCallback, timeout);
    }

    /**
     * Similar to <a href="https://en.wikipedia.org/wiki/User_Datagram_Protocol">UDP</a>, this method won't wait for
     * acknowledgement from broker before return. Obviously, it has maximums throughput yet potentials of message loss.
     *
     * @param msg Message to send.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    // 单向方式发送消息，且发送到指定的消息队列。
    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg);
    }

    /**
     * Same to {@link #send(Message)} with target message queue specified in addition.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq));
    }

    /**
     * Same to {@link #send(Message)} with target message queue and send timeout specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), timeout);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with target message queue specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), sendCallback);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with target message queue and send timeout specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @param timeout Send timeout.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), sendCallback, timeout);
    }

    /**
     * Same to {@link #sendOneway(Message)} with target message queue specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg,
        MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg, queueWithNamespace(mq));
    }

    /**
     * Same to {@link #send(Message)} with message queue selector specified.
     *
     * @param msg Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg Argument to work along with message queue selector.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    // 消息发送，指定消息选择算法，覆盖消息生产者默认的消息队列负载。
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, selector, arg);
    }

    /**
     * Same to {@link #send(Message, MessageQueueSelector, Object)} with send timeout specified.
     *
     * @param msg Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg Argument to work along with message queue selector.
     * @param timeout Send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, selector, arg, timeout);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with message queue selector specified.
     *
     * @param msg Message to send.
     * @param selector Message selector through which to get target message queue.
     * @param arg Argument used along with message queue selector.
     * @param sendCallback callback to execute on sending completion.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback);
    }

    /**
     * Same to {@link #send(Message, MessageQueueSelector, Object, SendCallback)} with timeout specified.
     *
     * @param msg Message to send.
     * @param selector Message selector through which to get target message queue.
     * @param arg Argument used along with message queue selector.
     * @param sendCallback callback to execute on sending completion.
     * @param timeout Send timeout.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback, timeout);
    }

    /**
     * Send request message in synchronous mode. This method returns only when the consumer consume the request message and reply a message. </p>
     *
     * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
     * {@link #retryTimesWhenSendFailed} times before claiming failure. As a result, multiple messages may be potentially
     * delivered to broker(s). It's up to the application developers to resolve potential duplication issue.
     *
     * @param msg request message to send
     * @param timeout request timeout
     * @return reply message
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws RequestTimeoutException if request timeout.
     */
    @Override
    public Message request(final Message msg, final long timeout) throws RequestTimeoutException, MQClientException,
        RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, timeout);
    }

    /**
     * Request asynchronously. </p>
     * This method returns immediately. On receiving reply message, <code>requestCallback</code> will be executed. </p>
     *
     * Similar to {@link #request(Message, long)}, internal implementation would potentially retry up to {@link
     * #retryTimesWhenSendAsyncFailed} times before claiming sending failure, which may yield message duplication and
     * application developers are the one to resolve this potential issue.
     *
     * @param msg request message to send
     * @param requestCallback callback to execute on request completion.
     * @param timeout request timeout
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQBrokerException if there is any broker error.
     */
    @Override
    public void request(final Message msg, final RequestCallback requestCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, requestCallback, timeout);
    }

    /**
     * Same to {@link #request(Message, long)}  with message queue selector specified.
     *
     * @param msg request message to send
     * @param selector message queue selector, through which we get target message queue to deliver message to.
     * @param arg argument to work along with message queue selector.
     * @param timeout timeout of request.
     * @return reply message
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws RequestTimeoutException if request timeout.
     */
    @Override
    public Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final long timeout) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException, RequestTimeoutException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, selector, arg, timeout);
    }

    /**
     * Same to {@link #request(Message, RequestCallback, long)} with target message selector specified.
     *
     * @param msg requst message to send
     * @param selector message queue selector, through which we get target message queue to deliver message to.
     * @param arg argument to work along with message queue selector.
     * @param requestCallback callback to execute on request completion.
     * @param timeout timeout of request.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQBrokerException if there is any broker error.
     */
    @Override
    public void request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final RequestCallback requestCallback, final long timeout) throws MQClientException, RemotingException,
        InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, selector, arg, requestCallback, timeout);
    }

    /**
     * Same to {@link #request(Message, long)}  with target message queue specified in addition.
     *
     * @param msg request message to send
     * @param mq target message queue.
     * @param timeout request timeout
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws RequestTimeoutException if request timeout.
     */
    @Override
    public Message request(final Message msg, final MessageQueue mq, final long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, mq, timeout);
    }

    /**
     * Same to {@link #request(Message, RequestCallback, long)} with target message queue specified.
     *
     * @param msg request message to send
     * @param mq target message queue.
     * @param requestCallback callback to execute on request completion.
     * @param timeout timeout of request.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQBrokerException if there is any broker error.
     */
    @Override
    public void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, mq, requestCallback, timeout);
    }

    /**
     * Same to {@link #sendOneway(Message)} with message queue selector specified.
     *
     * @param msg Message to send.
     * @param selector Message queue selector, through which to determine target message queue to deliver message
     * @param arg Argument used along with message queue selector.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
    }

    /**
     * This method is to send transactional messages.
     *
     * @param msg Transactional message to send.
     * @param tranExecuter local transaction executor.
     * @param arg Argument used along with local transaction executor.
     * @return Transaction result.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter,
        final Object arg)
        throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }

    /**
     * This method is used to send transactional messages.
     *
     * @param msg Transactional message to send.
     * @param arg Argument used along with local transaction executor.
     * @return Transaction result.
     * @throws MQClientException
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg,
        Object arg) throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param key accesskey
     * @param newTopic topic name
     * @param queueNum topic's queue number
     * @throws MQClientException if there is any client error.
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, withNamespace(newTopic), queueNum, 0);
    }

    /**
     * Create a topic on broker. This method will be removed in a certain version after April 5, 2020, so please do not
     * use this method.
     *
     * @param key accesskey
     * @param newTopic topic name
     * @param queueNum topic's queue number
     * @param topicSysFlag topic system flag
     * @throws MQClientException if there is any client error.
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQProducerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
    }

    /**
     * Search consume queue offset of the given time stamp.
     *
     * @param mq Instance of MessageQueue
     * @param timestamp from when in milliseconds.
     * @return Consume queue offset.
     * @throws MQClientException if there is any client error.
     */

    // 根据时间戳从队列中查找其偏移量
    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQProducerImpl.searchOffset(queueWithNamespace(mq), timestamp);
    }

    /**
     * Query maximum offset of the given message queue.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param mq Instance of MessageQueue
     * @return maximum offset of the given consume queue.
     * @throws MQClientException if there is any client error.
     */
    @Deprecated
    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.maxOffset(queueWithNamespace(mq));
    }

    /**
     * Query minimum offset of the given message queue.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param mq Instance of MessageQueue
     * @return minimum offset of the given message queue.
     * @throws MQClientException if there is any client error.
     */

    // 查找该消息队列中最大的物理偏移量。
    @Deprecated
    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.minOffset(queueWithNamespace(mq));
    }

    /**
     * Query the earliest message store time.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param mq Instance of MessageQueue
     * @return earliest message store time.
     * @throws MQClientException if there is any client error.
     */
    //查找该消息队列中最小的物理偏移量。
    @Deprecated
    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
    }

    /**
     * Query message of the given offset message ID.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param offsetMsgId message id
     * @return Message specified.
     * @throws MQBrokerException if there is any broker error.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    // 根据消息偏移量查找消息
    @Deprecated
    @Override
    public MessageExt viewMessage(
        String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQProducerImpl.viewMessage(offsetMsgId);
    }

    /**
     * Query message by key.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param topic message topic
     * @param key message key index word
     * @param maxNum max message number
     * @param begin from when
     * @param end to when
     * @return QueryResult instance contains matched messages.
     * @throws MQClientException if there is any client error.
     * @throws InterruptedException if the thread is interrupted.
     */
    // 根据条件查询消息
    @Deprecated
    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.defaultMQProducerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
    }

    /**
     * Query message of the given message ID.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param topic Topic
     * @param msgId Message ID
     * @return Message specified.
     * @throws MQBrokerException if there is any broker error.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    // 根据主题与消息ID查找消息
    @Deprecated
    @Override
    public MessageExt viewMessage(String topic,
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            return this.viewMessage(msgId);
        } catch (Exception ignored) {
        }
        return this.defaultMQProducerImpl.queryMessageByUniqKey(withNamespace(topic), msgId);
    }

    /**
     * 批量消息发送是将同一主题的多条消息一起打包发送到消息服务端，减少网络调用次数，提高网络传输效率。当然，并不是在同一批
     * 次中发送的消息数量越多，性能就越好，判断依据是单条消息的长度，如果单条消息内容比较长，则打包发送多条消息会影响其他线程
     * 发送消息的响应时间，并且单批次消息发送总长度不能超过DefaultMQProducer#maxMessageSize。批量发送消息要解决的是如何将这些消
     * 息编码，以便服务端能够正确解码每条消息的内容。
     * @param msgs
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    @Override
    public SendResult send(
        Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs));
    }

    @Override
    public SendResult send(Collection<Message> msgs,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), timeout);
    }

    @Override
    public SendResult send(Collection<Message> msgs,
        MessageQueue messageQueue) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue);
    }

    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue messageQueue,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue, timeout);
    }

    @Override
    public void send(Collection<Message> msgs, SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQProducerImpl.send(batch(msgs), sendCallback);
    }

    @Override
    public void send(Collection<Message> msgs, SendCallback sendCallback,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQProducerImpl.send(batch(msgs), sendCallback, timeout);
    }

    @Override
    public void send(Collection<Message> msgs, MessageQueue mq,
        SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQProducerImpl.send(batch(msgs), queueWithNamespace(mq), sendCallback);
    }

    @Override
    public void send(Collection<Message> msgs, MessageQueue mq,
        SendCallback sendCallback, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.defaultMQProducerImpl.send(batch(msgs), queueWithNamespace(mq), sendCallback, timeout);
    }

    /**
     * Sets an Executor to be used for executing callback methods. If the Executor is not set, {@link
     * NettyRemotingClient#publicExecutor} will be used.
     *
     * @param callbackExecutor the instance of Executor
     */
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.defaultMQProducerImpl.setCallbackExecutor(callbackExecutor);
    }

    /**
     * Sets an Executor to be used for executing asynchronous send. If the Executor is not set, {@link
     * DefaultMQProducerImpl#defaultAsyncSenderExecutor} will be used.
     *
     * @param asyncSenderExecutor the instance of Executor
     */
    public void setAsyncSenderExecutor(final ExecutorService asyncSenderExecutor) {
        this.defaultMQProducerImpl.setAsyncSenderExecutor(asyncSenderExecutor);
    }

    /**
     * Add response code for retrying.
     *
     * @param responseCode response code, {@link ResponseCode}
     */
    public void addRetryResponseCode(int responseCode) {
        this.retryResponseCodes.add(responseCode);
    }

    /**
     * 将一批消息封装成MessageBatch对象。Message-Batch继承自Message对象，内部持有List<Message> messages。
     * 这样一来，批量消息发送与单条消息发送的处理流程就完全一样了。MessageBatch只需要将该集合中每条消息
     * 的消息体聚合成一个byte[]数组，在消息服务端能够从该byte[]数组中正确解析出消息
     * @param msgs 消息列表
     * @return MessageBatch
     * @throws MQClientException
     */
    private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
        MessageBatch msgBatch;
        try {
            //1 将消息集合转换为MessageBatch
            msgBatch = MessageBatch.generateFromList(msgs);
            for (Message message : msgBatch) {
                Validators.checkMessage(message, this);
                //2 迭代每个消息，逐一设置Unique Key
                MessageClientIDSetter.setUniqID(message);
                message.setTopic(withNamespace(message.getTopic()));
            }
            // MessageBatch的body属性是byte[]类型，需要将MessageBatch对象编码成byte[]数组
            msgBatch.setBody(msgBatch.encode());
        } catch (Exception e) {
            throw new MQClientException("Failed to initiate the MessageBatch", e);
        }
        msgBatch.setTopic(withNamespace(msgBatch.getTopic()));
        return msgBatch;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getCreateTopicKey() {
        return createTopicKey;
    }

    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }

    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }

    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }

    @Deprecated
    public DefaultMQProducerImpl getDefaultMQProducerImpl() {
        return defaultMQProducerImpl;
    }

    public boolean isRetryAnotherBrokerWhenNotStoreOK() {
        return retryAnotherBrokerWhenNotStoreOK;
    }

    public void setRetryAnotherBrokerWhenNotStoreOK(boolean retryAnotherBrokerWhenNotStoreOK) {
        this.retryAnotherBrokerWhenNotStoreOK = retryAnotherBrokerWhenNotStoreOK;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public boolean isSendMessageWithVIPChannel() {
        return isVipChannelEnabled();
    }

    public void setSendMessageWithVIPChannel(final boolean sendMessageWithVIPChannel) {
        this.setVipChannelEnabled(sendMessageWithVIPChannel);
    }

    public long[] getNotAvailableDuration() {
        return this.defaultMQProducerImpl.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.defaultMQProducerImpl.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.defaultMQProducerImpl.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.defaultMQProducerImpl.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.defaultMQProducerImpl.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.defaultMQProducerImpl.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public int getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(final int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }

    public TraceDispatcher getTraceDispatcher() {
        return traceDispatcher;
    }

    public Set<Integer> getRetryResponseCodes() {
        return retryResponseCodes;
    }
}
