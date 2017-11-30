/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.XAConnection;

import org.apache.activemq.advisory.DestinationSource;
import org.apache.activemq.blob.BlobTransferPolicy;
import org.apache.activemq.broker.region.policy.RedeliveryPolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ControlCommand;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.management.JMSConnectionStatsImpl;
import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.management.StatsCapable;
import org.apache.activemq.management.StatsImpl;
import org.apache.activemq.state.CommandVisitorAdapter;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.RequestTimedOutIOException;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.ServiceSupport;
import org.apache.activemq.util.ThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveMQConnection implements Connection, TopicConnection, QueueConnection, StatsCapable, Closeable, TransportListener, EnhancedConnection {
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQConnection.class);


    public static final String DEFAULT_USER = ActiveMQConnectionFactory.DEFAULT_USER;
    public static final String DEFAULT_PASSWORD = ActiveMQConnectionFactory.DEFAULT_PASSWORD;
    public static final String DEFAULT_BROKER_URL = ActiveMQConnectionFactory.DEFAULT_BROKER_URL;
    public static int DEFAULT_THREAD_POOL_SIZE = 1000;//默认线程池大小


    public final ConcurrentMap<ActiveMQTempDestination, ActiveMQTempDestination> activeTempDestinations = new ConcurrentHashMap<ActiveMQTempDestination, ActiveMQTempDestination>();

    /**
     * 是否异步分发消息（true:是，false:否）
     */
    protected boolean dispatchAsync=true;

    /**
     * 客户端session是否使用异步转发，和上面的dispatchAsync参数设置无关。
     * 即当底层的Trasport接收到broker发送的消息后，会交付给session，那么session是否会采用异步的方式将消息传递给consumer，默认值是true:采用异步方式转发。
     * 如果为false，表示使用同步。当consumer使用receive()获取消息时，那么session将会把消息添加到consumer的本地queue，然后唤醒receive等待；当consumer使用messageListener异步侦听消息时，将会调用其onMessage()方法直到方法执行完毕，然后返回。
     * 如果为true，表示使用异步。session接受的消息，将会首先放入session buffer中(队列)，那么此后的线程池将会负责移除buffer中的消息，并转发给相应的consumer。
     * 当session中有多个consumer时，或者Transport中消息量比较密集，异步方式是最佳的。如果session中只有一个consumer，或者transport中消息量很少，使用异步并不能明显的提升性能。
     */
    protected boolean alwaysSessionAsync = true;

    /**
     * sessionTask运行的线程池工厂类
     */
    private TaskRunnerFactory sessionTaskRunner;

    private final ThreadPoolExecutor executor;// 线程池执行对象属性

    // 连接状态变量
    private final ConnectionInfo info; // 连接信息对象
    private ExceptionListener exceptionListener; // 异常监听器
    private ClientInternalExceptionListener clientInternalExceptionListener; // 客户端中断异常监听器
    private boolean clientIDSet; // clientID是否设置属性
    private boolean isConnectionInfoSentToBroker;// 连接信息数据包是否发送给broker端标志
    private boolean userSpecifiedClientID;// 用户是否指定clientID

    // 配置选项变量
    private ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();//为不同类型的消费者定义定义消息预取策略对象属性
    private BlobTransferPolicy blobTransferPolicy;// 二进制大对象如何在producer、broker、consumer之间传输、转移的策略配置类对象属性
    private RedeliveryPolicyMap redeliveryPolicyMap;// destination配置策略类对象属性
    private MessageTransformer transformer;// 消息转换类对象属性

    private boolean disableTimeStampsByDefault;// 是否默认禁止时间戳
    private boolean optimizedMessageDispatch = true;// 是否优化消息的分发
    private boolean copyMessageOnSend = true;// 是否在发送消息时对消息进行copy
    private boolean useCompression;// 是否压缩
    private boolean objectMessageSerializationDefered;
    private boolean useAsyncSend;// 是否使用已不发送（producer到broker）
    private boolean optimizeAcknowledge;// 是否开启消息的ack优化(这是ActiveMQ对于consumer在消息消费时，对消息ACK的优化选项，也是consumer端最重要的优化参数之一)
    private long optimizeAcknowledgeTimeOut = 0;// consumer接收到消息之后ack的超时时间设置
    private long optimizedAckScheduledAckInterval = 0;// 优化消息ack的预定ack间隔
    private boolean nestedMapAndListEnabled = true;// 嵌套的map和list数据是否允许
    private boolean useRetroactiveConsumer;// 是否使用消费者追溯
    private boolean exclusiveConsumer;
    private boolean alwaysSyncSend;
    private int closeTimeout = 15000;
    private boolean watchTopicAdvisories = true;
    private long warnAboutUnstartedConnectionTimeout = 500L;
    private int sendTimeout =0;
    private boolean sendAcksAsync=true;
    private boolean checkForDuplicates = true;
    private boolean queueOnlyConnection = false;
    private boolean consumerExpiryCheckEnabled = true;

    private final Transport transport;// 消息的发送、消费transport类对象(Transport类和ActiveMQConnection类之间是组合模式)
    private final IdGenerator clientIdGenerator;// clientID生成器
    private final JMSStatsImpl factoryStats;
    private final JMSConnectionStatsImpl stats;

    private final AtomicBoolean started = new AtomicBoolean(false);// 是否已开启标志
    private final AtomicBoolean closing = new AtomicBoolean(false);// 是否关闭中标志
    private final AtomicBoolean closed = new AtomicBoolean(false);// 是否已经关闭标志
    private final AtomicBoolean transportFailed = new AtomicBoolean(false);// 是否传送失败标志
    private final CopyOnWriteArrayList<ActiveMQSession> sessions = new CopyOnWriteArrayList<ActiveMQSession>();// session列表
    private final CopyOnWriteArrayList<ActiveMQConnectionConsumer> connectionConsumers = new CopyOnWriteArrayList<ActiveMQConnectionConsumer>();// 连接的consumer列表
    private final CopyOnWriteArrayList<TransportListener> transportListeners = new CopyOnWriteArrayList<TransportListener>();// transport listener列表

    // consumerId和ActiveMQDispatcher对象之间的关联Map属性
    private final ConcurrentMap<ConsumerId, ActiveMQDispatcher> dispatchers = new ConcurrentHashMap<ConsumerId, ActiveMQDispatcher>();
    // producerId和ActiveMQMessageProducer的对应关系Map
    private final ConcurrentMap<ProducerId, ActiveMQMessageProducer> producers = new ConcurrentHashMap<ProducerId, ActiveMQMessageProducer>();
    private final LongSequenceGenerator sessionIdGenerator = new LongSequenceGenerator();// sessionid 生成器
    private final SessionId connectionSessionId;// sessionId
    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();// consumerId生成器
    private final LongSequenceGenerator tempDestinationIdGenerator = new LongSequenceGenerator();// 临时consumerId生成器
    private final LongSequenceGenerator localTransactionIdGenerator = new LongSequenceGenerator();// 本地事物ID生成器

    private AdvisoryConsumer advisoryConsumer;
    private final CountDownLatch brokerInfoReceived = new CountDownLatch(1);
    private BrokerInfo brokerInfo;
    private IOException firstFailureError;
    private int producerWindowSize = ActiveMQConnectionFactory.DEFAULT_PRODUCER_WINDOW_SIZE;

    // Assume that protocol is the latest. Change to the actual protocol
    // version when a WireFormatInfo is received.
    private final AtomicInteger protocolVersion = new AtomicInteger(CommandTypes.PROTOCOL_VERSION);
    private final long timeCreated;
    private final ConnectionAudit connectionAudit = new ConnectionAudit();
    private DestinationSource destinationSource;
    private final Object ensureConnectionInfoSentMutex = new Object();
    private boolean useDedicatedTaskRunner;
    protected AtomicInteger transportInterruptionProcessingComplete = new AtomicInteger(0);
    private long consumerFailoverRedeliveryWaitPeriod;
    private Scheduler scheduler;
    private boolean messagePrioritySupported = false;
    private boolean transactedIndividualAck = false;
    private boolean nonBlockingRedelivery = false;// 是否非阻塞返还
    private boolean rmIdFromConnectionId = false;

    private int maxThreadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    private RejectedExecutionHandler rejectedTaskHandler = null;

    private List<String> trustedPackages = new ArrayList<String>();
    private boolean trustAllPackages = false;
	private int connectResponseTimeout;


	/**************************构造方法 start***************************/

    protected ActiveMQConnection(final Transport transport, IdGenerator clientIdGenerator, IdGenerator connectionIdGenerator, JMSStatsImpl factoryStats) throws Exception {

        this.transport = transport;
        this.clientIdGenerator = clientIdGenerator;
        this.factoryStats = factoryStats;

        /**
         * ThreadPoolExecutor的核心构造器的参数详解：
         * corePoolSize	核心线程池大小
         * maximumPoolSize	最大线程池大小
         * keepAliveTime	线程池中超过corePoolSize数目的空闲线程最大存活时间；可以allowCoreThreadTimeOut(true)使得核心线程有效时间
         * TimeUnit	keepAliveTime时间单位
         * workQueue	阻塞任务队列
         * threadFactory	新建线程工厂
         * RejectedExecutionHandler	当提交任务数超过maxmumPoolSize+workQueue之和时，任务会交给RejectedExecutionHandler来处理
         */
        executor = new ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "ActiveMQ Connection Executor: " + transport);
                //Don't make these daemon threads - see https://issues.apache.org/jira/browse/AMQ-796
                //thread.setDaemon(true);
                return thread;
            }
        });

        // ConnectionInfo对象属性初始化
        String uniqueId = connectionIdGenerator.generateId();
        this.info = new ConnectionInfo(new ConnectionId(uniqueId));
        this.info.setManageable(true);
        this.info.setFaultTolerant(transport.isFaultTolerant());

        // SessionId对象属性初始化
        this.connectionSessionId = new SessionId(info.getConnectionId(), -1);

        // ActiveMQConnection和Transport之间的组合模式，将ActiveMQConnection对象初始化成Transport对象属性
        this.transport.setTransportListener(this);

        this.stats = new JMSConnectionStatsImpl(sessions, this instanceof XAConnection);
        this.factoryStats.addConnection(this);
        this.timeCreated = System.currentTimeMillis();
        this.connectionAudit.setCheckForDuplicates(transport.isFaultTolerant());
    }

    /**************************构造方法 end***************************/



    /**
     * ActiveMQConnectionFactory创建ActiveMQConnection连接方法（static）
     */
    public static ActiveMQConnection makeConnection() throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        return (ActiveMQConnection)factory.createConnection();
    }

    /**
     * ActiveMQConnectionFactory创建ActiveMQConnection连接方法（static）
     */
    public static ActiveMQConnection makeConnection(String uri) throws JMSException, URISyntaxException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(uri);
        return (ActiveMQConnection)factory.createConnection();
    }

    /**
     * ActiveMQConnectionFactory创建ActiveMQConnection连接方法（static）
     */
    public static ActiveMQConnection makeConnection(String user, String password, String uri) throws JMSException, URISyntaxException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(user, password, new URI(uri));
        return (ActiveMQConnection)factory.createConnection();
    }


    /**
     * 创建session对象方法
     * @param transacted 是否在session中使用事物模式
     *
     * @param acknowledgeMode 消息签收机制
     * （activeMQ消息签收机制一共有四种，如下：
     *                   1：自动确认
     *                   2、客户端手动确认
     *                   3、自动批量确认，消息可能会重复发送。在第二次重新传送消息的时候，消息头的JmsDelivered会被置为true标示当前消息已经传送过一次，客户端需要进行消息的重复处理控制，
     *                   0、事务提交并确认 ）
     * @return
     * @throws JMSException
     */
    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {

        // 校验连接是否是关闭或者失败
        checkClosedOrFailed();

        // 发送数据包给broker，并设置对应的属性(确保可以和broker连接)
        ensureConnectionInfoSent();

        // 消息签收机制模式的合法性校验
        if (!transacted) {
            if (acknowledgeMode == Session.SESSION_TRANSACTED) {
                throw new JMSException("acknowledgeMode SESSION_TRANSACTED cannot be used for an non-transacted Session");
            } else if (acknowledgeMode < Session.SESSION_TRANSACTED || acknowledgeMode > ActiveMQSession.MAX_ACK_CONSTANT) {
                throw new JMSException("invalid acknowledgeMode: " + acknowledgeMode + ". Valid values are Session.AUTO_ACKNOWLEDGE (1), " +
                        "Session.CLIENT_ACKNOWLEDGE (2), Session.DUPS_OK_ACKNOWLEDGE (3), ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE (4) or for transacted sessions Session.SESSION_TRANSACTED (0)");
            }
        }

        // 创建ActiveMQSession对象
       return new ActiveMQSession(this, getNextSessionId(), transacted ? Session.SESSION_TRANSACTED : acknowledgeMode, isDispatchAsync(), isAlwaysSessionAsync());
    }



    /**
     * 获取clientId属性
     */
    @Override
    public String getClientID() throws JMSException {
        checkClosedOrFailed();
        return this.info.getClientId();
    }

    /**
     * 设置clientId，一般需要唯一
     */
    @Override
    public void setClientID(String newClientID) throws JMSException {
        checkClosedOrFailed();

        if (this.clientIDSet) {
            throw new IllegalStateException("The clientID has already been set");
        }

        if (this.isConnectionInfoSentToBroker) {
            throw new IllegalStateException("Setting clientID on a used Connection is not allowed");
        }

        this.info.setClientId(newClientID);
        this.userSpecifiedClientID = true;
        ensureConnectionInfoSent();
    }

    /**
     * 默认设置clientID
     */
    public void setDefaultClientID(String clientID) throws JMSException {
        this.info.setClientId(clientID);
        this.userSpecifiedClientID = true;
    }

    /**
     * Gets the metadata for this connection.
     */
    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        checkClosedOrFailed();
        return ActiveMQConnectionMetaData.INSTANCE;
    }

    /**
     * Gets the <CODE>ExceptionListener</CODE> object for this connection. Not
     */
    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        checkClosedOrFailed();
        return this.exceptionListener;
    }

    /**
     * Sets an exception listener for this connection.
     */
    @Override
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        checkClosedOrFailed();
        this.exceptionListener = listener;
    }

    /**
     * Gets the <code>ClientInternalExceptionListener</code> object for this connection.
     */
    public ClientInternalExceptionListener getClientInternalExceptionListener() {
        return clientInternalExceptionListener;
    }

    /**
     * Sets a client internal exception listener for this connection.
     */
    public void setClientInternalExceptionListener(ClientInternalExceptionListener listener) {
        this.clientInternalExceptionListener = listener;
    }

    /**
     * start方法
     */
    @Override
    public void start() throws JMSException {
        // 校验是否关闭或者失败
        checkClosedOrFailed();

        // 校验是否和broker端保持了连接
        ensureConnectionInfoSent();

        // connection对象下的所有session开启
        if (started.compareAndSet(false, true)) {
            for (Iterator<ActiveMQSession> i = sessions.iterator(); i.hasNext();) {
                ActiveMQSession session = i.next();
                session.start();
            }
        }
    }

    /**
     * stop方法
     */
    @Override
    public void stop() throws JMSException {
        doStop(true);
    }

    void doStop(boolean checkClosed) throws JMSException {
        if (checkClosed) {
            checkClosedOrFailed();
        }
        if (started.compareAndSet(true, false)) {
            synchronized(sessions) {
                for (Iterator<ActiveMQSession> i = sessions.iterator(); i.hasNext();) {
                    ActiveMQSession s = i.next();
                    s.stop();
                }
            }
        }
    }

    /**
     * close方法
     */
    @Override
    public void close() throws JMSException {
        try {
            // 若还在运行，则close的时候先进行stop操作
            if (!closed.get() && !transportFailed.get()) {
                doStop(false);
            }

            synchronized (this) {
                if (!closed.get()) {
                    closing.set(true);

                    if (destinationSource != null) {
                        destinationSource.stop();
                        destinationSource = null;
                    }
                    if (advisoryConsumer != null) {
                        advisoryConsumer.dispose();
                        advisoryConsumer = null;
                    }

                    Scheduler scheduler = this.scheduler;
                    if (scheduler != null) {
                        try {
                            scheduler.stop();
                        } catch (Exception e) {
                            JMSException ex =  JMSExceptionSupport.create(e);
                            throw ex;
                        }
                    }

                    long lastDeliveredSequenceId = -1;
                    for (Iterator<ActiveMQSession> i = this.sessions.iterator(); i.hasNext();) {
                        ActiveMQSession s = i.next();
                        s.dispose();
                        lastDeliveredSequenceId = Math.max(lastDeliveredSequenceId, s.getLastDeliveredSequenceId());
                    }
                    for (Iterator<ActiveMQConnectionConsumer> i = this.connectionConsumers.iterator(); i.hasNext();) {
                        ActiveMQConnectionConsumer c = i.next();
                        c.dispose();
                    }

                    this.activeTempDestinations.clear();

                    try {
                        if (isConnectionInfoSentToBroker) {
                            // 告知broker客户端连接已关闭信息
                            RemoveInfo removeCommand = info.createRemoveCommand();
                            removeCommand.setLastDeliveredSequenceId(lastDeliveredSequenceId);
                            try {
                                syncSendPacket(removeCommand, closeTimeout);
                            } catch (JMSException e) {
                                if (e.getCause() instanceof RequestTimedOutIOException) {
                                    // expected
                                } else {
                                    throw e;
                                }
                            }
                            doAsyncSendPacket(new ShutdownInfo());
                        }
                    } finally { // release anyway even if previous communication fails
                        started.set(false);

                        // TODO if we move the TaskRunnerFactory to the connection
                        // factory
                        // then we may need to call
                        // factory.onConnectionClose(this);
                        if (sessionTaskRunner != null) {
                            sessionTaskRunner.shutdown();
                        }
                        closed.set(true);
                        closing.set(false);
                    }
                }
            }
        } finally {
            try {
                if (executor != null) {
                    ThreadPoolUtils.shutdown(executor);
                }
            } catch (Throwable e) {
                LOG.warn("Error shutting down thread pool: " + executor + ". This exception will be ignored.", e);
            }

            ServiceSupport.dispose(this.transport);

            factoryStats.removeConnection(this);
        }
    }

    /**
     * createDurableConnectionConsumer方法具体实现
     */
    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages)
        throws JMSException {
        return this.createDurableConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages, false);
    }

    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages,
                                                              boolean noLocal) throws JMSException {
        checkClosedOrFailed();

        if (queueOnlyConnection) {
            throw new IllegalStateException("QueueConnection cannot be used to create Pub/Sub based resources.");
        }

        ensureConnectionInfoSent();
        SessionId sessionId = new SessionId(info.getConnectionId(), -1);
        ConsumerInfo info = new ConsumerInfo(new ConsumerId(sessionId, consumerIdGenerator.getNextSequenceId()));
        info.setDestination(ActiveMQMessageTransformation.transformDestination(topic));
        info.setSubscriptionName(subscriptionName);
        info.setSelector(messageSelector);
        info.setPrefetchSize(maxMessages);
        info.setDispatchAsync(isDispatchAsync());

        // Allows the options on the destination to configure the consumerInfo
        if (info.getDestination().getOptions() != null) {
            Map<String, String> options = new HashMap<String, String>(info.getDestination().getOptions());
            IntrospectionSupport.setProperties(this.info, options, "consumer.");
        }

        return new ActiveMQConnectionConsumer(this, sessionPool, info);
    }




    /**
     * Used internally for adding Sessions to the Connection
     *
     * @param session
     * @throws JMSException
     * @throws JMSException
     */
    protected void addSession(ActiveMQSession session) throws JMSException {
        this.sessions.add(session);
        if (sessions.size() > 1 || session.isTransacted()) {
            optimizedMessageDispatch = false;
        }
    }

    /**
     * Used interanlly for removing Sessions from a Connection
     *
     * @param session
     */
    protected void removeSession(ActiveMQSession session) {
        this.sessions.remove(session);
        this.removeDispatcher(session);
    }

    /**
     * Add a ConnectionConsumer
     *
     * @param connectionConsumer
     * @throws JMSException
     */
    protected void addConnectionConsumer(ActiveMQConnectionConsumer connectionConsumer) throws JMSException {
        this.connectionConsumers.add(connectionConsumer);
    }

    /**
     * Remove a ConnectionConsumer
     *
     * @param connectionConsumer
     */
    protected void removeConnectionConsumer(ActiveMQConnectionConsumer connectionConsumer) {
        this.connectionConsumers.remove(connectionConsumer);
        this.removeDispatcher(connectionConsumer);
    }

    /**
     * createTopicSession方法的具体实现
     */
    @Override
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return new ActiveMQTopicSession((ActiveMQSession)createSession(transacted, acknowledgeMode));
    }

    /**
     * createConnectionConsumer方法的具体实现
     */
    @Override
    public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return createConnectionConsumer(topic, messageSelector, sessionPool, maxMessages, false);
    }

    /**
     * createConnectionConsumer方法的具体实现
     */
    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return createConnectionConsumer(queue, messageSelector, sessionPool, maxMessages, false);
    }

    /**
     * createConnectionConsumer方法的具体实现
     */
    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return createConnectionConsumer(destination, messageSelector, sessionPool, maxMessages, false);
    }

    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages, boolean noLocal)
        throws JMSException {

        checkClosedOrFailed();
        ensureConnectionInfoSent();

        ConsumerId consumerId = createConsumerId();
        ConsumerInfo consumerInfo = new ConsumerInfo(consumerId);
        consumerInfo.setDestination(ActiveMQMessageTransformation.transformDestination(destination));
        consumerInfo.setSelector(messageSelector);
        consumerInfo.setPrefetchSize(maxMessages);
        consumerInfo.setNoLocal(noLocal);
        consumerInfo.setDispatchAsync(isDispatchAsync());

        // Allows the options on the destination to configure the consumerInfo
        if (consumerInfo.getDestination().getOptions() != null) {
            Map<String, String> options = new HashMap<String, String>(consumerInfo.getDestination().getOptions());
            IntrospectionSupport.setProperties(consumerInfo, options, "consumer.");
        }

        return new ActiveMQConnectionConsumer(this, sessionPool, consumerInfo);
    }

    /**
     * @return a newly created ConsumedId unique to this connection session instance.
     */
    private ConsumerId createConsumerId() {
        return new ConsumerId(connectionSessionId, consumerIdGenerator.getNextSequenceId());
    }

    /**
     * createQueueSession方法的实现
     */
    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return new ActiveMQQueueSession((ActiveMQSession)createSession(transacted, acknowledgeMode));
    }

    /**
     * 检查clientId是否是手动指定的
     */
    public void checkClientIDWasManuallySpecified() throws JMSException {
        if (!userSpecifiedClientID) {
            throw new JMSException("You cannot create a durable subscriber without specifying a unique clientID on a Connection");
        }
    }

    /**
     * 异步发送数据包给broker
     */
    public void asyncSendPacket(Command command) throws JMSException {
        if (isClosed()) {
            throw new ConnectionClosedException();
        } else {
            doAsyncSendPacket(command);
        }
    }

    private void doAsyncSendPacket(Command command) throws JMSException {
        try {
            this.transport.oneway(command);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    /**
     * 同步发送数据包给broker
     */
    public void syncSendPacket(final Command command, final AsyncCallback onComplete) throws JMSException {
        if(onComplete==null) {
            syncSendPacket(command);
        } else {
            if (isClosed()) {
                throw new ConnectionClosedException();
            }
            try {
                this.transport.asyncRequest(command, new ResponseCallback() {
                    @Override
                    public void onCompletion(FutureResponse resp) {
                        Response response;
                        Throwable exception = null;
                        try {
                            response = resp.getResult();
                            if (response.isException()) {
                                ExceptionResponse er = (ExceptionResponse)response;
                                exception = er.getException();
                            }
                        } catch (Exception e) {
                            exception = e;
                        }
                        if(exception!=null) {
                            if ( exception instanceof JMSException) {
                                onComplete.onException((JMSException) exception);
                            } else {
                                if (isClosed()||closing.get()) {
                                    LOG.debug("Received an exception but connection is closing");
                                }
                                JMSException jmsEx = null;
                                try {
                                    jmsEx = JMSExceptionSupport.create(exception);
                                } catch(Throwable e) {
                                    LOG.error("Caught an exception trying to create a JMSException for " +exception,e);
                                }
                                // dispose of transport for security exceptions on connection initiation
                                if (exception instanceof SecurityException && command instanceof ConnectionInfo){
                                    forceCloseOnSecurityException(exception);
                                }
                                if (jmsEx !=null) {
                                    onComplete.onException(jmsEx);
                                }
                            }
                        } else {
                            onComplete.onSuccess();
                        }
                    }
                });
            } catch (IOException e) {
                throw JMSExceptionSupport.create(e);
            }
        }
    }

    private void forceCloseOnSecurityException(Throwable exception) {
        LOG.trace("force close on security exception:" + this + ", transport=" + transport, exception);
        onException(new IOException("Force close due to SecurityException on connect", exception));
    }

    /**
     * 同步发送数据包到broker
     */
    public Response syncSendPacket(Command command, int timeout) throws JMSException {
        // close should always check
        if (isClosed()) {
            throw new ConnectionClosedException();
        } else {

            try {
                // send command to broker
                Response response = (Response)(timeout > 0
                        ? this.transport.request(command, timeout)
                        : this.transport.request(command));
                if (response.isException()) {
                    ExceptionResponse er = (ExceptionResponse)response;
                    if (er.getException() instanceof JMSException) {
                        throw (JMSException)er.getException();
                    } else {
                        if (isClosed()||closing.get()) {
                            LOG.debug("Received an exception but connection is closing");
                        }
                        JMSException jmsEx = null;
                        try {
                            jmsEx = JMSExceptionSupport.create(er.getException());
                        } catch(Throwable e) {
                            LOG.error("Caught an exception trying to create a JMSException for " +er.getException(),e);
                        }
                        if (er.getException() instanceof SecurityException && command instanceof ConnectionInfo){
                            forceCloseOnSecurityException(er.getException());
                        }
                        if (jmsEx !=null) {
                            throw jmsEx;
                        }
                    }
                }
                return response;
            } catch (IOException e) {
                throw JMSExceptionSupport.create(e);
            }
        }
    }

    /**
     * 同步发送数据包到broker
     */
    public Response syncSendPacket(Command command) throws JMSException {
        return syncSendPacket(command, 0);
    }

    /**
     * @return statistics for this Connection
     */
    @Override
    public StatsImpl getStats() {
        return stats;
    }

    /**
     * simply throws an exception if the Connection is already closed or the
     * Transport has failed
     *
     * @throws JMSException
     */
    protected synchronized void checkClosedOrFailed() throws JMSException {
        checkClosed();
        if (transportFailed.get()) {
            throw new ConnectionFailedException(firstFailureError);
        }
    }

    /**
     * simply throws an exception if the Connection is already closed
     *
     * @throws JMSException
     */
    protected synchronized void checkClosed() throws JMSException {
        if (closed.get()) {
            throw new ConnectionClosedException();
        }
    }

    /**
     * 同步发送数据包到broker，以确保可以和broker连接通
     */
    protected void ensureConnectionInfoSent() throws JMSException {
        synchronized(this.ensureConnectionInfoSentMutex) {

            // 若已经发送数据包给了broker或者连接已经关闭，则返回，此处若已经关闭为什么不抛异常？？
            if (isConnectionInfoSentToBroker || closed.get()) {
                return;
            }
            // clientId空校验
            if (info.getClientId() == null || info.getClientId().trim().length() == 0) {
                info.setClientId(clientIdGenerator.generateId());// clientId在不特殊设置的默认情况下，是一个自增的数值
            }

            // 同步发送数据包给broker
            syncSendPacket(info.copy(), getConnectResponseTimeout());

            // 设置是否发送数据包给broker的标识为true
            this.isConnectionInfoSentToBroker = true;

            // 生成consumerID
            ConsumerId consumerId = new ConsumerId(new SessionId(info.getConnectionId(), -1), consumerIdGenerator.getNextSequenceId());

            // 若是否监控topic动向的设置为true，则初始化advisoryConsumer对象属性
            if (watchTopicAdvisories) {
                advisoryConsumer = new AdvisoryConsumer(this, consumerId);
            }
        }
    }


    /**
     * 创建SessionId对象
     */
    protected SessionId getNextSessionId() {
        return new SessionId(info.getConnectionId(), sessionIdGenerator.getNextSequenceId());
    }


    public void cleanup() throws JMSException {
        doCleanup(false);
    }

    public void doCleanup(boolean removeConnection) throws JMSException {
        if (advisoryConsumer != null && !isTransportFailed()) {
            advisoryConsumer.dispose();
            advisoryConsumer = null;
        }

        for (Iterator<ActiveMQSession> i = this.sessions.iterator(); i.hasNext();) {
            ActiveMQSession s = i.next();
            s.dispose();
        }
        for (Iterator<ActiveMQConnectionConsumer> i = this.connectionConsumers.iterator(); i.hasNext();) {
            ActiveMQConnectionConsumer c = i.next();
            c.dispose();
        }

        if (removeConnection) {
            if (isConnectionInfoSentToBroker) {
                if (!transportFailed.get() && !closing.get()) {
                    syncSendPacket(info.createRemoveCommand());
                }
                isConnectionInfoSentToBroker = false;
            }
            if (userSpecifiedClientID) {
                info.setClientId(null);
                userSpecifiedClientID = false;
            }
            clientIDSet = false;
        }

        started.set(false);
    }


    public synchronized boolean isWatchTopicAdvisories() {
        return watchTopicAdvisories;
    }

    public synchronized void setWatchTopicAdvisories(boolean watchTopicAdvisories) {
        this.watchTopicAdvisories = watchTopicAdvisories;
    }


    public boolean isUseAsyncSend() {
        return useAsyncSend;
    }

    public void setUseAsyncSend(boolean useAsyncSend) {
        this.useAsyncSend = useAsyncSend;
    }

    public boolean isAlwaysSyncSend() {
        return this.alwaysSyncSend;
    }

    public void setAlwaysSyncSend(boolean alwaysSyncSend) {
        this.alwaysSyncSend = alwaysSyncSend;
    }

    public boolean isMessagePrioritySupported() {
        return this.messagePrioritySupported;
    }

    public void setMessagePrioritySupported(boolean messagePrioritySupported) {
        this.messagePrioritySupported = messagePrioritySupported;
    }

    public boolean isUserSpecifiedClientID() {
        return userSpecifiedClientID;
    }



    /**
     * Changes the associated username/password that is associated with this
     * connection. If the connection has been used, you must called cleanup()
     * before calling this method.
     *
     * @throws IllegalStateException if the connection is in used.
     */
    public void changeUserInfo(String userName, String password) throws JMSException {
        if (isConnectionInfoSentToBroker) {
            throw new IllegalStateException("changeUserInfo used Connection is not allowed");
        }
        this.info.setUserName(userName);
        this.info.setPassword(password);
    }

    /**
     * @return Returns the resourceManagerId.
     * @throws JMSException
     */
    public String getResourceManagerId() throws JMSException {
        if (isRmIdFromConnectionId()) {
            return info.getConnectionId().getValue();
        }
        waitForBrokerInfo();
        if (brokerInfo == null) {
            throw new JMSException("Connection failed before Broker info was received.");
        }
        return brokerInfo.getBrokerId().getValue();
    }

    /**
     * Returns the broker name if one is available or null if one is not
     * available yet.
     */
    public String getBrokerName() {
        try {
            brokerInfoReceived.await(5, TimeUnit.SECONDS);
            if (brokerInfo == null) {
                return null;
            }
            return brokerInfo.getBrokerName();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * Returns the broker information if it is available or null if it is not
     * available yet.
     */
    public BrokerInfo getBrokerInfo() {
        return brokerInfo;
    }

    /**
     * @return Returns the RedeliveryPolicy.
     * @throws JMSException
     */
    public RedeliveryPolicy getRedeliveryPolicy() throws JMSException {
        return redeliveryPolicyMap.getDefaultEntry();
    }

    /**
     * Sets the redelivery policy to be used when messages are rolled back
     */
    public void setRedeliveryPolicy(RedeliveryPolicy redeliveryPolicy) {
        this.redeliveryPolicyMap.setDefaultEntry(redeliveryPolicy);
    }

    public BlobTransferPolicy getBlobTransferPolicy() {
        if (blobTransferPolicy == null) {
            blobTransferPolicy = createBlobTransferPolicy();
        }
        return blobTransferPolicy;
    }

    /**
     * Sets the policy used to describe how out-of-band BLOBs (Binary Large
     * OBjects) are transferred from producers to brokers to consumers
     */
    public void setBlobTransferPolicy(BlobTransferPolicy blobTransferPolicy) {
        this.blobTransferPolicy = blobTransferPolicy;
    }

    /**
     * @return Returns the alwaysSessionAsync.
     */
    public boolean isAlwaysSessionAsync() {
        return alwaysSessionAsync;
    }

    /**
     * If this flag is not set then a separate thread is not used for dispatching messages for each Session in
     * the Connection. However, a separate thread is always used if there is more than one session, or the session
     * isn't in auto acknowledge or duplicates ok mode.  By default this value is set to true and session dispatch
     * happens asynchronously.
     */
    public void setAlwaysSessionAsync(boolean alwaysSessionAsync) {
        this.alwaysSessionAsync = alwaysSessionAsync;
    }

    /**
     * @return Returns the optimizeAcknowledge.
     */
    public boolean isOptimizeAcknowledge() {
        return optimizeAcknowledge;
    }

    /**
     * Enables an optimised acknowledgement mode where messages are acknowledged
     * in batches rather than individually
     *
     * @param optimizeAcknowledge The optimizeAcknowledge to set.
     */
    public void setOptimizeAcknowledge(boolean optimizeAcknowledge) {
        this.optimizeAcknowledge = optimizeAcknowledge;
    }

    /**
     * The max time in milliseconds between optimized ack batches
     * @param optimizeAcknowledgeTimeOut
     */
    public void setOptimizeAcknowledgeTimeOut(long optimizeAcknowledgeTimeOut) {
        this.optimizeAcknowledgeTimeOut =  optimizeAcknowledgeTimeOut;
    }

    public long getOptimizeAcknowledgeTimeOut() {
        return optimizeAcknowledgeTimeOut;
    }

    public long getWarnAboutUnstartedConnectionTimeout() {
        return warnAboutUnstartedConnectionTimeout;
    }

    /**
     * Enables the timeout from a connection creation to when a warning is
     * generated if the connection is not properly started via {@link #start()}
     * and a message is received by a consumer. It is a very common gotcha to
     * forget to <a
     * href="http://activemq.apache.org/i-am-not-receiving-any-messages-what-is-wrong.html">start
     * the connection</a> so this option makes the default case to create a
     * warning if the user forgets. To disable the warning just set the value to <
     * 0 (say -1).
     */
    public void setWarnAboutUnstartedConnectionTimeout(long warnAboutUnstartedConnectionTimeout) {
        this.warnAboutUnstartedConnectionTimeout = warnAboutUnstartedConnectionTimeout;
    }

    /**
     * @return the sendTimeout (in milliseconds)
     */
    public int getSendTimeout() {
        return sendTimeout;
    }

    /**
     * @param sendTimeout the sendTimeout to set (in milliseconds)
     */
    public void setSendTimeout(int sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    /**
     * @return the sendAcksAsync
     */
    public boolean isSendAcksAsync() {
        return sendAcksAsync;
    }

    /**
     * @param sendAcksAsync the sendAcksAsync to set
     */
    public void setSendAcksAsync(boolean sendAcksAsync) {
        this.sendAcksAsync = sendAcksAsync;
    }

    /**
     * Returns the time this connection was created
     */
    public long getTimeCreated() {
        return timeCreated;
    }

    private void waitForBrokerInfo() throws JMSException {
        try {
            brokerInfoReceived.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw JMSExceptionSupport.create(e);
        }
    }

    // Package protected so that it can be used in unit tests
    public Transport getTransport() {
        return transport;
    }

    public void addProducer(ProducerId producerId, ActiveMQMessageProducer producer) {
        producers.put(producerId, producer);
    }

    public void removeProducer(ProducerId producerId) {
        producers.remove(producerId);
    }

    public void addDispatcher(ConsumerId consumerId, ActiveMQDispatcher dispatcher) {
        dispatchers.put(consumerId, dispatcher);
    }

    public void removeDispatcher(ConsumerId consumerId) {
        dispatchers.remove(consumerId);
    }

    public boolean hasDispatcher(ConsumerId consumerId) {
        return dispatchers.containsKey(consumerId);
    }

    /**
     * @param o - the command to consume
     */
    @Override
    public void onCommand(final Object o) {
        final Command command = (Command)o;
        if (!closed.get() && command != null) {
            try {
                command.visit(new CommandVisitorAdapter() {
                    @Override
                    public Response processProducerAck(ProducerAck pa) throws Exception {
                        if (pa != null && pa.getProducerId() != null) {
                            ActiveMQMessageProducer producer = producers.get(pa.getProducerId());
                            if (producer != null) {
                                producer.onProducerAck(pa);
                            }
                        }
                        return null;
                    }

                    @Override
                    public Response processMessageDispatch(MessageDispatch md) throws Exception {
                        waitForTransportInterruptionProcessingToComplete();
                        ActiveMQDispatcher dispatcher = dispatchers.get(md.getConsumerId());
                        if (dispatcher != null) {
                            // Copy in case a embedded broker is dispatching via
                            // vm://
                            // md.getMessage() == null to signal end of queue
                            // browse.
                            Message msg = md.getMessage();
                            if (msg != null) {
                                msg = msg.copy();
                                msg.setReadOnlyBody(true);
                                msg.setReadOnlyProperties(true);
                                msg.setRedeliveryCounter(md.getRedeliveryCounter());
                                msg.setConnection(ActiveMQConnection.this);
                                msg.setMemoryUsage(null);
                                md.setMessage(msg);
                            }
                            dispatcher.dispatch(md);
                        } else {
                            LOG.debug("{} no dispatcher for {} in {}", this, md, dispatchers);
                        }
                        return null;
                    }

                    @Override
                    public Response processBrokerInfo(BrokerInfo info) throws Exception {
                        brokerInfo = info;
                        brokerInfoReceived.countDown();
                        optimizeAcknowledge &= !brokerInfo.isFaultTolerantConfiguration();
                        getBlobTransferPolicy().setBrokerUploadUrl(info.getBrokerUploadUrl());
                        return null;
                    }

                    @Override
                    public Response processConnectionError(final ConnectionError error) throws Exception {
                        executor.execute(new Runnable() {
                            @Override
                            public void run() {
                                onAsyncException(error.getException());
                            }
                        });
                        return null;
                    }

                    @Override
                    public Response processControlCommand(ControlCommand command) throws Exception {
                        return null;
                    }

                    @Override
                    public Response processConnectionControl(ConnectionControl control) throws Exception {
                        onConnectionControl((ConnectionControl)command);
                        return null;
                    }

                    @Override
                    public Response processConsumerControl(ConsumerControl control) throws Exception {
                        onConsumerControl((ConsumerControl)command);
                        return null;
                    }

                    @Override
                    public Response processWireFormat(WireFormatInfo info) throws Exception {
                        onWireFormatInfo((WireFormatInfo)command);
                        return null;
                    }
                });
            } catch (Exception e) {
                onClientInternalException(e);
            }
        }

        for (Iterator<TransportListener> iter = transportListeners.iterator(); iter.hasNext();) {
            TransportListener listener = iter.next();
            listener.onCommand(command);
        }
    }

    protected void onWireFormatInfo(WireFormatInfo info) {
        protocolVersion.set(info.getVersion());
    }

    /**
     * Handles async client internal exceptions.
     * A client internal exception is usually one that has been thrown
     * by a container runtime component during asynchronous processing of a
     * message that does not affect the connection itself.
     * This method notifies the <code>ClientInternalExceptionListener</code> by invoking
     * its <code>onException</code> method, if one has been registered with this connection.
     *
     * @param error the exception that the problem
     */
    public void onClientInternalException(final Throwable error) {
        if ( !closed.get() && !closing.get() ) {
            if ( this.clientInternalExceptionListener != null ) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        ActiveMQConnection.this.clientInternalExceptionListener.onException(error);
                    }
                });
            } else {
                LOG.debug("Async client internal exception occurred with no exception listener registered: "
                        + error, error);
            }
        }
    }

    /**
     * Used for handling async exceptions
     *
     * @param error
     */
    public void onAsyncException(Throwable error) {
        if (!closed.get() && !closing.get()) {
            if (this.exceptionListener != null) {

                if (!(error instanceof JMSException)) {
                    error = JMSExceptionSupport.create(error);
                }
                final JMSException e = (JMSException)error;

                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        ActiveMQConnection.this.exceptionListener.onException(e);
                    }
                });

            } else {
                LOG.debug("Async exception with no exception listener: " + error, error);
            }
        }
    }

    @Override
    public void onException(final IOException error) {
        onAsyncException(error);
        if (!closed.get() && !closing.get()) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    transportFailed(error);
                    ServiceSupport.dispose(ActiveMQConnection.this.transport);
                    brokerInfoReceived.countDown();
                    try {
                        doCleanup(true);
                    } catch (JMSException e) {
                        LOG.warn("Exception during connection cleanup, " + e, e);
                    }
                    for (Iterator<TransportListener> iter = transportListeners.iterator(); iter.hasNext();) {
                        TransportListener listener = iter.next();
                        listener.onException(error);
                    }
                }
            });
        }
    }

    @Override
    public void transportInterupted() {
        transportInterruptionProcessingComplete.set(1);
        for (Iterator<ActiveMQSession> i = this.sessions.iterator(); i.hasNext();) {
            ActiveMQSession s = i.next();
            s.clearMessagesInProgress(transportInterruptionProcessingComplete);
        }

        for (ActiveMQConnectionConsumer connectionConsumer : this.connectionConsumers) {
            connectionConsumer.clearMessagesInProgress(transportInterruptionProcessingComplete);
        }

        if (transportInterruptionProcessingComplete.decrementAndGet() > 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transport interrupted - processing required, dispatchers: " + transportInterruptionProcessingComplete.get());
            }
            signalInterruptionProcessingNeeded();
        }

        for (Iterator<TransportListener> iter = transportListeners.iterator(); iter.hasNext();) {
            TransportListener listener = iter.next();
            listener.transportInterupted();
        }
    }

    @Override
    public void transportResumed() {
        for (Iterator<TransportListener> iter = transportListeners.iterator(); iter.hasNext();) {
            TransportListener listener = iter.next();
            listener.transportResumed();
        }
    }

    /**
     * Create the DestinationInfo object for the temporary destination.
     *
     * @param topic - if its true topic, else queue.
     * @return DestinationInfo
     * @throws JMSException
     */
    protected ActiveMQTempDestination createTempDestination(boolean topic) throws JMSException {

        // Check if Destination info is of temporary type.
        ActiveMQTempDestination dest;
        if (topic) {
            dest = new ActiveMQTempTopic(info.getConnectionId(), tempDestinationIdGenerator.getNextSequenceId());
        } else {
            dest = new ActiveMQTempQueue(info.getConnectionId(), tempDestinationIdGenerator.getNextSequenceId());
        }

        DestinationInfo info = new DestinationInfo();
        info.setConnectionId(this.info.getConnectionId());
        info.setOperationType(DestinationInfo.ADD_OPERATION_TYPE);
        info.setDestination(dest);
        syncSendPacket(info);

        dest.setConnection(this);
        activeTempDestinations.put(dest, dest);
        return dest;
    }

    /**
     * @param destination
     * @throws JMSException
     */
    public void deleteTempDestination(ActiveMQTempDestination destination) throws JMSException {

        checkClosedOrFailed();

        for (ActiveMQSession session : this.sessions) {
            if (session.isInUse(destination)) {
                throw new JMSException("A consumer is consuming from the temporary destination");
            }
        }

        activeTempDestinations.remove(destination);

        DestinationInfo destInfo = new DestinationInfo();
        destInfo.setConnectionId(this.info.getConnectionId());
        destInfo.setOperationType(DestinationInfo.REMOVE_OPERATION_TYPE);
        destInfo.setDestination(destination);
        destInfo.setTimeout(0);
        syncSendPacket(destInfo);
    }

    public boolean isDeleted(ActiveMQDestination dest) {

        // If we are not watching the advisories.. then
        // we will assume that the temp destination does exist.
        if (advisoryConsumer == null) {
            return false;
        }

        return !activeTempDestinations.containsValue(dest);
    }

    public boolean isCopyMessageOnSend() {
        return copyMessageOnSend;
    }

    public LongSequenceGenerator getLocalTransactionIdGenerator() {
        return localTransactionIdGenerator;
    }

    public boolean isUseCompression() {
        return useCompression;
    }

    /**
     * Enables the use of compression of the message bodies
     */
    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    public void destroyDestination(ActiveMQDestination destination) throws JMSException {

        checkClosedOrFailed();
        ensureConnectionInfoSent();

        DestinationInfo info = new DestinationInfo();
        info.setConnectionId(this.info.getConnectionId());
        info.setOperationType(DestinationInfo.REMOVE_OPERATION_TYPE);
        info.setDestination(destination);
        info.setTimeout(0);
        syncSendPacket(info);
    }

    public boolean isDispatchAsync() {
        return dispatchAsync;
    }

    /**
     * Enables or disables the default setting of whether or not consumers have
     * their messages <a
     * href="http://activemq.apache.org/consumer-dispatch-async.html">dispatched
     * synchronously or asynchronously by the broker</a>. For non-durable
     * topics for example we typically dispatch synchronously by default to
     * minimize context switches which boost performance. However sometimes its
     * better to go slower to ensure that a single blocked consumer socket does
     * not block delivery to other consumers.
     *
     * @param asyncDispatch If true then consumers created on this connection
     *                will default to having their messages dispatched
     *                asynchronously. The default value is true.
     */
    public void setDispatchAsync(boolean asyncDispatch) {
        this.dispatchAsync = asyncDispatch;
    }

    public boolean isObjectMessageSerializationDefered() {
        return objectMessageSerializationDefered;
    }

    /**
     * When an object is set on an ObjectMessage, the JMS spec requires the
     * object to be serialized by that set method. Enabling this flag causes the
     * object to not get serialized. The object may subsequently get serialized
     * if the message needs to be sent over a socket or stored to disk.
     */
    public void setObjectMessageSerializationDefered(boolean objectMessageSerializationDefered) {
        this.objectMessageSerializationDefered = objectMessageSerializationDefered;
    }

    /**
     * Unsubscribes a durable subscription that has been created by a client.
     * <P>
     * This method deletes the state being maintained on behalf of the
     * subscriber by its provider.
     * <P>
     * It is erroneous for a client to delete a durable subscription while there
     * is an active <CODE>MessageConsumer </CODE> or
     * <CODE>TopicSubscriber</CODE> for the subscription, or while a consumed
     * message is part of a pending transaction or has not been acknowledged in
     * the session.
     *
     * @param name the name used to identify this subscription
     * @throws JMSException if the session fails to unsubscribe to the durable
     *                 subscription due to some internal error.
     * @throws InvalidDestinationException if an invalid subscription name is
     *                 specified.
     * @since 1.1
     */
    public void unsubscribe(String name) throws InvalidDestinationException, JMSException {
        checkClosedOrFailed();
        RemoveSubscriptionInfo rsi = new RemoveSubscriptionInfo();
        rsi.setConnectionId(getConnectionInfo().getConnectionId());
        rsi.setSubscriptionName(name);
        rsi.setClientId(getConnectionInfo().getClientId());
        syncSendPacket(rsi);
    }

    /**
     * Internal send method optimized: - It does not copy the message - It can
     * only handle ActiveMQ messages. - You can specify if the send is async or
     * sync - Does not allow you to send /w a transaction.
     */
    void send(ActiveMQDestination destination, ActiveMQMessage msg, MessageId messageId, int deliveryMode, int priority, long timeToLive, boolean async) throws JMSException {
        checkClosedOrFailed();

        if (destination.isTemporary() && isDeleted(destination)) {
            throw new JMSException("Cannot publish to a deleted Destination: " + destination);
        }

        msg.setJMSDestination(destination);
        msg.setJMSDeliveryMode(deliveryMode);
        long expiration = 0L;

        if (!isDisableTimeStampsByDefault()) {
            long timeStamp = System.currentTimeMillis();
            msg.setJMSTimestamp(timeStamp);
            if (timeToLive > 0) {
                expiration = timeToLive + timeStamp;
            }
        }

        msg.setJMSExpiration(expiration);
        msg.setJMSPriority(priority);
        msg.setJMSRedelivered(false);
        msg.setMessageId(messageId);
        msg.onSend();
        msg.setProducerId(msg.getMessageId().getProducerId());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending message: " + msg);
        }

        if (async) {
            asyncSendPacket(msg);
        } else {
            syncSendPacket(msg);
        }
    }

    protected void onConnectionControl(ConnectionControl command) {
        if (command.isFaultTolerant()) {
            this.optimizeAcknowledge = false;
            for (Iterator<ActiveMQSession> i = this.sessions.iterator(); i.hasNext();) {
                ActiveMQSession s = i.next();
                s.setOptimizeAcknowledge(false);
            }
        }
    }

    protected void onConsumerControl(ConsumerControl command) {
        if (command.isClose()) {
            for (ActiveMQSession session : this.sessions) {
                session.close(command.getConsumerId());
            }
        } else {
            for (ActiveMQSession session : this.sessions) {
                session.setPrefetchSize(command.getConsumerId(), command.getPrefetch());
            }
            for (ActiveMQConnectionConsumer connectionConsumer: connectionConsumers) {
                ConsumerInfo consumerInfo = connectionConsumer.getConsumerInfo();
                if (consumerInfo.getConsumerId().equals(command.getConsumerId())) {
                    consumerInfo.setPrefetchSize(command.getPrefetch());
                }
            }
        }
    }

    protected void transportFailed(IOException error) {
        transportFailed.set(true);
        if (firstFailureError == null) {
            firstFailureError = error;
        }
    }

    /**
     * Should a JMS message be copied to a new JMS Message object as part of the
     * send() method in JMS. This is enabled by default to be compliant with the
     * JMS specification. You can disable it if you do not mutate JMS messages
     * after they are sent for a performance boost
     */
    public void setCopyMessageOnSend(boolean copyMessageOnSend) {
        this.copyMessageOnSend = copyMessageOnSend;
    }

    @Override
    public String toString() {
        return "ActiveMQConnection {id=" + info.getConnectionId() + ",clientId=" + info.getClientId() + ",started=" + started.get() + "}";
    }

    protected BlobTransferPolicy createBlobTransferPolicy() {
        return new BlobTransferPolicy();
    }

    public int getProtocolVersion() {
        return protocolVersion.get();
    }

    public int getProducerWindowSize() {
        return producerWindowSize;
    }

    public void setProducerWindowSize(int producerWindowSize) {
        this.producerWindowSize = producerWindowSize;
    }

    public void setAuditDepth(int auditDepth) {
        connectionAudit.setAuditDepth(auditDepth);
    }

    public void setAuditMaximumProducerNumber(int auditMaximumProducerNumber) {
        connectionAudit.setAuditMaximumProducerNumber(auditMaximumProducerNumber);
    }

    protected void removeDispatcher(ActiveMQDispatcher dispatcher) {
        connectionAudit.removeDispatcher(dispatcher);
    }

    protected boolean isDuplicate(ActiveMQDispatcher dispatcher, Message message) {
        return checkForDuplicates && connectionAudit.isDuplicate(dispatcher, message);
    }

    protected void rollbackDuplicate(ActiveMQDispatcher dispatcher, Message message) {
        connectionAudit.rollbackDuplicate(dispatcher, message);
    }

    public IOException getFirstFailureError() {
        return firstFailureError;
    }

    protected void waitForTransportInterruptionProcessingToComplete() throws InterruptedException {
        if (!closed.get() && !transportFailed.get() && transportInterruptionProcessingComplete.get()>0) {
            LOG.warn("dispatch with outstanding dispatch interruption processing count " + transportInterruptionProcessingComplete.get());
            signalInterruptionProcessingComplete();
        }
    }

    protected void transportInterruptionProcessingComplete() {
        if (transportInterruptionProcessingComplete.decrementAndGet() == 0) {
            signalInterruptionProcessingComplete();
        }
    }

    private void signalInterruptionProcessingComplete() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transportInterruptionProcessingComplete: " + transportInterruptionProcessingComplete.get()
                        + " for:" + this.getConnectionInfo().getConnectionId());
            }

            FailoverTransport failoverTransport = transport.narrow(FailoverTransport.class);
            if (failoverTransport != null) {
                failoverTransport.connectionInterruptProcessingComplete(this.getConnectionInfo().getConnectionId());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("notified failover transport (" + failoverTransport
                            + ") of interruption completion for: " + this.getConnectionInfo().getConnectionId());
                }
            }
            transportInterruptionProcessingComplete.set(0);
    }

    private void signalInterruptionProcessingNeeded() {
        FailoverTransport failoverTransport = transport.narrow(FailoverTransport.class);
        if (failoverTransport != null) {
            failoverTransport.getStateTracker().transportInterrupted(this.getConnectionInfo().getConnectionId());
            if (LOG.isDebugEnabled()) {
                LOG.debug("notified failover transport (" + failoverTransport
                        + ") of pending interruption processing for: " + this.getConnectionInfo().getConnectionId());
            }
        }
    }

    /*
     * specify the amount of time in milliseconds that a consumer with a transaction pending recovery
     * will wait to receive re dispatched messages.
     * default value is 0 so there is no wait by default.
     */
    public void setConsumerFailoverRedeliveryWaitPeriod(long consumerFailoverRedeliveryWaitPeriod) {
        this.consumerFailoverRedeliveryWaitPeriod = consumerFailoverRedeliveryWaitPeriod;
    }

    public long getConsumerFailoverRedeliveryWaitPeriod() {
        return consumerFailoverRedeliveryWaitPeriod;
    }

    protected Scheduler getScheduler() throws JMSException {
        Scheduler result = scheduler;
        if (result == null) {
            if (isClosing() || isClosed()) {
                // without lock contention report the closing state
                throw new ConnectionClosedException();
            }
            synchronized (this) {
                result = scheduler;
                if (result == null) {
                    checkClosed();
                    try {
                        result = new Scheduler("ActiveMQConnection["+info.getConnectionId().getValue()+"] Scheduler");
                        result.start();
                        scheduler = result;
                    } catch(Exception e) {
                        throw JMSExceptionSupport.create(e);
                    }
                }
            }
        }
        return result;
    }

    protected ThreadPoolExecutor getExecutor() {
        return this.executor;
    }

    protected CopyOnWriteArrayList<ActiveMQSession> getSessions() {
        return sessions;
    }

    /**
     * @return the checkForDuplicates
     */
    public boolean isCheckForDuplicates() {
        return this.checkForDuplicates;
    }

    /**
     * @param checkForDuplicates the checkForDuplicates to set
     */
    public void setCheckForDuplicates(boolean checkForDuplicates) {
        this.checkForDuplicates = checkForDuplicates;
    }

    public boolean isTransactedIndividualAck() {
        return transactedIndividualAck;
    }

    public void setTransactedIndividualAck(boolean transactedIndividualAck) {
        this.transactedIndividualAck = transactedIndividualAck;
    }

    public boolean isNonBlockingRedelivery() {
        return nonBlockingRedelivery;
    }

    public void setNonBlockingRedelivery(boolean nonBlockingRedelivery) {
        this.nonBlockingRedelivery = nonBlockingRedelivery;
    }

    public boolean isRmIdFromConnectionId() {
        return rmIdFromConnectionId;
    }

    public void setRmIdFromConnectionId(boolean rmIdFromConnectionId) {
        this.rmIdFromConnectionId = rmIdFromConnectionId;
    }

    /**
     * Removes any TempDestinations that this connection has cached, ignoring
     * any exceptions generated because the destination is in use as they should
     * not be removed.
     * Used from a pooled connection, b/c it will not be explicitly closed.
     */
    public void cleanUpTempDestinations() {

        if (this.activeTempDestinations == null || this.activeTempDestinations.isEmpty()) {
            return;
        }

        Iterator<ConcurrentMap.Entry<ActiveMQTempDestination, ActiveMQTempDestination>> entries
            = this.activeTempDestinations.entrySet().iterator();
        while(entries.hasNext()) {
            ConcurrentMap.Entry<ActiveMQTempDestination, ActiveMQTempDestination> entry = entries.next();
            try {
                // Only delete this temp destination if it was created from this connection. The connection used
                // for the advisory consumer may also have a reference to this temp destination.
                ActiveMQTempDestination dest = entry.getValue();
                String thisConnectionId = (info.getConnectionId() == null) ? "" : info.getConnectionId().toString();
                if (dest.getConnectionId() != null && dest.getConnectionId().equals(thisConnectionId)) {
                    this.deleteTempDestination(entry.getValue());
                }
            } catch (Exception ex) {
                // the temp dest is in use so it can not be deleted.
                // it is ok to leave it to connection tear down phase
            }
        }
    }

    /**
     * Sets the Connection wide RedeliveryPolicyMap for handling messages that are being rolled back.
     * @param redeliveryPolicyMap the redeliveryPolicyMap to set
     */
    public void setRedeliveryPolicyMap(RedeliveryPolicyMap redeliveryPolicyMap) {
        this.redeliveryPolicyMap = redeliveryPolicyMap;
    }

    /**
     * Gets the Connection's configured RedeliveryPolicyMap which will be used by all the
     * Consumers when dealing with transaction messages that have been rolled back.
     *
     * @return the redeliveryPolicyMap
     */
    public RedeliveryPolicyMap getRedeliveryPolicyMap() {
        return redeliveryPolicyMap;
    }

    public int getMaxThreadPoolSize() {
        return maxThreadPoolSize;
    }

    public void setMaxThreadPoolSize(int maxThreadPoolSize) {
        this.maxThreadPoolSize = maxThreadPoolSize;
    }

    /**
     * Enable enforcement of QueueConnection semantics.
     *
     * @return this object, useful for chaining
     */
    ActiveMQConnection enforceQueueOnlyConnection() {
        this.queueOnlyConnection = true;
        return this;
    }

    public RejectedExecutionHandler getRejectedTaskHandler() {
        return rejectedTaskHandler;
    }

    public void setRejectedTaskHandler(RejectedExecutionHandler rejectedTaskHandler) {
        this.rejectedTaskHandler = rejectedTaskHandler;
    }

    /**
     * Gets the configured time interval that is used to force all MessageConsumers that have optimizedAcknowledge enabled
     * to send an ack for any outstanding Message Acks.  By default this value is set to zero meaning that the consumers
     * will not do any background Message acknowledgment.
     *
     * @return the scheduledOptimizedAckInterval
     */
    public long getOptimizedAckScheduledAckInterval() {
        return optimizedAckScheduledAckInterval;
    }

    /**
     * Sets the amount of time between scheduled sends of any outstanding Message Acks for consumers that
     * have been configured with optimizeAcknowledge enabled.
     *
     * @param optimizedAckScheduledAckInterval the scheduledOptimizedAckInterval to set
     */
    public void setOptimizedAckScheduledAckInterval(long optimizedAckScheduledAckInterval) {
        this.optimizedAckScheduledAckInterval = optimizedAckScheduledAckInterval;
    }

    /**
     * @return true if MessageConsumer instance will check for expired messages before dispatch.
     */
    public boolean isConsumerExpiryCheckEnabled() {
        return consumerExpiryCheckEnabled;
    }

    /**
     * Controls whether message expiration checking is done in each MessageConsumer
     * prior to dispatching a message.  Disabling this check can lead to consumption
     * of expired messages.
     *
     * @param consumerExpiryCheckEnabled
     *        controls whether expiration checking is done prior to dispatch.
     */
    public void setConsumerExpiryCheckEnabled(boolean consumerExpiryCheckEnabled) {
        this.consumerExpiryCheckEnabled = consumerExpiryCheckEnabled;
    }

    public List<String> getTrustedPackages() {
        return trustedPackages;
    }

    public void setTrustedPackages(List<String> trustedPackages) {
        this.trustedPackages = trustedPackages;
    }

    public boolean isTrustAllPackages() {
        return trustAllPackages;
    }

    public void setTrustAllPackages(boolean trustAllPackages) {
        this.trustAllPackages = trustAllPackages;
    }

    public int getConnectResponseTimeout() {
    	return connectResponseTimeout;
    }

	public void setConnectResponseTimeout(int connectResponseTimeout) {
		this.connectResponseTimeout = connectResponseTimeout;
	}

    protected void setUserName(String userName) {
        this.info.setUserName(userName);
    }

    protected void setPassword(String password) {
        this.info.setPassword(password);
    }

    public JMSConnectionStatsImpl getConnectionStats() {
        return stats;
    }


    public boolean isStarted() {
        return started.get();
    }

    public boolean isClosed() {
        return closed.get();
    }

    public boolean isClosing() {
        return closing.get();
    }

    public boolean isTransportFailed() {
        return transportFailed.get();
    }

    public ActiveMQPrefetchPolicy getPrefetchPolicy() {
        return prefetchPolicy;
    }

    public void setPrefetchPolicy(ActiveMQPrefetchPolicy prefetchPolicy) {
        this.prefetchPolicy = prefetchPolicy;
    }

    public Transport getTransportChannel() {
        return transport;
    }

    public String getInitializedClientID() throws JMSException {
        ensureConnectionInfoSent();
        return info.getClientId();
    }

    public boolean isDisableTimeStampsByDefault() {
        return disableTimeStampsByDefault;
    }

    public void setDisableTimeStampsByDefault(boolean timeStampsDisableByDefault) {
        this.disableTimeStampsByDefault = timeStampsDisableByDefault;
    }

    public boolean isOptimizedMessageDispatch() {
        return optimizedMessageDispatch;
    }

    public void setOptimizedMessageDispatch(boolean dispatchOptimizedMessage) {
        this.optimizedMessageDispatch = dispatchOptimizedMessage;
    }
    public int getCloseTimeout() {
        return closeTimeout;
    }

    public void setCloseTimeout(int closeTimeout) {
        this.closeTimeout = closeTimeout;
    }

    public ConnectionInfo getConnectionInfo() {
        return this.info;
    }

    public boolean isUseRetroactiveConsumer() {
        return useRetroactiveConsumer;
    }

    public void setUseRetroactiveConsumer(boolean useRetroactiveConsumer) {
        this.useRetroactiveConsumer = useRetroactiveConsumer;
    }

    public boolean isNestedMapAndListEnabled() {
        return nestedMapAndListEnabled;
    }
    public void setNestedMapAndListEnabled(boolean structuredMapsEnabled) {
        this.nestedMapAndListEnabled = structuredMapsEnabled;
    }
    public boolean isExclusiveConsumer() {
        return exclusiveConsumer;
    }
    public void setExclusiveConsumer(boolean exclusiveConsumer) {
        this.exclusiveConsumer = exclusiveConsumer;
    }

    public void addTransportListener(TransportListener transportListener) {
        transportListeners.add(transportListener);
    }

    public void removeTransportListener(TransportListener transportListener) {
        transportListeners.remove(transportListener);
    }

    public boolean isUseDedicatedTaskRunner() {
        return useDedicatedTaskRunner;
    }

    public void setUseDedicatedTaskRunner(boolean useDedicatedTaskRunner) {
        this.useDedicatedTaskRunner = useDedicatedTaskRunner;
    }

    public TaskRunnerFactory getSessionTaskRunner() {
        synchronized (this) {
            if (sessionTaskRunner == null) {
                sessionTaskRunner = new TaskRunnerFactory("ActiveMQ Session Task", ThreadPriorities.INBOUND_CLIENT_SESSION, false, 1000, isUseDedicatedTaskRunner(), maxThreadPoolSize);
                sessionTaskRunner.setRejectedTaskHandler(rejectedTaskHandler);
            }
        }
        return sessionTaskRunner;
    }

    public void setSessionTaskRunner(TaskRunnerFactory sessionTaskRunner) {
        this.sessionTaskRunner = sessionTaskRunner;
    }

    public MessageTransformer getTransformer() {
        return transformer;
    }

    public void setTransformer(MessageTransformer transformer) {
        this.transformer = transformer;
    }

    public boolean isStatsEnabled() {
        return this.stats.isEnabled();
    }

    public void setStatsEnabled(boolean statsEnabled) {
        this.stats.setEnabled(statsEnabled);
    }

    /**
     * Returns the {@link DestinationSource} object which can be used to listen to destinations
     * being created or destroyed or to enquire about the current destinations available on the broker
     *
     * @return a lazily created destination source
     * @throws JMSException
     */
    @Override
    public DestinationSource getDestinationSource() throws JMSException {
        if (destinationSource == null) {
            destinationSource = new DestinationSource(this);
            destinationSource.start();
        }
        return destinationSource;
    }
}

