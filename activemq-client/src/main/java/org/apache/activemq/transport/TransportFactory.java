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
package org.apache.activemq.transport;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public abstract class TransportFactory {

    private static final FactoryFinder TRANSPORT_FACTORY_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/transport/");
    private static final FactoryFinder WIREFORMAT_FACTORY_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/wireformat/");
    private static final ConcurrentMap<String, TransportFactory> TRANSPORT_FACTORYS = new ConcurrentHashMap<String, TransportFactory>();

    private static final String WRITE_TIMEOUT_FILTER = "soWriteTimeout";
    private static final String THREAD_NAME_FILTER = "threadName";


    public abstract TransportServer doBind(URI location) throws IOException;

    /**
     * 创建normal transport对象
     */
    public Transport doConnect(URI location, Executor ex) throws Exception {
        return doConnect(location);
    }

    /**
     * 创建简化版高传输效率的transport对象
     */
    public Transport doCompositeConnect(URI location, Executor ex) throws Exception {
        return doCompositeConnect(location);
    }

    /**
     * 创建normal Transport对象
     */
    public static Transport connect(URI location) throws Exception {
        TransportFactory tf = findTransportFactory(location);
        return tf.doConnect(location);
    }

    /**
     * 创建normal Transport对象
     */
    public static Transport connect(URI location, Executor ex) throws Exception {
        TransportFactory tf = findTransportFactory(location);
        return tf.doConnect(location, ex);
    }

    /**
     * Creates a slimmed down transport that is more efficient so that it can be
     * used by composite transports like reliable and HA.
     * 创建一个精简版的传输效率更高的Transport对象，可以被复合的transport如HA等使用
     */
    public static Transport compositeConnect(URI location) throws Exception {
        TransportFactory tf = findTransportFactory(location);
        return tf.doCompositeConnect(location);
    }

    /**
     * 创建精简版transport对象
     */
    public static Transport compositeConnect(URI location, Executor ex) throws Exception {
        TransportFactory tf = findTransportFactory(location);
        return tf.doCompositeConnect(location, ex);
    }

    /**
     * 创建TransportServer对象
     */
    public static TransportServer bind(URI location) throws IOException {
        TransportFactory tf = findTransportFactory(location);
        return tf.doBind(location);
    }

    /**
     * 创建normal transport对象
     */
    public Transport doConnect(URI location) throws Exception {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));

            if( !options.containsKey("wireFormat.host") ) {
                options.put("wireFormat.host", location.getHost());
            }
            // 协议信息解析类
            WireFormat wf = createWireFormat(options);

            // 用WireFormat实例装饰Transport实例（装饰者设计模式）
            Transport transport = createTransport(location, wf);

            // 对transport实例进行过滤链封装
            Transport rc = configure(transport, wf, options);

            //去除选项属性中的"auto."前缀
            IntrospectionSupport.extractProperties(options, "auto.");

            if (!options.isEmpty()) {
                throw new IllegalArgumentException("Invalid connect parameters: " + options);
            }
            return rc;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    /**
     * 创建精简版的transport对象
     */
    public Transport doCompositeConnect(URI location) throws Exception {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));

            WireFormat wf = createWireFormat(options);

            Transport transport = createTransport(location, wf);

            Transport rc = compositeConfigure(transport, wf, options);

            if (!options.isEmpty()) {
                throw new IllegalArgumentException("Invalid connect parameters: " + options);
            }
            return rc;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }



    public static void registerTransportFactory(String scheme, TransportFactory tf) {
        TRANSPORT_FACTORYS.put(scheme, tf);
    }


    protected Transport createTransport(URI location, WireFormat wf) throws MalformedURLException, UnknownHostException, IOException {
        throw new IOException("createTransport() method not implemented!");
    }

    /**
     * 根据URI前缀生成对应的TransportFactory实例（工厂模式）
     */
    public static TransportFactory findTransportFactory(URI location) throws IOException {
        String scheme = location.getScheme();
        if (scheme == null) {
            throw new IOException("Transport not scheme specified: [" + location + "]");
        }

        TransportFactory tf = TRANSPORT_FACTORYS.get(scheme);

        if (tf == null) {
            try {
                /**
                 * 在META-INF/services/org/apache/activemq/transport/路径下对应的tcp、udp等文件中，
                 * 取出其中class对应的值，该值是各个schema对应的transport具体实现类的包路径
                 * 然后利用Java反射机制生成对应transport类的实例对象
                 */
                tf = (TransportFactory)TRANSPORT_FACTORY_FINDER.newInstance(scheme);

                TRANSPORT_FACTORYS.put(scheme, tf);

            } catch (Throwable e) {
                throw IOExceptionSupport.create("Transport scheme NOT recognized: [" + scheme + "]", e);
            }
        }
        return tf;
    }

    /**
     * 根据选项配置参数生成对应的WireFormat对象
     */
    protected WireFormat createWireFormat(Map<String, String> options) throws IOException {
        WireFormatFactory factory = createWireFormatFactory(options);
        WireFormat format = factory.createWireFormat();
        return format;
    }

    protected WireFormatFactory createWireFormatFactory(Map<String, String> options) throws IOException {
        String wireFormat = options.remove("wireFormat");
        if (wireFormat == null) {
            wireFormat = getDefaultWireFormatType();
        }

        try {
            WireFormatFactory wff = (WireFormatFactory)WIREFORMAT_FACTORY_FINDER.newInstance(wireFormat);
            IntrospectionSupport.setProperties(wff, options, "wireFormat.");
            return wff;
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Could not create wire format factory for: " + wireFormat + ", reason: " + e, e);
        }
    }

    protected String getDefaultWireFormatType() {
        return "default";
    }

    /**
     * 客户端transport属性配置，同时添加对应的transport filter
     */
    @SuppressWarnings("rawtypes")
    public Transport configure(Transport transport, WireFormat wf, Map options) throws Exception {
        //
        transport = compositeConfigure(transport, wf, options);

        // MutexTransportFilter类实现了对每个请求的同步锁，同一时间只允许发送一个请求，如果有第二个请求需要等待第一个请求发送完毕才可继续发送。
        transport = new MutexTransport(transport);

        // ResponseCorrelator实现了异步请求但需要获取响应信息否则就会阻塞等待功能
        transport = new ResponseCorrelator(transport);

        return transport;
    }

    /**
     * 服务端端transport属性配置，同时添加对应的transport filters，broker使用
     * 这和configure()方法之间的主要区别是，broker不发请求到客户端，不需要responsecorrelator
     */
    @SuppressWarnings("rawtypes")
    public Transport serverConfigure(Transport transport, WireFormat format, HashMap options) throws Exception {

        if (options.containsKey(THREAD_NAME_FILTER)) {
            transport = new ThreadNameFilter(transport);
        }
        transport = compositeConfigure(transport, format, options);

        transport = new MutexTransport(transport);

        return transport;
    }

    /**
     * transport属性配置
     */
    @SuppressWarnings("rawtypes")
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {

        // 配置参数中若包含soWriteTimeout，则将transport对象封装成WriteTimeoutFilter并设置WriteTimeout属性
        if (options.containsKey(WRITE_TIMEOUT_FILTER)) {
            transport = new WriteTimeoutFilter(transport);
            String soWriteTimeout = (String)options.remove(WRITE_TIMEOUT_FILTER);
            if (soWriteTimeout!=null) {
                ((WriteTimeoutFilter)transport).setWriteTimeout(Long.parseLong(soWriteTimeout));
            }
        }

        // 利用Java反射机制给transport对象属性赋值
        IntrospectionSupport.setProperties(transport, options);

        return transport;
    }

    @SuppressWarnings("rawtypes")
    protected String getOption(Map options, String key, String def) {
        String rc = (String) options.remove(key);
        if( rc == null ) {
            rc = def;
        }
        return rc;
    }
}
