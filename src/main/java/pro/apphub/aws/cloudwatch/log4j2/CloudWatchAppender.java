/*
 * Copyright (C) 2017 Dmitry Kotlyarov.
 * All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pro.apphub.aws.cloudwatch.log4j2;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.DescribeLogGroupsRequest;
import com.amazonaws.services.logs.model.DescribeLogGroupsResult;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.LogGroup;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public final class CloudWatchAppender extends AbstractAppender {
    public static final String INSTANCE = retrieveInstance();

    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicBoolean flag = new AtomicBoolean(true);
    private final AtomicLong lost = new AtomicLong(0L);
    private final String group;
    private final String stream;
    private final AWSLogsClient client;
    private final Buffer buffer1;
    private final Buffer buffer2;
    private final FlushWait flushWait;
    private final Thread flushThread;

    public CloudWatchAppender(String name,
                              String group,
                              String streamPrefix,
                              String streamPostfix,
                              String access,
                              String secret,
                              int capacity,
                              int span,
                              Layout<? extends Serializable> layout) {
        super(name, null, (layout != null) ? layout : PatternLayout.createDefaultLayout(), false);

        if (group != null) {
            this.group = group;
            this.stream = initStream(streamPrefix, streamPostfix);
            this.client = initClient(access, secret);
            this.buffer1 = new Buffer(capacity);
            this.buffer2 = new Buffer(capacity);
            this.flushWait = new FlushWait(span);
            this.flushThread = new Thread("aws-cloudwatch-log4j2-flush") {
                @Override
                public void run() {
                    while (enabled.get()) {
                        try {
                            flushWait.await(buffer1, buffer2);
                        } catch (Throwable e) {
                        }
                    }
                }
            };

            if (!checkGroup(group, client)) {
                throw new RuntimeException(String.format("Group '%s' is not found", group));
            }
        } else {
            this.group = null;
            this.stream = null;
            this.client = null;
            this.buffer1 = null;
            this.buffer2 = null;
            this.flushWait = null;
            this.flushThread = null;
        }
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public String getGroup() {
        return group;
    }

    public String getStream() {
        return stream;
    }

    @Override
    public void start() {
        super.start();
        if (group != null) {
            enabled.set(true);
            flushThread.setDaemon(false);
            flushThread.start();
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (group != null) {
            enabled.set(false);
            flushWait.signalAll();
            try {
                flushThread.join();
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void append(LogEvent event) {
        if (enabled.get()) {
            InputLogEvent e = new InputLogEvent();
            e.setTimestamp(event.getTimeMillis());
            e.setMessage(new String(getLayout().toByteArray(event)));
            if (flag.get()) {
                if (!buffer1.append(e, flushWait)) {
                    if (buffer2.append(e, flushWait)) {
                        flag.set(false);
                    } else {
                        lost.incrementAndGet();
                    }
                }
            } else {
                if (!buffer2.append(e, flushWait)) {
                    if (buffer1.append(e, flushWait)) {
                        flag.set(true);
                    } else {
                        lost.incrementAndGet();
                    }
                }
            }
        }
    }

    @PluginFactory
    public static CloudWatchAppender createAppender(@PluginAttribute("name") String name,
                                                    @PluginAttribute("group") String group,
                                                    @PluginAttribute("streamPrefix") String streamPrefix,
                                                    @PluginAttribute("streamPostfix") String streamPostfix,
                                                    @PluginAttribute("access") String access,
                                                    @PluginAttribute("secret") String secret,
                                                    @PluginAttribute("capacity") String capacity,
                                                    @PluginAttribute("span") String span,
                                                    @PluginElement("Layout") Layout<? extends Serializable> layout) {
        return new CloudWatchAppender((name != null) ? name : "CloudWatchAppender",
                                      getProperty("aws.cloudwatch.group", "AWS_CLOUDWATCH_GROUP", group, null),
                                      getProperty("aws.cloudwatch.stream.prefix", "AWS_CLOUDWATCH_STREAM_PREFIX", streamPrefix, null),
                                      getProperty("aws.cloudwatch.stream.postfix", "AWS_CLOUDWATCH_STREAM_POSTFIX", streamPostfix, null),
                                      getProperty("aws.cloudwatch.access", "AWS_CLOUDWATCH_ACCESS", access, null),
                                      getProperty("aws.cloudwatch.secret", "AWS_CLOUDWATCH_SECRET", secret, null),
                                      Integer.parseInt(getProperty("aws.cloudwatch.capacity", "AWS_CLOUDWATCH_CAPACITY", capacity, "10000")),
                                      Integer.parseInt(getProperty("aws.cloudwatch.span", "AWS_CLOUDWATCH_SPAN", span, "60")),
                                      layout);
    }

    private static String retrieveInstance() {
        try {
            URL url = new URL("http://169.254.169.254/latest/meta-data/instance-id");
            URLConnection conn = url.openConnection();
            conn.setConnectTimeout(10000);
            try (InputStream in = conn.getInputStream()) {
                BufferedReader r = new BufferedReader(new InputStreamReader(in));
                String instance = r.readLine();
                if (instance != null) {
                    return instance;
                } else {
                    throw new IOException("Instance is null");
                }
            }
        } catch (IOException e) {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e1) {
                throw new RuntimeException(e1);
            }
        }
    }

    private static String getProperty(String property, String variable, String value) {
        String v = getProperty(property, variable, value, null);
        if (v != null) {
            return v;
        } else {
            throw new RuntimeException(String.format("Property ['%s', '%s'] is not defined", property, variable));
        }
    }

    private static String getProperty(String property, String variable, String value, String def) {
        String v = System.getProperty(property);
        if (v != null) {
            return v;
        } else {
            v = System.getenv(variable);
            if (v != null) {
                return v;
            } else {
                if (value != null) {
                    return value;
                } else {
                    return def;
                }
            }
        }
    }

    private static String initStream(String prefix, String postfix) {
        String s = INSTANCE;
        if (prefix != null) {
            s = String.format("%s/%s", prefix, s);
        }
        if (postfix != null) {
            s = String.format("%s/%s", s, postfix);
        }
        return s;
    }

    private static AWSLogsClient initClient(String access, String secret) {
        if ((access != null) && (secret != null)) {
            return new AWSLogsClient(new BasicAWSCredentials(access, secret));
        } else {
            return new AWSLogsClient();
        }
    }

    private static boolean checkGroup(String group, AWSLogsClient client) {
        DescribeLogGroupsResult dlgr = client.describeLogGroups(new DescribeLogGroupsRequest().withLogGroupNamePrefix(group));
        if (dlgr != null) {
            List<LogGroup> lgs = dlgr.getLogGroups();
            if (lgs != null) {
                for (LogGroup lg : lgs) {
                    if (lg.getLogGroupName().equals(group)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
