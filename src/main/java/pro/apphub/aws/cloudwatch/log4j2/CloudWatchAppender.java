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

import com.amazonaws.services.logs.model.InputLogEvent;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public final class CloudWatchAppender extends AbstractAppender {
    private static final String instance = retrieveInstance();

    private final Buffer buffer1;
    private final Buffer buffer2;
    private final AtomicBoolean flag = new AtomicBoolean(true);
    private final AtomicLong lost = new AtomicLong(0L);
    private final FlushWait flushWait = new FlushWait(60000L);

    public CloudWatchAppender(String name, Filter filter, Layout<? extends Serializable> layout) {
        super(name, filter, layout);

        this.buffer1 = new Buffer(Buffer.MAX_BATCH_COUNT);
        this.buffer2 = new Buffer(Buffer.MAX_BATCH_COUNT);
    }

    @Override
    public void append(LogEvent event) {
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
}
