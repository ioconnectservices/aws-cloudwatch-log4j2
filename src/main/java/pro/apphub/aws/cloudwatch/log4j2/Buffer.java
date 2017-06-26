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

import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.InputLogEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
final class Buffer {
    private final AtomicBoolean flush = new AtomicBoolean(false);
    private final AtomicInteger threads = new AtomicInteger(0);
    private final LinkedBlockingQueue<InputLogEvent> events;
    private final ArrayList<InputLogEvent> eventsList;
    private final ArrayList<InputLogEvent> eventsPutList;

    public Buffer(int capacity) {
        this.events = new LinkedBlockingQueue<>(capacity);
        this.eventsList = new ArrayList<>(capacity);
        this.eventsPutList = new ArrayList<>(capacity);
    }

    public boolean append(InputLogEvent event) {
        if (!flush.get()) {
            threads.incrementAndGet();
            try {
                if (!flush.get()) {
                    return events.offer(event);
                } else {
                    return false;
                }
            } finally {
                threads.decrementAndGet();
            }
        } else {
            return false;
        }
    }

    public FlushInfo flush(AWSLogsClient logsClient, String group, String stream, FlushInfo flushInfo, AtomicLong lost) {
        flush.set(true);
        try {
            while (threads.get() > 0) {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                events.drainTo(eventsList);
                Collections.sort(eventsList, new Comparator<InputLogEvent>() {
                    @Override
                    public int compare(InputLogEvent o1, InputLogEvent o2) {
                        return o1.getTimestamp().compareTo(o2.getTimestamp());
                    }
                });
                if (flushInfo.lastTimestamp > 0) {
                    for (InputLogEvent event : eventsList) {
                        if (event.getTimestamp() < flushInfo.lastTimestamp) {
                            event.setTimestamp(flushInfo.lastTimestamp);
                        } else {
                            break;
                        }
                    }
                }
                int c = 0;
                int s = 0;
                for (InputLogEvent event : eventsList) {
                    int es = event.getMessage().length() * 4 + 26;
                    if ((c + 1 < 10000) && (s + es < 1048576)) {
                        c++;
                        s += es;
                        eventsPutList.add(event);
                    } else {
                        c = 1;
                        s = es;
                        eventsPutList.clear();
                        eventsPutList.add(event);
                    }
                }
            } finally {
                eventsList.clear();
            }
        } finally {
            flush.set(false);
        }
    }
}
