/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.samples;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Tracker and reporter of throughput rates.
 */
public class RateReporter implements Runnable
{
    /**
     * Interface for reporting of rate information
     */
    @FunctionalInterface
    public interface Reporter
    {
        /**
         * Called for a rate report.
         *
         * @param messagesPerSec since last report
         * @param bytesPerSec    since last report
         * @param totalMessages  since beginning of reporting
         * @param totalBytes     since beginning of reporting
         */
        void onReport(double messagesPerSec, double bytesPerSec, long totalMessages, long totalBytes);
    }

    private final long reportIntervalNs;
    private final long parkNs;
    private long lastTotalBytes;
    private long lastTotalMessages;
    private long lastTimestamp;
    private volatile boolean halt = false;
    private final AtomicLong totalBytes = new AtomicLong();
    private final AtomicLong totalMessages = new AtomicLong();
    private final Reporter reportingFunc;

    /**
     * Create a rate reporter with the given report interval in nanoseconds and the reporting function.
     *
     * @param reportInterval in nanoseconds
     * @param reportingFunc  to call for reporting rates
     */
    public RateReporter(final long reportInterval, final Reporter reportingFunc)
    {
        this.reportIntervalNs = reportInterval;
        this.parkNs = reportInterval;
        this.reportingFunc = reportingFunc;
        lastTimestamp = System.nanoTime();
    }

    /**
     * Run loop for the rate reporter
     */
    public void run()
    {
        do
        {
            LockSupport.parkNanos(parkNs);

            final long currentTotalMessages = totalMessages.get();
            final long currentTotalBytes = totalBytes.get();
            final long currentTimestamp = System.nanoTime();

            final long timeSpanNs = currentTimestamp - lastTimestamp;
            final double messagesPerSec =
                ((currentTotalMessages - lastTotalMessages) * (double)reportIntervalNs) / (double)timeSpanNs;
            final double bytesPerSec =
                ((currentTotalBytes - lastTotalBytes) * (double)reportIntervalNs) / (double)timeSpanNs;

            reportingFunc.onReport(messagesPerSec, bytesPerSec, currentTotalMessages, currentTotalBytes);

            lastTotalBytes = currentTotalBytes;
            lastTotalMessages = currentTotalMessages;
            lastTimestamp = currentTimestamp;
        }
        while (!halt);
    }

    /**
     * Signal the run loop to exit. Does not block.
     */
    public void halt()
    {
        halt = true;
    }

    /**
     * Notify rate reporter of number of messages and bytes received, sent, etc.
     *
     * @param messages received, sent, etc.
     * @param bytes    received, sent, etc.
     */
    public void onMessage(final long messages, final long bytes)
    {
        totalBytes.lazySet(totalBytes.get() + bytes);
        totalMessages.lazySet(totalMessages.get() + messages);
    }
}
