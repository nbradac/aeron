/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class MultipleMulticastsSubscriptionsTest
{
    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private final MediaDriver.Context context = new MediaDriver.Context()
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED);
    private TestMediaDriver driver;

    @BeforeEach
    void setUp()
    {
        TestMediaDriver.enableMultiGapLoss(context, 0, 4096, 100, 100);
    }

    private void launch(final MediaDriver.Context context)
    {
        driver = TestMediaDriver.launch(context, watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietClose(driver);
    }

    @Test
    void shouldBindToMultipleMulticastAddressOnTheSamePort()
    {
        launch(context);

        final String uriA = "aeron:udp?endpoint=239.192.11.87:20123";
        final String uriB = "aeron:udp?endpoint=239.192.11.91:20123";

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication pubA = aeron.addPublication(uriA, 10000);
            Publication pubB = aeron.addPublication(uriB, 10000);
            Subscription subA = aeron.addSubscription(uriA, 10000);
            Subscription subB = aeron.addSubscription(uriB, 10000))
        {
            Tests.awaitConnected(pubA);
            Tests.awaitConnected(pubB);
            Tests.awaitConnected(subA);
            Tests.awaitConnected(subB);
        }
    }

    @Test
    void shouldBindToMultipleMulticastAddressOnTheSamePortAsMdsDestinations()
    {
        launch(context);

        final String uriA = "aeron:udp?endpoint=239.192.11.87:20123";
        final String uriB = "aeron:udp?endpoint=239.192.11.91:20123";
        final String mdsSubscription = "aeron:udp?control-mode=manual";

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication pubA = aeron.addPublication(uriA, 10000);
            Publication pubB = aeron.addPublication(uriB, 10000);
            Subscription subA = aeron.addSubscription(mdsSubscription, 10000);
            Subscription subB = aeron.addSubscription(mdsSubscription, 10000))
        {
            subA.addDestination(uriA);
            subB.addDestination(uriB);

            Tests.awaitConnected(pubA);
            Tests.awaitConnected(pubB);
            Tests.awaitConnected(subA);
            Tests.awaitConnected(subB);
        }
    }

    @Test
    void shouldBindToMultipleMulticastAddressOnTheSamePortMixingMdsAndSubscriptions()
    {
        launch(context);

        final String uriA = "aeron:udp?endpoint=239.192.11.87:20123";
        final String uriB = "aeron:udp?endpoint=239.192.11.91:20123";
        final String mdsSubscription = "aeron:udp?control-mode=manual";

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication pubA = aeron.addPublication(uriA, 10000);
            Publication pubB = aeron.addPublication(uriB, 10000);
            Subscription subA = aeron.addSubscription(uriA, 10000);
            Subscription subB = aeron.addSubscription(mdsSubscription, 10000))
        {
            subB.addDestination(uriB);

            Tests.awaitConnected(pubA);
            Tests.awaitConnected(pubB);
            Tests.awaitConnected(subA);
            Tests.awaitConnected(subB);
        }
    }
}
