/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.archive;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayMerge2;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.aeron.archive.ArchiveSystemTests.*;
import static io.aeron.archive.codecs.SourceLocation.REMOTE;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ReplayMerge2Test
{
    private static final String MESSAGE_PREFIX = "Message-Prefix-";
    private static final int STREAM_ID = 1033;
    private static final String CONTROL_ENDPOINT = "localhost:23265";
    private static final String RECORDING_ENDPOINT = "localhost:23266";
    private static final long GROUP_TAG = 99901L;
    private static final int INITIAL_MESSAGE_COUNT = 15;

    private final String publicationChannel = new ChannelUriStringBuilder()
        .media(CommonContext.UDP_MEDIA)
        .controlEndpoint(CONTROL_ENDPOINT)
        .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
        .termLength(TERM_LENGTH)
        .taggedFlowControl(GROUP_TAG, 1, "5s")
        .build();

    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    private final MutableLong receivedMessageCount = new MutableLong();
    private final MutableLong receivedPosition = new MutableLong();
    private final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();

    private TestMediaDriver driver;
    private Archive archive;
    private Aeron aeron;
    private AeronArchive aeronArchive;
    private int messagesPublished = 0;

    private final FragmentHandler fragmentHandler = new FragmentAssembler(
        (buffer, offset, length, header) ->
        {
            final String expected = MESSAGE_PREFIX + receivedMessageCount.get();
            final String actual = buffer.getStringWithoutLengthAscii(offset, length);

            //System.err.println("GOT MESSAGE :: " + actual);

            assertEquals(expected, actual);
            receivedMessageCount.incrementAndGet();
            receivedPosition.set(header.position());
        });

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void before()
    {
        final File archiveDir = new File(SystemUtil.tmpDirName(), "archive");

        driver = TestMediaDriver.launch(
            mediaDriverContext
                .termBufferSparseFile(true)
                .publicationTermBufferLength(TERM_LENGTH)
                .threadingMode(ThreadingMode.SHARED)
                .spiesSimulateConnection(false)
                .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(10))
                .dirDeleteOnStart(true),
            systemTestWatcher);
        systemTestWatcher.dataCollector().add(driver.context().aeronDirectory());

        archive = Archive.launch(
            TestContexts.localhostArchive()
                .catalogCapacity(CATALOG_CAPACITY)
                .aeronDirectoryName(driver.context().aeronDirectoryName())
                .archiveDir(archiveDir)
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(true));
        systemTestWatcher.dataCollector().add(archive.context().archiveDir());

        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(mediaDriverContext.aeronDirectoryName()));

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .errorHandler(Tests::onError)
                .controlRequestChannel(archive.context().localControlChannel())
                .controlRequestStreamId(archive.context().localControlStreamId())
                .controlResponseChannel(archive.context().localControlChannel())
                .aeron(aeron));
    }

    @AfterEach
    void after()
    {
        System.out.println("received " + receivedMessageCount.get() + ", sent " + messagesPublished);

        CloseHelper.closeAll(aeronArchive, aeron, archive, driver);
    }

    @Test
    @InterruptAfter(30)
    void shouldMergeFromReplayToLive() throws Exception
    {
        try (Publication publication = aeron.addPublication(publicationChannel, STREAM_ID))
        {
            final String recordingChannel = new ChannelUriStringBuilder()
                .media(CommonContext.UDP_MEDIA)
                .endpoint(RECORDING_ENDPOINT)
                .controlEndpoint(CONTROL_ENDPOINT)
                .sessionId(publication.sessionId())
                .groupTag(GROUP_TAG)
                .build();

            aeronArchive.startRecording(recordingChannel, STREAM_ID, REMOTE, true);
            final CountersReader counters = aeron.countersReader();
            final int recordingCounterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(counters, recordingCounterId);

            Tests.awaitConnected(publication);
            publishMessages(publication);
            Tests.awaitPosition(counters, recordingCounterId, publication.position());

            final Subscription subscription = aeron.addSubscription(publicationChannel, STREAM_ID);
            Tests.awaitConnected(subscription);

            final ReplayMerge2 rm2 = new ReplayMerge2(
                subscription.imageAtIndex(0),
                aeronArchive,
                new ChannelUriStringBuilder()
                    .media("udp")
                    //.controlMode("response")
                    .endpoint("localhost:23300")
                    .toString(),
                1234,
                recordingId,
                0,
                archive.context().aeron().context().epochClock()
            );

            int idx = INITIAL_MESSAGE_COUNT;

            offerMessage(publication, idx++);
            offerMessage(publication, idx++);
            rm2.poll(fragmentHandler, 10);
            Thread.sleep(200);
            rm2.poll(fragmentHandler, 10);
            Thread.sleep(100);
            rm2.poll(fragmentHandler, 10);
            offerMessage(publication, idx++);
            offerMessage(publication, idx++);
            Thread.sleep(100);
            rm2.poll(fragmentHandler, 10);
            Thread.sleep(100);
            rm2.poll(fragmentHandler, 10);
            Thread.sleep(100);
            rm2.poll(fragmentHandler, 10);

            assertTrue(rm2.isMerged());
        }
    }

    @Test
    @InterruptAfter(30)
    void shouldMergeFromReplayToLiveIPC() throws Exception
    {
        final String ipcPublicationChannel = new ChannelUriStringBuilder()
            .media(CommonContext.IPC_MEDIA)
            .termLength(TERM_LENGTH)
            .taggedFlowControl(GROUP_TAG, 1, "5s")
            .build();

        try (Publication publication = aeron.addPublication(ipcPublicationChannel, STREAM_ID))
        {
            final String recordingChannel = new ChannelUriStringBuilder()
                .media(CommonContext.IPC_MEDIA)
                .sessionId(publication.sessionId())
                .groupTag(GROUP_TAG)
                .build();

            aeronArchive.startRecording(recordingChannel, STREAM_ID, REMOTE, true);
            final CountersReader counters = aeron.countersReader();
            final int recordingCounterId =
                Tests.awaitRecordingCounterId(counters, publication.sessionId(), aeronArchive.archiveId());
            final long recordingId = RecordingPos.getRecordingId(counters, recordingCounterId);

            Tests.awaitConnected(publication);
            publishMessages(publication);
            Tests.awaitPosition(counters, recordingCounterId, publication.position());

            final Subscription subscription = aeron.addSubscription(ipcPublicationChannel, STREAM_ID);
            Tests.awaitConnected(subscription);

            final ReplayMerge2 rm2 = new ReplayMerge2(
                subscription.imageAtIndex(0),
                aeronArchive,
                new ChannelUriStringBuilder()
                    .media("udp")
                    //.controlMode("response")
                    .endpoint("localhost:23300")
                    .toString(),
                1234,
                recordingId,
                0,
                archive.context().aeron().context().epochClock()
            );

            int idx = INITIAL_MESSAGE_COUNT;

            offerMessage(publication, idx++);
            offerMessage(publication, idx++);
            rm2.poll(fragmentHandler, 10);
            Thread.sleep(200);
            rm2.poll(fragmentHandler, 10);
            Thread.sleep(100);
            rm2.poll(fragmentHandler, 10);
            offerMessage(publication, idx++);
            offerMessage(publication, idx++);
            Thread.sleep(100);
            rm2.poll(fragmentHandler, 10);
            Thread.sleep(100);
            rm2.poll(fragmentHandler, 10);
            Thread.sleep(100);
            rm2.poll(fragmentHandler, 10);

            assertTrue(rm2.isMerged());
        }
    }

    private long offerMessage(final Publication publication, final int index)
    {
        //System.err.println("send message :: " + index);

        int length = buffer.putStringWithoutLengthAscii(0, MESSAGE_PREFIX);
        length += buffer.putIntAscii(length, index);

        messagesPublished++;

        return publication.offer(buffer, 0, length);
    }

    private void publishMessages(final Publication publication)
    {
        for (int i = 0; i < INITIAL_MESSAGE_COUNT; i++)
        {
            int length = buffer.putStringWithoutLengthAscii(0, MESSAGE_PREFIX);
            length += buffer.putIntAscii(length, i);

            while (publication.offer(buffer, 0, length) <= 0)
            {
                Tests.yield();
            }
        }

        messagesPublished = INITIAL_MESSAGE_COUNT;
    }
}
