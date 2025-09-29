package io.aeron.archive.client;

import io.aeron.*;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.EpochClock;

public class ReplayMerge2
{
    @SuppressWarnings("checkstyle:JavadocVariable")
    enum State
    {
        REPLAY,
        CATCHUP,
        ATTEMPT_LIVE_JOIN,
        MERGED,
        FAILED,
        CLOSED
    }

    private final Image liveImage;
    private final AeronArchive archive;
    private final long recordingId;
    private final long startPosition;
    private final EpochClock epochClock;
    private final ChannelUri replayChannelUri;

    private State state;

    private Subscription replaySubscription = null;
    private Counter replayBoundingLimitCounter = null;

    public ReplayMerge2(
        final Image liveImage,
        final AeronArchive archive,
        final String replayChannel,
        final long recordingId,
        final long startPosition,
        final EpochClock epochClock)
    {
        this.liveImage = liveImage;
        this.archive = archive;
        this.recordingId = recordingId;
        this.startPosition = startPosition;
        this.epochClock = epochClock;

        replayChannelUri = ChannelUri.parse(replayChannel);
        //replayChannelUri.put(CommonContext.LINGER_PARAM_NAME, "0");
        //replayChannelUri.put(CommonContext.EOS_PARAM_NAME, "false");

        state = State.REPLAY;
    }

    public void close()
    {
        // TODO
    }

    public int doWork()
    {
        int workCount = 0;
        final long nowMs = epochClock.time();

        try
        {
            switch (state)
            {
                case REPLAY:
                    workCount += replay(nowMs);
                    break;

                case CATCHUP:
                    workCount += catchup(nowMs);
                    break;

                case ATTEMPT_LIVE_JOIN:
                    workCount += attemptLiveJoin(nowMs);
                    break;

                case MERGED:
                case CLOSED:
                case FAILED:
                    break;
            }
        }
        catch (final Exception ex)
        {
            state(State.FAILED);
            throw ex;
        }

        return workCount;
    }

    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        doWork();

        int frags;

        if (State.CATCHUP == state)
        {
            frags = replaySubscription.poll(fragmentHandler, fragmentLimit);
            System.err.println("replay :: " + fragmentLimit + " :: " + frags);

            // drain the live image a bit - TODO or should we drain it 'all the way'??
            frags = liveImage.poll((buffer, offset, length, header) -> {}, fragmentLimit);
            System.err.println("drain live image :: " + fragmentLimit + " :: " + frags);
        }
        else if (State.ATTEMPT_LIVE_JOIN == state)
        {
            frags = replaySubscription.poll(fragmentHandler, fragmentLimit);
            System.err.println("(ALJ) replay :: " + fragmentLimit + " :: " + frags);

            // stop draining the live image
        }
        else if (State.MERGED == state)
        {
            // only poll the live image from now on
            frags = liveImage.poll(fragmentHandler, fragmentLimit);
            System.err.println("MERGED poll live image :: " + fragmentLimit + " :: " + frags);
        }

        return 0;
    }

    private void state(final State newState)
    {
        System.out.println(state + " -> " + newState);
        state = newState;
    }

    private int replay(final long nowMs)
    {
        int workCount = 0;

        if (null == replaySubscription)
        {
            // TODO make this async?
            replayBoundingLimitCounter = archive.context().aeron().addCounter(1000, "replay bounding limit counter");

            replayBoundingLimitCounter.set(liveImage.position());

            // TODO make this async?
            replaySubscription = archive.replay(
                recordingId,
                replayChannelUri.toString(),
                1234, // TODO
                new ReplayParams()
                    .position(startPosition)
                    .boundingLimitCounterId(replayBoundingLimitCounter.id())
            );

            state(State.CATCHUP);
            workCount += 1;
        }

        return workCount;
    }

    private int catchup(final long nowMs)
    {
        int workCount = 0;

        if (replaySubscription.imageCount() > 0)
        {
            final long replayPosition = replaySubscription.imageAtIndex(0).position();
            final long livePosition = liveImage.position();

            replayBoundingLimitCounter.set(liveImage.position());

            if (livePosition - replayPosition < 100)
            {
                state(State.ATTEMPT_LIVE_JOIN);

                workCount += 1;
            }
        }

        return workCount;
    }

    private int attemptLiveJoin(final long nowMs)
    {
        int workCount = 0;

        final long replayPosition = replaySubscription.imageAtIndex(0).position();
        final long livePosition = liveImage.position();

        if (livePosition == replayPosition)
        {
            state(State.MERGED);

            // TODO stop the replay...  Just delete the subscription?
            replaySubscription.close();
            // archive.stopReplay(0); TODO

            workCount += 1;
        }

        return workCount;
    }
}
