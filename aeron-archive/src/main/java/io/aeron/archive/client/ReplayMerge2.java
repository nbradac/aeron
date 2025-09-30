package io.aeron.archive.client;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.EpochClock;

import static io.aeron.CommonContext.SESSION_ID_PARAM_NAME;

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
    private final int replayStreamId;
    private final long recordingId;
    private final long startPosition;
    private final EpochClock epochClock;
    private final ChannelUri replayChannelUri;
    private final long minimumWindow;

    private State state;

    private Subscription replaySubscription = null;
    private Counter replayBoundingLimitCounter = null;

    public ReplayMerge2(
        final Image liveImage,
        final AeronArchive archive,
        final String replayChannel,
        final int replayStreamId,
        final long recordingId,
        final long startPosition,
        final EpochClock epochClock)
    {
        this.liveImage = liveImage;
        this.archive = archive;

        replayChannelUri = ChannelUri.parse(replayChannel);
        replayChannelUri.put(CommonContext.LINGER_PARAM_NAME, "0");
        replayChannelUri.put(CommonContext.EOS_PARAM_NAME, "false");

        this.replayStreamId = replayStreamId;
        this.recordingId = recordingId;
        this.startPosition = startPosition;
        this.epochClock = epochClock;

        minimumWindow = 400;  // TODO should this be a function of the liveImage's termBufferLength?

        state = State.REPLAY;
    }

    public void close()
    {
        if (State.CLOSED != state)
        {
            if (null != replaySubscription)
            {
                stopReplay();
            }

            state(State.CLOSED);
        }
    }

    public int doWork()
    {
        int workCount = 0;
        final long nowMs = epochClock.time();

        try
        {
            workCount += switch (state)
            {
                case REPLAY -> replay(nowMs);
                case CATCHUP -> catchup(nowMs);
                case ATTEMPT_LIVE_JOIN -> attemptLiveJoin(nowMs);
                case MERGED, CLOSED, FAILED -> 0;
            };
        }
        catch (final Exception ex)
        {
            state(State.FAILED);
            throw ex;
        }

        return workCount;
    }

    public boolean isMerged()
    {
        return state == State.MERGED;
    }

    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        int workCount = 0;

        workCount += doWork();

        workCount += switch (state)
        {
            case CATCHUP -> replaySubscription.poll(fragmentHandler, fragmentLimit) +
                liveImage.poll((buffer, offset, length, header) -> {}, fragmentLimit);
            case ATTEMPT_LIVE_JOIN -> replaySubscription.poll(fragmentHandler, fragmentLimit);
            case MERGED -> liveImage.poll(fragmentHandler, fragmentLimit);
            case REPLAY, CLOSED, FAILED -> 0;
        };

        return workCount;
    }

    private void state(final State newState)
    {
        //System.out.println(state + " -> " + newState);
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
                replayStreamId,
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
            replayBoundingLimitCounter.set(liveImage.position());

            if (currentWindow() < minimumWindow)
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

        if (currentWindow() == 0)
        {
            state(State.MERGED);

            stopReplay();

            workCount += 1;
        }

        return workCount;
    }

    private long currentWindow()
    {
        return liveImage.position() - replaySubscription.imageAtIndex(0).position();
    }

    private void stopReplay()
    {
        archive.stopReplay(
            Long.parseLong(
                ChannelUri.parse(replaySubscription.channel()).get(SESSION_ID_PARAM_NAME)
            )
        );

        replaySubscription.close();
        replaySubscription = null;
    }
}
