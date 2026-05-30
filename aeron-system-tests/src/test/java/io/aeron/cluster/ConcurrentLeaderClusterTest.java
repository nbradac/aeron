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
package io.aeron.cluster;

import io.aeron.cluster.service.Cluster;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import net.bytebuddy.asm.Advice;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Attempts to reliably and quickly force the cluster into a state with two concurrent leaders (see
 * INVESTIGATION-dual-leader-truncation.md in the aeron-sequencer repo). Uses ByteBuddy instrumentation
 * ({@link ClusterInstrumentor}) to stall a freshly-elected leader inside {@code Election.leaderLogReplication} so that,
 * while it is finalizing leadership for term T, the remaining nodes time out and elect a new leader for term T+1 -
 * yielding two leaders at once. Deterministic and seed-free.
 */
@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ConcurrentLeaderClusterTest
{
    private static ClusterInstrumentor stallInstrumentor;

    @BeforeAll
    static void beforeAll()
    {
        stallInstrumentor = new ClusterInstrumentor(
            StallLeaderLogReplicationIntercept.class, "Election", "leaderLogReplication");
    }

    @AfterAll
    static void afterAll()
    {
        if (null != stallInstrumentor)
        {
            stallInstrumentor.reset();
        }
    }

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    public static class StallLeaderLogReplicationIntercept
    {
        static volatile boolean shouldStall = true;

        @Advice.OnMethodEnter
        static void leaderLogReplication(final long nowNs, @Advice.This final Object election)
        {
            if (shouldStall && 0 == ((Election)election).leadershipTermId())
            {
                shouldStall = false;
                System.out.println("[STALL] stalling leaderLogReplication for term 0");
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(3));
                System.out.println("[STALL] released leaderLogReplication stall");
            }
        }
    }

    @Test
    @InterruptAfter(60)
    void shouldGetIntoConcurrentLeaderState() throws InterruptedException
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        int maxConcurrentLeaders = 0;
        String prevLine = "";
        final long deadlineMs = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadlineMs)
        {
            int leaderish = 0;
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 3; i++)
            {
                sb.append('n').append(i).append('[');
                final TestNode node = cluster.node(i);
                if (null == node || node.isClosed())
                {
                    sb.append("closed]");
                    continue;
                }
                Cluster.Role role = null;
                ElectionState es = null;
                try
                {
                    role = node.role();
                    es = node.electionState();
                }
                catch (final Exception ignore)
                {
                }
                sb.append("role=").append(role).append(",es=").append(es).append(']').append(' ');

                final boolean isLeaderish = Cluster.Role.LEADER == role ||
                    ElectionState.LEADER_LOG_REPLICATION == es ||
                    ElectionState.LEADER_REPLAY == es ||
                    ElectionState.LEADER_INIT == es ||
                    ElectionState.LEADER_READY == es;
                if (isLeaderish)
                {
                    leaderish++;
                }
            }

            final String line = sb.toString();
            if (!line.equals(prevLine))
            {
                System.out.println("[ROLES] leaderish=" + leaderish + " : " + line);
                prevLine = line;
            }
            if (leaderish > maxConcurrentLeaders)
            {
                maxConcurrentLeaders = leaderish;
                System.out.println("[CONCURRENT-LEADERS] new max = " + leaderish + " : " + line);
            }

            if (maxConcurrentLeaders >= 2)
            {
                break; // observed the concurrent-leader state; no need to keep polling
            }

            //noinspection BusyWait
            Thread.sleep(20);
        }

        System.out.println("[CONCURRENT-LEADERS] max observed leaderish nodes = " + maxConcurrentLeaders);
        assertTrue(
            maxConcurrentLeaders >= 2,
            "expected to observe two concurrent leaders, but max observed = " + maxConcurrentLeaders);
    }
}
