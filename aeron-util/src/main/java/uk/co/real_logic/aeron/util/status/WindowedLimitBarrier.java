/*
 * Copyright 2014 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.aeron.util.status;

/**
 * An implementation of a limit barrier that defines a limit as being a window further along than a position.
 */
public class WindowedLimitBarrier implements LimitBarrier
{
    private final PositionIndicator indicator;
    private final long window;

    public WindowedLimitBarrier(final PositionIndicator indicator, final long window)
    {
        this.indicator = indicator;
        this.window = window;
    }

    public long limit()
    {
        return indicator.position() + window;
    }
}
