package de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDateBuilder;
import de.invesdwin.util.time.fdate.ftimezone.FTimeZone;
import de.invesdwin.util.time.range.TimeRange;

@NotThreadSafe
public class PeriodicalSegmentFinderTest extends ATest {

    @Test
    public void testTimeZone() {
        final PeriodicalSegmentFinder finder = PeriodicalSegmentFinder.newInstance(Duration.ONE_DAY);
        final TimeRange segment = finder
                .getSegment(FDateBuilder.newDate(2021, 01, 01).applyTimeZoneOffset(FTimeZone.EET))
                .revertTimeZoneOffset(FTimeZone.EET);
        Assertions.assertThat(segment.toString())
                .isEqualTo("2020-12-31T22:00:00.000 -> 2021-01-01T21:59:59.999 => PT23H59M59.999S");
    }

}
