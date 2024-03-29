package de.invesdwin.context.persistence.timeseriesdb.segmented;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.timezone.FTimeZone;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.range.TimeRange;

@NotThreadSafe
public class PeriodicalSegmentFinderTest extends ATest {

    @SuppressWarnings("deprecation")
    @Test
    public void testTimeZone() {
        final PeriodicalSegmentFinder finder = PeriodicalSegmentFinder.newInstance(Duration.ONE_DAY);
        final FDate reference = FDateBuilder.newDate(2021, 01, 01);
        final FTimeZone offsetTimeZone = FTimeZone.EET;
        final TimeRange segment = finder.getSegment(reference.applyTimeZoneOffset(offsetTimeZone))
                .revertTimeZoneOffset(offsetTimeZone);
        Assertions.assertThat(segment.toString())
                .isEqualTo("2020-12-31T22:00:00.000 -> 2021-01-01T21:59:59.999 => PT23H59M59.999S");
    }

}
