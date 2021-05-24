package de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.math.expression.lambda.IEvaluateGenericFDate;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FTimeUnit;
import de.invesdwin.util.time.fdate.FWeekday;
import de.invesdwin.util.time.range.TimeRange;

@ThreadSafe
public class PeriodicalSegmentFinder {

    public static final TimeRange DUMMY_RANGE = new TimeRange(FDate.MIN_DATE, FDate.MAX_DATE);
    public static final TimeRange BEFORE_DUMMY_RANGE = new TimeRange(FDate.MIN_DATE.addMilliseconds(-1),
            FDate.MIN_DATE.addMilliseconds(-1));
    public static final TimeRange AFTER_DUMMY_RANGE = new TimeRange(FDate.MAX_DATE.addMilliseconds(1),
            FDate.MAX_DATE.addMilliseconds(1));

    public static final AHistoricalCache<TimeRange> DUMMY_CACHE = new AHistoricalCache<TimeRange>() {

        @Override
        protected Integer getInitialMaximumSize() {
            //no caching
            return 0;
        }

        @Override
        protected FDate innerExtractKey(final TimeRange value) {
            return value.getFrom();
        }

        @Override
        protected IEvaluateGenericFDate<TimeRange> newLoadValue() {
            return pKey -> {
                final FDate key = pKey.asFDate();
                if (key.isBefore(DUMMY_RANGE.getFrom())) {
                    return BEFORE_DUMMY_RANGE;
                } else if (key.isAfter(DUMMY_RANGE.getTo())) {
                    return AFTER_DUMMY_RANGE;
                } else {
                    return DUMMY_RANGE;
                }
            };

        }

        @Override
        protected FDate innerCalculateNextKey(final FDate key) {
            return key;
        }

        @Override
        protected FDate innerCalculatePreviousKey(final FDate key) {
            return key;
        }

        @Override
        public void preloadData(final ExecutorService executor) {
            //noop
        }
    };

    private static final Duration TEN_MILLISECONDS = new Duration(10, FTimeUnit.MILLISECONDS);
    private final Function<FDate, FDate> incrementFunction;
    private final Function<FDate, FDate> decrementFunction;

    private final Duration period;
    private final FTimeUnit boundsTimeUnit;
    @GuardedBy("this")
    private TimeRange calculationBounds = null;
    @GuardedBy("this")
    private TimeRange curTimeRange = null;

    public PeriodicalSegmentFinder(final Duration period, final FTimeUnit incrementTimeUnit, final int incrementCount,
            final FTimeUnit boundsTimeUnit) {
        this.period = period;
        this.incrementFunction = new Function<FDate, FDate>() {
            @Override
            public FDate apply(final FDate t) {
                return t.add(incrementTimeUnit, incrementCount);
            }
        };
        this.decrementFunction = new Function<FDate, FDate>() {
            @Override
            public FDate apply(final FDate t) {
                return t.add(incrementTimeUnit, -incrementCount);
            }
        };
        this.boundsTimeUnit = boundsTimeUnit;
    }

    public PeriodicalSegmentFinder(final Duration period, final FTimeUnit boundsTimeUnit) {
        this.period = period;
        this.incrementFunction = new Function<FDate, FDate>() {
            @Override
            public FDate apply(final FDate t) {
                return t.add(period);
            }
        };
        this.decrementFunction = new Function<FDate, FDate>() {
            @Override
            public FDate apply(final FDate t) {
                return t.subtract(period);
            }
        };
        this.boundsTimeUnit = boundsTimeUnit;
    }

    public synchronized TimeRange getSegment(final FDate key) {
        if (calculationBounds == null || calculationBounds.getFrom().isAfter(key)
                || calculationBounds.getTo().isBefore(key)) {
            calculationBounds = newCalculationBounds(key);
            curTimeRange = null;
        }

        if (curTimeRange == null) {
            //init first timerange
            curTimeRange = calculateNextTimeRange(calculationBounds.getFrom());
        }

        if (curTimeRange.getFrom().isAfter(key)) {
            do {
                //go back a few steps
                curTimeRange = calculatePrevTimeRange(curTimeRange.getFrom());
            } while (curTimeRange.getFrom().isAfter(key));
        } else if (curTimeRange.getTo().isBefore(key)) {
            do {
                //go forward a few steps
                curTimeRange = calculateNextTimeRange(curTimeRange.getFrom());
            } while (curTimeRange.getTo().isBefore(key));
        }

        if (calculationBounds.getTo().isBefore(curTimeRange.getTo())) {
            curTimeRange = new TimeRange(curTimeRange.getFrom(), calculationBounds.getTo());
        }

        return curTimeRange;
    }

    private TimeRange calculateNextTimeRange(final FDate curTimeRangeFrom) {
        final FDate nextTimeRangeStart = incrementFunction.apply(curTimeRangeFrom);
        final FDate nextTimeRangeEnd = incrementFunction.apply(nextTimeRangeStart).addMilliseconds(-1);
        return new TimeRange(nextTimeRangeStart, nextTimeRangeEnd);
    }

    private TimeRange calculatePrevTimeRange(final FDate curTimeRangeFrom) {
        final FDate prevTimeRangeStart = decrementFunction.apply(curTimeRangeFrom);
        final FDate prevTimeRangeEnd = curTimeRangeFrom.addMilliseconds(-1);
        return new TimeRange(prevTimeRangeStart, prevTimeRangeEnd);
    }

    private TimeRange newCalculationBounds(final FDate lookupTime) {
        FDate start = lookupTime.truncate(boundsTimeUnit);
        if (period.isExactMultipleOfPeriod(FTimeUnit.WEEKS.durationValue())) {
            //weekly periods should start at the previous monday (even if it is in the previous year)
            start = start.setFWeekday(FWeekday.Monday);
        }
        final FDate end = start.add(boundsTimeUnit, 1).addMilliseconds(-1);
        return new TimeRange(start, end);
    }

    public static PeriodicalSegmentFinder newInstance(final Duration period) {
        PeriodicalSegmentFinder instance = getImmediateInstance(period);
        if (instance == null) {
            instance = getQuickMultiplesInstance(period);
        }
        if (instance == null) {
            instance = getSlowFlexibleInstance(period);
        }
        return instance;
    }

    private static PeriodicalSegmentFinder getImmediateInstance(final Duration period) {
        //max 1 value
        if (period.longValue() == 1) {
            final FTimeUnit timeUnit = period.getTimeUnit();
            return new PeriodicalSegmentFinder(period, timeUnit, 1, timeUnit);
        } else {
            final FTimeUnit timeUnit = FTimeUnit.valueOf(period);
            if (timeUnit != null) {
                return new PeriodicalSegmentFinder(period, timeUnit, 1, timeUnit);
            } else {
                return null;
            }
        }
    }

    private static PeriodicalSegmentFinder getQuickMultiplesInstance(final Duration period) {
        if (period.isExactMultipleOfPeriod(FTimeUnit.CENTURIES.durationValue())) {
            final int multiples = (int) period.getNumMultipleOfPeriod(FTimeUnit.CENTURIES.durationValue());
            if (FTimeUnit.CENTURIES_IN_MILLENIUM % multiples == 0) {
                return new PeriodicalSegmentFinder(period, FTimeUnit.CENTURIES, multiples, FTimeUnit.MILLENIA);
            }
        } else if (period.isExactMultipleOfPeriod(FTimeUnit.DECADES.durationValue())) {
            final int multiples = (int) period.getNumMultipleOfPeriod(FTimeUnit.DECADES.durationValue());
            if (FTimeUnit.DECADES_IN_CENTURY % multiples == 0) {
                return new PeriodicalSegmentFinder(period, FTimeUnit.DECADES, multiples, FTimeUnit.CENTURIES);
            }
        } else if (period.isExactMultipleOfPeriod(FTimeUnit.YEARS.durationValue())) {
            final int multiples = (int) period.getNumMultipleOfPeriod(FTimeUnit.YEARS.durationValue());
            if (FTimeUnit.YEARS_IN_DECADE % multiples == 0) {
                return new PeriodicalSegmentFinder(period, FTimeUnit.YEARS, multiples, FTimeUnit.DECADES);
            }
        } else if (period.isExactMultipleOfPeriod(FTimeUnit.MONTHS.durationValue())) {
            final int multiples = (int) period.getNumMultipleOfPeriod(FTimeUnit.MONTHS.durationValue());
            if (FTimeUnit.MONTHS_IN_YEAR % multiples == 0) {
                return new PeriodicalSegmentFinder(period, FTimeUnit.MONTHS, multiples, FTimeUnit.YEARS);
            }
        } else if (period.isExactMultipleOfPeriod(FTimeUnit.WEEKS.durationValue())) {
            final int multiples = (int) period.getNumMultipleOfPeriod(FTimeUnit.WEEKS.durationValue());
            if (FTimeUnit.WEEKS_IN_YEAR % multiples == 0) {
                return new PeriodicalSegmentFinder(period, FTimeUnit.WEEKS, multiples, FTimeUnit.YEARS);
            }
        } else if (period.isExactMultipleOfPeriod(FTimeUnit.DAYS.durationValue())) {
            //non easy way out here
            return null;
        } else if (period.isExactMultipleOfPeriod(FTimeUnit.HOURS.durationValue())) {
            final int multiples = (int) period.getNumMultipleOfPeriod(FTimeUnit.HOURS.durationValue());
            if (FTimeUnit.HOURS_IN_DAY % multiples == 0) {
                return new PeriodicalSegmentFinder(period, FTimeUnit.HOURS, multiples, FTimeUnit.DAYS);
            }
        } else if (period.isExactMultipleOfPeriod(FTimeUnit.MINUTES.durationValue())) {
            final int multiples = (int) period.getNumMultipleOfPeriod(FTimeUnit.MINUTES.durationValue());
            if (FTimeUnit.MINUTES_IN_HOUR % multiples == 0) {
                return new PeriodicalSegmentFinder(period, FTimeUnit.MINUTES, multiples, FTimeUnit.HOURS);
            }
        } else if (period.isExactMultipleOfPeriod(FTimeUnit.SECONDS.durationValue())) {
            final int multiples = (int) period.getNumMultipleOfPeriod(FTimeUnit.SECONDS.durationValue());
            if (FTimeUnit.SECONDS_IN_MINUTE % multiples == 0) {
                return new PeriodicalSegmentFinder(period, FTimeUnit.SECONDS, multiples, FTimeUnit.MINUTES);
            }
        } else if (period.isExactMultipleOfPeriod(FTimeUnit.MILLISECONDS.durationValue())) {
            final int multiples = (int) period.getNumMultipleOfPeriod(FTimeUnit.MILLISECONDS.durationValue());
            if (FTimeUnit.MILLISECONDS_IN_SECOND % multiples == 0) {
                return new PeriodicalSegmentFinder(period, FTimeUnit.MILLISECONDS, multiples, FTimeUnit.SECONDS);
            }
        }
        return null;
    }

    /**
     * This is the fallback when no faster boundsTimeUnit can be determined, it works for any crooked periods
     */
    private static PeriodicalSegmentFinder getSlowFlexibleInstance(final Duration period) {
        if (period.isLessThanOrEqualTo(Duration.ONE_MILLISECOND)) {
            throw new IllegalArgumentException("Period must be positive: " + period);
        } else if (period.isLessThan(TEN_MILLISECONDS)) {
            //2ms -> 10ms-1ms => max 500 values in second
            return new PeriodicalSegmentFinder(period, FTimeUnit.SECONDS);
        } else if (period.isLessThan(Duration.ONE_SECOND)) {
            //10ms -> 1s-1ms => max 6000 values in minute
            return new PeriodicalSegmentFinder(period, FTimeUnit.MINUTES);
        } else if (period.isLessThan(Duration.ONE_MINUTE)) {
            //1s -> 1m-1ms => max 3600 values in hour
            return new PeriodicalSegmentFinder(period, FTimeUnit.HOURS);
        } else if (period.isLessThan(Duration.ONE_HOUR)) {
            //1m -> 1h-1ms => max 1440 values in day
            return new PeriodicalSegmentFinder(period, FTimeUnit.DAYS);
        } else if (period.isLessThan(Duration.ONE_DAY)) {
            //1h -> 1d-1ms => max 744 values in month
            return new PeriodicalSegmentFinder(period, FTimeUnit.MONTHS);
        } else if (period.isLessThan(Duration.ONE_MONTH)) {
            //1d -> 1month-1ms => max 365 values in year
            return new PeriodicalSegmentFinder(period, FTimeUnit.YEARS);
        } else if (period.isLessThan(Duration.ONE_YEAR)) {
            //1month -> 100y-1ms => max 1200 values in century
            return new PeriodicalSegmentFinder(period, FTimeUnit.CENTURIES);
        } else {
            //1year -> anything => max 1000 values in millenium
            return new PeriodicalSegmentFinder(period, FTimeUnit.MILLENIA);
        }
    }

    public static AHistoricalCache<TimeRange> newCache(final Duration period) {
        return new AHistoricalCache<TimeRange>() {

            private final PeriodicalSegmentFinder calculation = PeriodicalSegmentFinder.newInstance(period);

            @Override
            protected Integer getInitialMaximumSize() {
                return 10;
            }

            @Override
            protected FDate innerExtractKey(final TimeRange value) {
                return value.getFrom();
            }

            @Override
            protected IEvaluateGenericFDate<TimeRange> newLoadValue() {
                return (key) -> calculation.getSegment(key.asFDate());
            }

            @Override
            protected FDate innerCalculateNextKey(final FDate key) {
                return query().getValue(key).getTo().addMilliseconds(1);
            }

            @Override
            protected FDate innerCalculatePreviousKey(final FDate key) {
                return query().getValue(key).getFrom().addMilliseconds(-1);
            }

            @Override
            public void preloadData(final ExecutorService executor) {
                //noop
            }

        };
    }

}
