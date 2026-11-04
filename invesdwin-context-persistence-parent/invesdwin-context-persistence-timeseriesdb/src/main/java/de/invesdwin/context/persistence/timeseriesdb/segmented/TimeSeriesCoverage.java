package de.invesdwin.context.persistence.timeseriesdb.segmented;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.range.TimeRange;

/**
 * The declared coverage of a store for a queried time range: the sub-ranges that the segment status table declares
 * (COMPLETE with data, COMPLETE_EMPTY as covered-without-data, INITIALIZING as declared-but-not-yet-complete) plus the
 * undeclared gap sub-ranges that remain inside the query range.
 *
 * Derived from the status table's segment ranges only - no row scan, no count probe, no bisection: callers localize
 * holes from the returned gap ranges instead of probing for them. Adjacent ranges of the same bucket are merged into
 * maximal sub-ranges, and all returned sub-ranges are limited to the query range.
 */
@Immutable
public final class TimeSeriesCoverage {

    private final TimeRange queryRange;
    private final List<TimeRange> coveredRanges;
    private final List<TimeRange> coveredEmptyRanges;
    private final List<TimeRange> initializingRanges;
    private final List<TimeRange> gapRanges;

    private TimeSeriesCoverage(final TimeRange queryRange, final List<TimeRange> coveredRanges,
            final List<TimeRange> coveredEmptyRanges, final List<TimeRange> initializingRanges) {
        this.queryRange = queryRange;
        this.coveredRanges = mergeAdjacent(coveredRanges);
        this.coveredEmptyRanges = mergeAdjacent(coveredEmptyRanges);
        this.initializingRanges = mergeAdjacent(initializingRanges);
        this.gapRanges = Collections.unmodifiableList(extractGaps(queryRange, this.coveredRanges,
                this.coveredEmptyRanges, this.initializingRanges));
    }

    public TimeRange getQueryRange() {
        return queryRange;
    }

    /**
     * Sub-ranges the store declares to hold data (segment status COMPLETE).
     */
    public List<TimeRange> getCoveredRanges() {
        return coveredRanges;
    }

    /**
     * Sub-ranges the store declares as fully searched but empty (segment status COMPLETE_EMPTY): covered-no-data, not
     * a hole.
     */
    public List<TimeRange> getCoveredEmptyRanges() {
        return coveredEmptyRanges;
    }

    /**
     * Sub-ranges declared INITIALIZING: an update is or was in progress there, so they are not covered data yet and
     * also not undeclared holes.
     */
    public List<TimeRange> getInitializingRanges() {
        return initializingRanges;
    }

    /**
     * Sub-ranges inside the query range that the store declares nothing about: undeclared holes. An empty list together
     * with empty {@link #getInitializingRanges()} means the query range is fully declared.
     */
    public List<TimeRange> getGapRanges() {
        return gapRanges;
    }

    /**
     * True when the whole query range is declared as COMPLETE or COMPLETE_EMPTY - nothing is missing and nothing is
     * still being worked on.
     */
    public boolean isFullyCovered() {
        return gapRanges.isEmpty() && initializingRanges.isEmpty();
    }

    @Override
    public String toString() {
        return "TimeSeriesCoverage[" + queryRange + ", covered=" + coveredRanges + ", coveredEmpty="
                + coveredEmptyRanges + ", initializing=" + initializingRanges + ", gaps=" + gapRanges + "]";
    }

    public static TimeSeriesCoverage valueOf(final FDate from, final FDate to,
            final Iterable<Entry<TimeRange, SegmentStatus>> declaredRanges) {
        return valueOf(from, to, declaredRanges, null);
    }

    /**
     * @param from
     *            query range start, null means unlimited
     * @param to
     *            query range end, null means unlimited
     * @param declaredRanges
     *            the segment status table rows (a null status counts as undeclared, e.g. a row deleted between listing
     *            and reading it)
     * @param extraCoveredRanges
     *            ranges to add to the covered bucket although they carry no status row yet - e.g. the live segment of a
     *            live store, which holds data before it is status-declared COMPLETE; may be null
     */
    public static TimeSeriesCoverage valueOf(final FDate from, final FDate to,
            final Iterable<Entry<TimeRange, SegmentStatus>> declaredRanges,
            final Iterable<TimeRange> extraCoveredRanges) {
        final FDate queryFrom = from != null ? from : FDates.MIN_DATE;
        final FDate queryTo = to != null ? to : FDates.MAX_DATE;
        final TimeRange queryRange = new TimeRange(queryFrom, queryTo);
        final List<TimeRange> covered = new ArrayList<TimeRange>();
        final List<TimeRange> coveredEmpty = new ArrayList<TimeRange>();
        final List<TimeRange> initializing = new ArrayList<TimeRange>();
        //an inverted query range intersects nothing and leaves nothing uncovered: an empty coverage
        final boolean invertedQueryRange = queryFrom.isAfterNotNullSafe(queryTo);
        if (!invertedQueryRange) {
            if (declaredRanges != null) {
                for (final Entry<TimeRange, SegmentStatus> row : declaredRanges) {
                    final TimeRange declaredRange = row.getKey();
                    final SegmentStatus status = row.getValue();
                    final TimeRange window = intersect(declaredRange, queryRange);
                    if (window == null) {
                        continue;
                    }
                    if (status == null) {
                        //status gone between listing and reading: undeclared
                        continue;
                    }
                    switch (status) {
                    case COMPLETE:
                        covered.add(window);
                        break;
                    case COMPLETE_EMPTY:
                        coveredEmpty.add(window);
                        break;
                    case INITIALIZING:
                        initializing.add(window);
                        break;
                    default:
                        throw UnknownArgumentException.newInstance(SegmentStatus.class, status);
                    }
                }
            }
            if (extraCoveredRanges != null) {
                for (final TimeRange extraRange : extraCoveredRanges) {
                    final TimeRange window = intersect(extraRange, queryRange);
                    if (window != null) {
                        covered.add(window);
                    }
                }
            }
        }
        return new TimeSeriesCoverage(queryRange, covered, coveredEmpty, initializing);
    }

    private static TimeRange intersect(final TimeRange range, final TimeRange queryRange) {
        if (range == null || range.getFrom() == null || range.getTo() == null) {
            return null;
        }
        if (range.getTo().isBeforeNotNullSafe(queryRange.getFrom())
                || range.getFrom().isAfterNotNullSafe(queryRange.getTo())) {
            return null;
        }
        return range.limit(queryRange);
    }

    private static List<TimeRange> mergeAdjacent(final List<TimeRange> ranges) {
        final List<TimeRange> sorted = new ArrayList<TimeRange>(ranges);
        Collections.sort(sorted, TimeRange.COMPARATOR);
        final List<TimeRange> merged = new ArrayList<TimeRange>();
        for (final TimeRange range : sorted) {
            if (merged.isEmpty()) {
                merged.add(range);
            } else {
                final TimeRange last = merged.get(merged.size() - 1);
                if (range.getFrom().isAfterNotNullSafe(last.getTo().addPicoseconds(1))) {
                    merged.add(range);
                } else if (range.getTo().isAfterNotNullSafe(last.getTo())) {
                    merged.set(merged.size() - 1, new TimeRange(last.getFrom(), range.getTo()));
                }
            }
        }
        return Collections.unmodifiableList(merged);
    }

    /**
     * The query range minus all declared sub-ranges (covered, covered-empty and initializing alike), as maximal
     * contiguous sub-ranges.
     */
    private static List<TimeRange> extractGaps(final TimeRange queryRange, final List<TimeRange> coveredRanges,
            final List<TimeRange> coveredEmptyRanges, final List<TimeRange> initializingRanges) {
        final List<TimeRange> declared = new ArrayList<TimeRange>();
        declared.addAll(coveredRanges);
        declared.addAll(coveredEmptyRanges);
        declared.addAll(initializingRanges);
        Collections.sort(declared, TimeRange.COMPARATOR);
        final List<TimeRange> gaps = new ArrayList<TimeRange>();
        final FDate queryTo = queryRange.getTo();
        FDate cursor = queryRange.getFrom();
        for (final TimeRange declaredRange : declared) {
            if (cursor.isAfterNotNullSafe(queryTo)) {
                break;
            }
            if (declaredRange.getTo().isBeforeNotNullSafe(cursor)) {
                continue;
            }
            if (declaredRange.getFrom().isAfterNotNullSafe(cursor)) {
                gaps.add(new TimeRange(cursor, declaredRange.getFrom().addPicoseconds(-1)));
            }
            if (declaredRange.getTo().isAfterOrEqualToNotNullSafe(cursor)) {
                cursor = declaredRange.getTo().addPicoseconds(1);
            }
        }
        if (!cursor.isAfterNotNullSafe(queryTo)) {
            gaps.add(new TimeRange(cursor, queryTo));
        }
        return gaps;
    }

}
