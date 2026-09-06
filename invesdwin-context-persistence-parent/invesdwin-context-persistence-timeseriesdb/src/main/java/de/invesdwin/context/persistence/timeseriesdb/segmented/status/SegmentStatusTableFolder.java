package de.invesdwin.context.persistence.timeseriesdb.segmented.status;

import java.io.File;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.filechannel.info.path.FileChannelPath;
import de.invesdwin.context.integration.filechannel.nio.NioFileInfo;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentStatus;
import de.invesdwin.util.bean.tuple.ImmutableEntry;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.range.TimeRange;

@ThreadSafe
public class SegmentStatusTableFolder {

    private static final String DATE_FORMAT = FDate.FORMAT_NUMBER_DATE_TIME_PS;
    private static final String STATUS_EXTENSION = ".status";

    private final AtomicNioFileChannel baseChannel;

    // Naturally sorted caches for high-performance iteration
    private final NavigableMap<TimeRange, SegmentStatus> terminalStatusCache = new ConcurrentSkipListMap<>(
            TimeRange.COMPARATOR);
    private final NavigableSet<TimeRange> knownRanges = new ConcurrentSkipListSet<>(TimeRange.COMPARATOR);
    private final Set<TimeRange> currentDiskRanges = ILockCollectionFactory.getInstance(false).newSet();

    private volatile FDate lastDirectoryScan = null;

    public SegmentStatusTableFolder(final File directory) {
        //CHECKSTYLE:OFF
        this(new AtomicNioFileChannel(
                FileChannelPath.valueOfDirectory(directory.toURI(), AtomicNioFileChannel.DEFAULT_SERVER_URI_F)));
        //CHECKSTYLE:ON
    }

    public SegmentStatusTableFolder(final AtomicNioFileChannel baseChannel) {
        this.baseChannel = baseChannel;
    }

    public SegmentStatus get(final TimeRange timeRange) {
        // 1. Fast path: terminal status already cached
        final SegmentStatus cached = terminalStatusCache.get(timeRange);
        if (cached != null) {
            return cached;
        }

        final AtomicNioFileChannel fileChannel = getChannelForRange(timeRange);

        if (!fileChannel.exists()) {
            return null; // Signals updater to initialize
        }

        try {
            final String content = fileChannel.downloadString();
            if (content == null || Strings.isBlank(content)) {
                return null;
            }
            final SegmentStatus status = SegmentStatus.valueOf(content);
            if (status.isComplete()) {
                terminalStatusCache.put(timeRange, status);
            }
            return status;
        } catch (final Exception e) {
            return null; // Corrupt/unreadable state triggers the updater
        }
    }

    public void put(final TimeRange timeRange, final SegmentStatus status) {
        final AtomicNioFileChannel fileChannel = getChannelForRange(timeRange);
        final String name = status.name();

        // Performs a temp file creation and atomic rename.
        // Crucially, this atomic move updates the base directory's lastModified timestamp!
        fileChannel.uploadString(name);

        knownRanges.add(timeRange);

        if (status.isComplete()) {
            terminalStatusCache.put(timeRange, status);
        }
    }

    private void syncCacheWithDirectory() {
        final FDate currentModTime = baseChannel.lastModified();

        // If the directory hasn't been modified since our last scan, we can safely skip the heavy filesystem list operation
        if (lastDirectoryScan != null && currentModTime != null && currentModTime.equals(lastDirectoryScan)) {
            return;
        }

        synchronized (this) {
            if (lastDirectoryScan != null && currentModTime != null && currentModTime.equals(lastDirectoryScan)) {
                return;
            }
            if (!currentDiskRanges.isEmpty()) {
                currentDiskRanges.clear();
            }

            try (ICloseableIterator<NioFileInfo> iterator = baseChannel.listIterator()) {
                while (iterator.hasNext()) {
                    final String fileName = iterator.next().getFilename();
                    if (fileName != null && fileName.endsWith(STATUS_EXTENSION)) {
                        final TimeRange timeRange = parseRangeFromFileName(fileName);
                        if (timeRange != null) {
                            currentDiskRanges.add(timeRange);
                            knownRanges.add(timeRange);
                        }
                    }
                }
            }

            // Evict any ranges that were deleted directly from disk by other processes
            knownRanges.retainAll(currentDiskRanges);
            lastDirectoryScan = currentModTime;
        }
    }

    public ICloseableIterator<Entry<TimeRange, SegmentStatus>> range() {
        return range(null, null);
    }

    public ICloseableIterator<Entry<TimeRange, SegmentStatus>> range(final FDate from, final FDate to) {
        // 1. Ensure our list of known TimeRanges is up-to-date (almost instantly skips if unmodified)
        syncCacheWithDirectory();

        NavigableSet<TimeRange> searchSpace = knownRanges;

        // 2. O(log N) Upper Bound Optimization:
        // We can safely drop anything that starts strictly after 'to'.
        if (to != null) {
            searchSpace = searchSpace.headSet(new TimeRange(to, to), true);
        }

        // Note: We CANNOT use tailSet(from) because a valid segment might have a
        // getFrom() < from but a getTo() >= from.

        // 3. Iterate directly over the naturally sorted memory set
        final java.util.Iterator<TimeRange> iterator = searchSpace.iterator();

        return new ICloseableIterator<Entry<TimeRange, SegmentStatus>>() {
            private Entry<TimeRange, SegmentStatus> nextElement = null;

            private void advance() {
                while (nextElement == null && iterator.hasNext()) {
                    final TimeRange timeRange = iterator.next();

                    // Apply Date bounds filtering only for the lower bound.
                    // The upper bound is now natively handled by the headSet view.
                    if (from != null && timeRange.getTo().compareTo(from) < 0) {
                        continue;
                    }

                    // Calling get() uses the cache for final states and only reads disk for INITIALIZING
                    final SegmentStatus status = get(timeRange);
                    if (status != null) {
                        nextElement = ImmutableEntry.of(timeRange, status);
                    }
                }
            }

            @Override
            public boolean hasNext() {
                if (nextElement == null) {
                    advance();
                }
                return nextElement != null;
            }

            @Override
            public Entry<TimeRange, SegmentStatus> next() {
                if (!hasNext()) {
                    throw FastNoSuchElementException.getInstance("SegmentStatusTableFolder.range.next end reached");
                }
                final Entry<TimeRange, SegmentStatus> result = nextElement;
                nextElement = null;
                return result;
            }

            @Override
            public void close() {
                // In-memory cache iterator doesn't require closing
            }
        };
    }

    public void delete(final TimeRange segment) {
        if (segment == null) {
            return;
        }
        terminalStatusCache.remove(segment);
        knownRanges.remove(segment);
        getChannelForRange(segment).delete();
    }

    private AtomicNioFileChannel getChannelForRange(final TimeRange timeRange) {
        final String fileName = timeRange.getFrom().toString(DATE_FORMAT) + "_"
                + timeRange.getTo().toString(DATE_FORMAT) + STATUS_EXTENSION;
        return baseChannel.withFilename(fileName);
    }

    private TimeRange parseRangeFromFileName(final String fileName) {
        try {
            final String nameWithoutExt = fileName.substring(0, fileName.length() - STATUS_EXTENSION.length());
            final String[] parts = nameWithoutExt.split("_", 2);
            final FDate fromMillis = FDate.valueOf(parts[0], DATE_FORMAT);
            final FDate toMillis = FDate.valueOf(parts[1], DATE_FORMAT);
            return new TimeRange(fromMillis, toMillis);
        } catch (final Exception e) {
            return null;
        }
    }

    public void deleteRange() {
        try (ICloseableIterator<NioFileInfo> iterator = baseChannel.listIterator()) {
            while (true) {
                final NioFileInfo info = iterator.next();
                final String fileName = info.getFilename();
                if (fileName != null && fileName.endsWith(STATUS_EXTENSION)) {
                    baseChannel.withFilename(fileName).delete();
                }
            }
        } catch (final NoSuchElementException e) {
            // End of iterator reached, nothing to do
        }
        terminalStatusCache.clear();
        knownRanges.clear();
        lastDirectoryScan = null;
    }

    public void deleteRange(final FDate from, final FDate to) {
        // Fast path for clearing everything: clear caches and delete all status files directly
        if (from == null && to == null) {
            deleteRange();
            return;
        }

        // Bounded range deletion
        syncCacheWithDirectory();

        if (knownRanges.isEmpty()) {
            return;
        }

        NavigableSet<TimeRange> targetRange = knownRanges;

        if (to != null) {
            targetRange = targetRange.headSet(new TimeRange(to, to), true);
        }

        final java.util.List<TimeRange> toDelete = new java.util.ArrayList<>();
        for (final TimeRange range : targetRange) {
            if (from != null && range.getTo().compareTo(from) < 0) {
                continue;
            }
            if (to != null && range.getFrom().compareTo(to) > 0) {
                break;
            }
            toDelete.add(range);
        }

        for (final TimeRange segment : toDelete) {
            terminalStatusCache.remove(segment);
            knownRanges.remove(segment);
            getChannelForRange(segment).delete();
        }
    }

    public Entry<TimeRange, SegmentStatus> getLatest() {
        return getLatest(null);
    }

    public Entry<TimeRange, SegmentStatus> getLatest(final TimeRange timeRange) {
        // 1. Ensure our list of known TimeRanges is up-to-date
        syncCacheWithDirectory();

        NavigableSet<TimeRange> searchSpace = knownRanges;

        // 2. O(log N) jump: Because COMPARATOR only evaluates getFrom(),
        // this perfectly bounds the set to ranges that start on or before 'to'.
        if (timeRange != null) {
            searchSpace = searchSpace.headSet(timeRange, true);
        }

        // 3. Iterate backwards from our optimized subset
        for (final TimeRange range : searchSpace.descendingSet()) {

            // 4. O(1) exit: Since we are iterating newest-to-oldest, once we hit
            // a range that ends before our 'from' date, all older segments will
            // also fall out of bounds. We can instantly stop.
            if (timeRange != null && timeRange.getFrom() != null && range.getTo().compareTo(timeRange.getFrom()) < 0) {
                break;
            }

            // Verify file state and resolve cache
            final SegmentStatus status = get(range);
            if (status != null) {
                return ImmutableEntry.of(range, status);
            }
        }

        return null;
    }
}