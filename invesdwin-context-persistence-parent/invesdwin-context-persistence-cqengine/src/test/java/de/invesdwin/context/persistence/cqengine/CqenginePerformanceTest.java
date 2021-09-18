package de.invesdwin.context.persistence.cqengine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.codegen.AttributeSourceGenerator;
import com.googlecode.cqengine.index.navigable.NavigableIndex;
import com.googlecode.cqengine.index.support.CloseableIterable;
import com.googlecode.cqengine.index.support.CloseableIterator;
import com.googlecode.cqengine.index.support.KeyValue;
import com.googlecode.cqengine.index.unique.UniqueIndex;
import com.googlecode.cqengine.persistence.onheap.OnHeapPersistence;
import com.googlecode.cqengine.query.QueryFactory;
import com.googlecode.cqengine.query.option.EngineThresholds;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.resultset.ResultSet;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
//@Ignore("manual test")
public class CqenginePerformanceTest extends ADatabasePerformanceTest {

    @Test
    public void testGenerateAttributes() {
        final String str = AttributeSourceGenerator.generateAttributesForPastingIntoTargetClass(Content.class);
        //CHECKSTYLE:OFF
        System.out.println(str);
        //CHECKSTYLE:ON
    }

    @Test
    public void testCqenginePerformance() throws InterruptedException, IOException {

        final File folder = new File(ContextProperties.getCacheDirectory(),
                CqenginePerformanceTest.class.getSimpleName());
        final File file = new File(folder, "table.db");
        Files.deleteNative(folder);
        Files.forceMkdir(folder);

        //        final DiskPersistence<Content, Long> persistence = DiskPersistence.onPrimaryKeyInFile(Content.KEY, file);
        //        final OffHeapPersistence<Content, Long> persistence = OffHeapPersistence.onPrimaryKey(Content.KEY);
        final OnHeapPersistence<Content, Long> persistence = OnHeapPersistence.onPrimaryKey(Content.KEY);
        final IndexedCollection<Content> table = new ConcurrentIndexedCollection<Content>(persistence);
        final NavigableIndex<Long, Content> navigableIndex = NavigableIndex.onAttribute(Content.KEY);
        //        NavigableIndex<Long, Content> navigableIndex = NavigableIndex.withQuantizerOnAttribute(LongQuantizer.withCompressionFactor(5), KeyValue.KEY);
        table.addIndex(navigableIndex);
        table.addIndex(UniqueIndex.onAttribute(Content.KEY));

        final Instant writesStart = new Instant();
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck();
        int i = 0;
        final List<Content> flushable = new ArrayList<>();
        for (final FDate date : newValues()) {
            flushable.add(new Content(date.millisValue(), date));
            i++;
            if (i % FLUSH_INTERVAL == 0) {
                table.addAll(flushable);
                flushable.clear();
                if (loopCheck.check()) {
                    printProgress("Writes", writesStart, i, VALUES);
                }
            }
        }
        if (!flushable.isEmpty()) {
            table.addAll(flushable);
        }
        //        persistence.compact();
        printProgress("WritesFinished", writesStart, VALUES, VALUES);

        readIterator(table);
        readOrdered(table);
        readOrderedIndex(navigableIndex);
        readGet(table);
        readGetLatest(table);
        readGetLatestIndex(navigableIndex);

        table.clear();
    }

    private void readIterator(final IndexedCollection<Content> table) throws InterruptedException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            int count = 0;
            final Iterator<Content> iterator = table.iterator();
            try {
                while (true) {
                    final FDate value = iterator.next().getValue();
                    //                    if (prevValue != null) {
                    //                        Assertions.checkTrue(prevValue.isBefore(value));
                    //                    }
                    prevValue = value;
                    count++;
                }
            } catch (final NoSuchElementException e) {
                //end reached
            }
            Assertions.checkEquals(count, VALUES);
            if (loopCheck.check()) {
                printProgress("Reads", readsStart, VALUES * reads, VALUES * READS);
            }
        }
        printProgress("ReadsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void readOrdered(final IndexedCollection<Content> table) throws InterruptedException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final Instant readsStart = new Instant();
        for (int reads = 1; reads <= READS; reads++) {
            FDate prevValue = null;
            int count = 0;
            try (ResultSet<Content> result = table.retrieve(QueryFactory.all(Content.class),
                    QueryFactory.queryOptions(QueryFactory.orderBy(QueryFactory.ascending(Content.KEY))))) {
                final Iterator<Content> iterator = result.iterator();
                try {
                    while (true) {
                        final FDate value = iterator.next().getValue();
                        if (prevValue != null) {
                            Assertions.checkTrue(prevValue.isBefore(value));
                        }
                        prevValue = value;
                        count++;
                    }
                } catch (final NoSuchElementException e) {
                    //end reached
                }
            }
            Assertions.checkEquals(count, VALUES);
            if (loopCheck.check()) {
                printProgress("ReadsOrdered", readsStart, VALUES * reads, VALUES * READS);
            }
        }
        printProgress("ReadsOrderedFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void readOrderedIndex(final NavigableIndex<Long, Content> index) throws InterruptedException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final Instant readsStart = new Instant();
        for (int reads = 0; reads < READS; reads++) {
            FDate prevValue = null;
            int count = 0;
            final CloseableIterable<KeyValue<Long, Content>> result = index
                    .getKeysAndValues(QueryFactory.noQueryOptions());
            try (CloseableIterator<KeyValue<Long, Content>> it = result.iterator()) {
                try {
                    while (it.hasNext()) {
                        final FDate value = it.next().getValue().getValue();
                        if (prevValue != null) {
                            Assertions.checkTrue(prevValue.isBefore(value));
                        }
                        prevValue = value;
                        count++;
                    }
                } catch (final NoSuchElementException e) {
                    //end reached
                }
            }
            Assertions.checkEquals(count, VALUES);
            if (loopCheck.check()) {
                printProgress("ReadOrderedIndexs", readsStart, VALUES * reads, VALUES * READS);
            }
        }
        printProgress("ReadOrderedIndexsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void readGet(final IndexedCollection<Content> table) throws InterruptedException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (int reads = 0; reads < READS; reads++) {
            FDate prevValue = null;
            int count = 0;
            for (int i = 0; i < values.size(); i++) {
                try (ResultSet<Content> result = table
                        .retrieve(QueryFactory.equal(Content.KEY, values.get(i).millisValue()))) {
                    final FDate value = result.uniqueResult().getValue();
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(value));
                    }
                    prevValue = value;
                    count++;
                }
            }
            Assertions.checkEquals(count, VALUES);
            if (loopCheck.check()) {
                printProgress("Gets", readsStart, VALUES * reads, VALUES * READS);
            }
        }
        printProgress("GetsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void readGetLatest(final IndexedCollection<Content> table) throws InterruptedException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (int reads = 0; reads < READS; reads++) {
            FDate prevValue = null;
            int count = 0;
            for (int i = 0; i < values.size(); i++) {
                try (ResultSet<Content> result = table.retrieve(
                        QueryFactory.lessThanOrEqualTo(Content.KEY, values.get(i).millisValue()),
                        QueryFactory.queryOptions(QueryFactory.orderBy(QueryFactory.descending(Content.KEY)),
                                QueryFactory.applyThresholds(
                                        QueryFactory.threshold(EngineThresholds.INDEX_ORDERING_SELECTIVITY, 1.0))))) {
                    final Iterator<Content> iterator = result.iterator();
                    final FDate value = iterator.next().getValue();
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(value));
                    }
                    prevValue = value;
                    count++;
                }
            }
            Assertions.checkEquals(count, VALUES);
            if (loopCheck.check()) {
                printProgress("GetLatests", readsStart, VALUES * reads, VALUES * READS);
            }
        }
        printProgress("GetLatestsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    private void readGetLatestIndex(final NavigableIndex<Long, Content> index) throws InterruptedException {
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
        final List<FDate> values = Lists.toList(newValues());
        final Instant readsStart = new Instant();
        for (int reads = 0; reads < READS; reads++) {
            FDate prevValue = null;
            int count = 0;
            for (int i = 0; i < values.size(); i++) {
                final CloseableIterable<KeyValue<Long, Content>> result = index.getKeysAndValuesDescending(
                        Long.MIN_VALUE, true, values.get(i).millisValue(), true, QueryFactory.noQueryOptions());
                try (CloseableIterator<KeyValue<Long, Content>> it = result.iterator()) {
                    final FDate value = it.next().getValue().getValue();
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(value));
                    }
                    prevValue = value;
                    count++;
                }
            }
            Assertions.checkEquals(count, VALUES);
            if (loopCheck.check()) {
                printProgress("GetLatestIndexs", readsStart, VALUES * reads, VALUES * READS);
            }
        }
        printProgress("GetLatestIndexsFinished", readsStart, VALUES * READS, VALUES * READS);
    }

    public static final class Content {

        /**
         * CQEngine attribute for accessing field {@code KeyValue.key}.
         */
        // Note: For best performance:
        // - if this field cannot be null, replace this SimpleNullableAttribute with a SimpleAttribute
        public static final SimpleAttribute<Content, Long> KEY = new SimpleAttribute<Content, Long>("KEY") {
            @Override
            public Long getValue(final Content keyvalue, final QueryOptions queryOptions) {
                return keyvalue.key;
            }
        };

        /**
         * CQEngine attribute for accessing field {@code KeyValue.value}.
         */
        // Note: For best performance:
        // - if this field cannot be null, replace this SimpleNullableAttribute with a SimpleAttribute
        public static final SimpleAttribute<Content, FDate> VALUE = new SimpleAttribute<Content, FDate>("VALUE") {
            @Override
            public FDate getValue(final Content keyvalue, final QueryOptions queryOptions) {
                return keyvalue.value;
            }
        };

        private final Long key;
        private final FDate value;

        public Content(final Long key, final FDate value) {
            this.key = key;
            this.value = value;
        }

        public Long getKey() {
            return key;
        }

        public FDate getValue() {
            return value;
        }
    }
}
