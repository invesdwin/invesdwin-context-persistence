package de.invesdwin.context.persistence.timeseriesdb.base.root;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.collections.loadingcache.historical.AGapHistoricalCache;
import de.invesdwin.util.collections.loadingcache.historical.key.APullingHistoricalCacheAdjustKeyProvider;
import de.invesdwin.util.collections.loadingcache.historical.key.APushingHistoricalCacheAdjustKeyProvider;
import de.invesdwin.util.collections.loadingcache.historical.key.IHistoricalCacheAdjustKeyProvider;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

@NotThreadSafe
public abstract class ARootDBTest extends ATest {

    protected static final String KEY = "asdf";

    protected final List<FDate> entities;

    protected int countReadAllValuesAscendingFrom;
    protected int countReadNewestValueTo;
    protected int countInnerExtractKey;
    protected int countAdjustKey;
    protected boolean returnNullInReadNewestValueTo;
    protected boolean returnAllInReadAllValuesAscendingFrom;
    protected Integer returnMaxResults;
    protected final int testReturnMaxResultsValue = 2;
    protected final ATestGapHistoricalCache cache = newTestGapHistoricalCache();

    protected ITimeSeriesDB<String, FDate> table;

    public ARootDBTest() {
        this.entities = new ArrayList<FDate>();
        entities.add(FDateBuilder.newDate(1990, 1, 1));
        entities.add(FDateBuilder.newDate(1991, 1, 1));
        entities.add(FDateBuilder.newDate(1992, 1, 1));
        entities.add(FDateBuilder.newDate(1993, 1, 1));
        entities.add(FDateBuilder.newDate(1994, 1, 1));
        entities.add(FDateBuilder.newDate(1995, 1, 1));
    }

    protected abstract ATestGapHistoricalCache newTestGapHistoricalCache();

    protected abstract void putNewEntity(FDate newEntity) throws IncompleteUpdateRetryableException;

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        table.deleteRange(KEY);
        table.close();
    }

    protected <T> List<T> asList(final Iterable<T> iterable) {
        return Lists.toListWithoutHasNext(iterable);
    }

    @Test
    public void testRandomizedPreviousValues() {
        final List<Pair<Integer, Integer>> reproduce = new ArrayList<Pair<Integer, Integer>>();
        try {
            for (int i = 0; i < 100000; i++) {
                final int keyIndex = RandomUtils.nextInt(0, entities.size());
                final int shiftBackUnits = RandomUtils.nextInt(1, Math.max(1, keyIndex));
                reproduce.add(Pair.of(keyIndex, shiftBackUnits));
                final FDate key = entities.get(keyIndex);
                final Collection<FDate> previousValues = asList(cache.query().getPreviousValues(key, shiftBackUnits));
                final List<FDate> expectedValues = entities.subList(keyIndex - shiftBackUnits + 1, keyIndex + 1);
                Assertions.assertThat(previousValues).isEqualTo(expectedValues);
                if (i % 100 == 0) {
                    cache.clear();
                    reproduce.clear();
                }
            }
        } catch (final Throwable t) {
            reproduce(reproduce, t);
            throw t;
        }
    }

    protected void reproduce(final List<Pair<Integer, Integer>> reproduce, final Throwable t) {
        //CHECKSTYLE:OFF
        System.out.println(reproduce.size() + ". step: " + t.toString());
        //CHECKSTYLE:ON
        cache.clear();
        for (int step = 1; step <= reproduce.size(); step++) {
            final Pair<Integer, Integer> keyIndex_shiftBackUnits = reproduce.get(step - 1);
            final int keyIndex = keyIndex_shiftBackUnits.getFirst();
            final int shiftBackUnits = keyIndex_shiftBackUnits.getSecond();
            final FDate key = entities.get(keyIndex);
            final List<FDate> expectedValues = entities.subList(keyIndex - shiftBackUnits + 1, keyIndex + 1);
            if (step == reproduce.size()) {
                //CHECKSTYLE:OFF
                System.out.println("now");
                //CHECKSTYLE:ON
            }
            final Collection<FDate> previousValues = asList(cache.query().getPreviousValues(key, shiftBackUnits));
            Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        }
    }

    @Test
    public void testGetPreviousAndNextWithTable() {
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = table.getPreviousValue(KEY, entities.get(entities.size() - 1), i);
            final FDate expectedValue = entities.get(entities.size() - i - 1);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = table.getPreviousValue(KEY, FDates.MAX_DATE, i);
            final FDate expectedValue = entities.get(entities.size() - i - 1);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = table.getPreviousValue(KEY, FDates.MIN_DATE, i);
            final FDate expectedValue = entities.get(0);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }

        for (int i = 0; i < entities.size(); i++) {
            final FDate value = table.getNextValue(KEY, entities.get(0), i);
            final FDate expectedValue = entities.get(i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = table.getNextValue(KEY, FDates.MIN_DATE, i);
            final FDate expectedValue = entities.get(i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = table.getNextValue(KEY, FDates.MAX_DATE, i);
            final FDate expectedValue = entities.get(entities.size() - 1);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
    }

    @Test
    public void testNoData() {
        final List<FDate> liste = new ArrayList<FDate>(entities);
        table.deleteRange(KEY);
        entities.clear();
        for (final FDate entity : liste) {
            final FDate cachedEntity = cache.query().getValue(entity);
            Assertions.assertThat(cachedEntity).isNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(6);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(18);

        //new minKey limit gets tested
        final Collection<FDate> values = asList(cache.query().getPreviousValues(FDates.MIN_DATE, 5));
        Assertions.assertThat(values).isEmpty();
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(7);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(21);
    }

    @Test
    public void testNewEntityIncomingPullingAdjustKeyProvider() throws IncompleteUpdateRetryableException {
        cache.setAdjustKeyProvider(new APullingHistoricalCacheAdjustKeyProvider(cache) {
            @Override
            protected FDate innerGetHighestAllowedKey() {
                return entities.get(entities.size() - 1);
            }

            @Override
            protected boolean isPullingRecursive() {
                return false;
            }
        });
        final List<FDate> newEntities = new ArrayList<FDate>(entities);
        final FDate newEntity = FDateBuilder.newDate(1996, 1, 1);
        newEntities.add(newEntity);
        for (final FDate entity : newEntities) {
            final FDate value = cache.query().getValue(entity);
            if (newEntity.equals(entity)) {
                Assertions.assertThat(value).isNotEqualTo(newEntity);
                Assertions.assertThat(value).isEqualTo(entities.get(entities.size() - 1));
            } else {
                Assertions.assertThat(value).isEqualTo(entity);
            }
        }
        entities.add(newEntity);
        putNewEntity(newEntity);
        final FDate correctValue = cache.query().getValue(newEntity);
        Assertions.assertThat(correctValue).isEqualTo(newEntity);
    }

    @Test
    public void testNewEntityIncomingPushingAdjustKeyProvider() throws IncompleteUpdateRetryableException {
        final APushingHistoricalCacheAdjustKeyProvider adjustKeyProvider = new APushingHistoricalCacheAdjustKeyProvider(
                cache) {
            @Override
            protected FDate getInitialHighestAllowedKey() {
                return null;
            }

            @Override
            protected boolean isPullingRecursive() {
                return false;
            }
        };
        cache.setAdjustKeyProvider(adjustKeyProvider);
        adjustKeyProvider.pushHighestAllowedKey(entities.get(entities.size() - 1));
        final List<FDate> newEntities = new ArrayList<FDate>(entities);
        final FDate newEntity = FDateBuilder.newDate(1996, 1, 1);
        newEntities.add(newEntity);
        for (final FDate entity : newEntities) {
            final FDate value = cache.query().getValue(entity);
            if (newEntity.equals(entity)) {
                Assertions.assertThat(value).isNotEqualTo(newEntity);
                Assertions.assertThat(value).isEqualTo(entities.get(entities.size() - 1));
            } else {
                Assertions.assertThat(value).isEqualTo(entity);
            }
        }
        adjustKeyProvider.pushHighestAllowedKey(newEntity);
        entities.add(newEntity);
        putNewEntity(newEntity);
        final FDate correctValue = cache.query().getValue(newEntity);
        Assertions.assertThat(correctValue).isEqualTo(newEntity);
    }

    @Test
    public void testNewEntityIncomingPushingAdjustKeyProviderWithoutInitialPush()
            throws IncompleteUpdateRetryableException {
        final APushingHistoricalCacheAdjustKeyProvider adjustKeyProvider = new APushingHistoricalCacheAdjustKeyProvider(
                cache) {
            @Override
            protected FDate getInitialHighestAllowedKey() {
                return null;
            }

            @Override
            protected boolean isPullingRecursive() {
                return false;
            }
        };
        cache.setAdjustKeyProvider(adjustKeyProvider);
        final List<FDate> newEntities = new ArrayList<FDate>(entities);
        final FDate newEntity = FDateBuilder.newDate(1996, 1, 1);
        newEntities.add(newEntity);
        for (final FDate entity : newEntities) {
            final FDate value = cache.query().getValue(entity);
            if (newEntity.equals(entity)) {
                Assertions.assertThat(value).isNotEqualTo(newEntity);
                Assertions.assertThat(value).isEqualTo(entities.get(entities.size() - 1));
            } else {
                Assertions.assertThat(value).isEqualTo(entity);
            }
        }
        adjustKeyProvider.pushHighestAllowedKey(newEntity);
        entities.add(newEntity);
        putNewEntity(newEntity);
        final FDate correctValue = cache.query().getValue(newEntity);
        Assertions.assertThat(correctValue).isEqualTo(newEntity);
    }

    @Test
    public void testNewEntityIncomingPushingAdjustKeyProviderWithoutPush() throws IncompleteUpdateRetryableException {
        final APushingHistoricalCacheAdjustKeyProvider adjustKeyProvider = new APushingHistoricalCacheAdjustKeyProvider(
                cache) {
            @Override
            protected FDate getInitialHighestAllowedKey() {
                return entities.get(entities.size() - 1);
            }

            @Override
            protected boolean isPullingRecursive() {
                return false;
            }
        };
        cache.setAdjustKeyProvider(adjustKeyProvider);
        final List<FDate> newEntities = new ArrayList<FDate>(entities);
        final FDate newEntity = FDateBuilder.newDate(1996, 1, 1);
        newEntities.add(newEntity);
        for (final FDate entity : newEntities) {
            final FDate value = cache.query().getValue(entity);
            if (newEntity.equals(entity)) {
                Assertions.assertThat(value).isNotEqualTo(newEntity);
                Assertions.assertThat(value).isEqualTo(entities.get(entities.size() - 1));
            } else {
                Assertions.assertThat(value).isEqualTo(entity);
            }
        }
        adjustKeyProvider.pushHighestAllowedKey(newEntity);
        entities.add(newEntity);
        putNewEntity(newEntity);
        final FDate correctValue = cache.query().getValue(newEntity);
        Assertions.assertThat(correctValue).isEqualTo(newEntity);
    }

    public abstract class ATestGapHistoricalCache extends AGapHistoricalCache<FDate> {

        @Override
        protected FDate adjustKey(final FDate key) {
            countAdjustKey++;
            return super.adjustKey(key);
        }

        @Override
        public void setAdjustKeyProvider(final IHistoricalCacheAdjustKeyProvider adjustKeyProvider) {
            super.setAdjustKeyProvider(adjustKeyProvider);
        }

        @Override
        protected Iterable<FDate> readAllValuesAscendingFrom(final FDate key) {
            countReadAllValuesAscendingFrom++;
            if (returnMaxResults != null) {
                Assertions.assertThat(returnAllInReadAllValuesAscendingFrom).isFalse();
                Assertions.assertThat(returnNullInReadNewestValueTo).isFalse();
            }
            List<FDate> result;
            if (returnAllInReadAllValuesAscendingFrom && returnMaxResults == null) {
                result = Lists.toListWithoutHasNext(table.rangeValues(KEY, null, null));
            } else {
                result = Lists.toListWithoutHasNext(table.rangeValues(KEY, key, null));
            }
            if (returnMaxResults != null && !result.isEmpty()) {
                result = result.subList(0, Math.min(result.size(), returnMaxResults));
            }
            return new BufferingIterator<FDate>(result);
        }

        @Override
        protected FDate innerExtractKey(final FDate entity) {
            countInnerExtractKey++;
            return entity;
        }

        @Override
        protected FDate readLatestValueFor(final FDate key) {
            countReadNewestValueTo++;
            if (returnNullInReadNewestValueTo) {
                return null;
            } else {
                return table.getLatestValue(KEY, key);
            }
        }

        @Override
        protected FDate innerCalculatePreviousKey(final FDate key) {
            return table.getPreviousValueKey(KEY, key.addMilliseconds(-1), 0);
        }

        @Override
        protected FDate innerCalculateNextKey(final FDate key) {
            return table.getNextValueKey(KEY, key.addMilliseconds(1), 0);
        }

    }

}
