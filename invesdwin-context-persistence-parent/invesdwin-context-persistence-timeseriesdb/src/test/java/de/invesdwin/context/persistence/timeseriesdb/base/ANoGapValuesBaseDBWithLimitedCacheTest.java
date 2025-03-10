package de.invesdwin.context.persistence.timeseriesdb.base;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.base.root.ARootDBTest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.collections.loadingcache.historical.IHistoricalEntry;
import de.invesdwin.util.collections.loadingcache.historical.refresh.HistoricalCacheRefreshManager;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

@NotThreadSafe
public abstract class ANoGapValuesBaseDBWithLimitedCacheTest extends ARootDBTest {

    @Override
    protected ATestGapHistoricalCache newTestGapHistoricalCache() {
        return new TestGapHistoricalCache();
    }

    @Test
    public void testNewEntityIncomingAfterClear() throws IncompleteUpdateRetryableException {
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
        final FDate wrongValue = cache.query().getValue(newEntity);
        Assertions.assertThat(wrongValue).isEqualTo(entities.get(entities.size() - 2));
        HistoricalCacheRefreshManager.forceRefresh();
        final FDate correctValue = cache.query().getValue(newEntity);
        Assertions.assertThat(correctValue).isEqualTo(newEntity);
    }

    @Test
    public void testGetPreviousAndNextValue() {
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = cache.query().getPreviousValue(entities.get(entities.size() - 1), i);
            final FDate expectedValue = entities.get(entities.size() - i - 1);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = cache.query().getPreviousValue(FDates.MAX_DATE, i);
            final FDate expectedValue = entities.get(entities.size() - i - 1);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = cache.query().setFutureEnabled().getPreviousValue(FDates.MIN_DATE, i);
            final FDate expectedValue = null; //filtering query removes the result because it is not a previous result
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }

        for (int i = 0; i < entities.size(); i++) {
            final FDate value = cache.query().setFutureEnabled().getNextValue(entities.get(0), i);
            final FDate expectedValue = entities.get(i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = cache.query().setFutureEnabled().getNextValue(FDates.MIN_DATE, i);
            final FDate expectedValue = entities.get(i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = cache.query().setFutureEnabled().getNextValue(FDates.MAX_DATE, i);
            final FDate expectedValue = null; //filtering query removes the result because it is not a previous result
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
    }

    @Test
    public void testGetPreviousAndNextValues() {
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists
                    .toListWithoutHasNext(cache.query().getPreviousValues(entities.get(entities.size() - 1), i));
            final List<FDate> expectedValue = entities.subList(entities.size() - i, entities.size());
            Assertions.checkEquals(expectedValue.size(), i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists.toListWithoutHasNext(cache.query().getPreviousValues(FDates.MAX_DATE, i));
            final List<FDate> expectedValue = entities.subList(entities.size() - i, entities.size());
            Assertions.checkEquals(expectedValue.size(), i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists
                    .toListWithoutHasNext(cache.query().setFutureEnabled().getPreviousValues(FDates.MIN_DATE, i));
            final List<FDate> expectedValue = Collections.emptyList(); //filtering query removes the result because it is not a previous result
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }

        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists
                    .toListWithoutHasNext(cache.query().setFutureEnabled().getNextValues(entities.get(0), i));
            final List<FDate> expectedValue = entities.subList(0, i);
            Assertions.checkEquals(expectedValue.size(), i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists
                    .toListWithoutHasNext(cache.query().setFutureEnabled().getNextValues(FDates.MIN_DATE, i));
            final List<FDate> expectedValue = entities.subList(0, i);
            Assertions.checkEquals(expectedValue.size(), i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists
                    .toListWithoutHasNext(cache.query().setFutureEnabled().getNextValues(FDates.MAX_DATE, i));
            final List<FDate> expectedValue = Collections.emptyList(); //filtering query removes the result because it is not a previous result
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
    }

    @Test
    public void testGetPreviousAndNextKeys() {
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists
                    .toListWithoutHasNext(cache.query().getPreviousKeys(entities.get(entities.size() - 1), i));
            final List<FDate> expectedValue = entities.subList(entities.size() - i, entities.size());
            Assertions.checkEquals(expectedValue.size(), i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists.toListWithoutHasNext(cache.query().getPreviousKeys(FDates.MAX_DATE, i));
            final List<FDate> expectedValue = entities.subList(entities.size() - i, entities.size());
            Assertions.checkEquals(expectedValue.size(), i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists
                    .toListWithoutHasNext(cache.query().setFutureEnabled().getPreviousKeys(FDates.MIN_DATE, i));
            final List<FDate> expectedValue = Collections.emptyList(); //filtering query removes the result because it is not a previous result
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }

        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists
                    .toListWithoutHasNext(cache.query().setFutureEnabled().getNextKeys(entities.get(0), i));
            final List<FDate> expectedValue = entities.subList(0, i);
            Assertions.checkEquals(expectedValue.size(), i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists
                    .toListWithoutHasNext(cache.query().setFutureEnabled().getNextKeys(FDates.MIN_DATE, i));
            final List<FDate> expectedValue = entities.subList(0, i);
            Assertions.checkEquals(expectedValue.size(), i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists
                    .toListWithoutHasNext(cache.query().setFutureEnabled().getNextKeys(FDates.MAX_DATE, i));
            final List<FDate> expectedValue = Collections.emptyList(); //filtering query removes the result because it is not a previous result
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
    }

    @Test
    public void testGetPreviousAndNextEntries() {
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists.toListWithoutHasNext(IHistoricalEntry
                    .unwrapEntryValues(cache.query().getPreviousEntries(entities.get(entities.size() - 1), i)));
            final List<FDate> expectedValue = entities.subList(entities.size() - i, entities.size());
            Assertions.checkEquals(expectedValue.size(), i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists.toListWithoutHasNext(
                    IHistoricalEntry.unwrapEntryValues(cache.query().getPreviousEntries(FDates.MAX_DATE, i)));
            final List<FDate> expectedValue = entities.subList(entities.size() - i, entities.size());
            Assertions.checkEquals(expectedValue.size(), i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists.toListWithoutHasNext(IHistoricalEntry
                    .unwrapEntryValues(cache.query().setFutureEnabled().getPreviousEntries(FDates.MIN_DATE, i)));
            final List<FDate> expectedValue = Collections.emptyList(); //filtering query removes the result because it is not a previous result
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }

        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists.toListWithoutHasNext(IHistoricalEntry
                    .unwrapEntryValues(cache.query().setFutureEnabled().getNextEntries(entities.get(0), i)));
            final List<FDate> expectedValue = entities.subList(0, i);
            Assertions.checkEquals(expectedValue.size(), i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists.toListWithoutHasNext(IHistoricalEntry
                    .unwrapEntryValues(cache.query().setFutureEnabled().getNextEntries(FDates.MIN_DATE, i)));
            final List<FDate> expectedValue = entities.subList(0, i);
            Assertions.checkEquals(expectedValue.size(), i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 1; i < entities.size(); i++) {
            final List<FDate> value = Lists.toListWithoutHasNext(IHistoricalEntry
                    .unwrapEntryValues(cache.query().setFutureEnabled().getNextEntries(FDates.MAX_DATE, i)));
            final List<FDate> expectedValue = Collections.emptyList(); //filtering query removes the result because it is not a previous result
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
    }

    @Test
    public void testGetPreviousAndNextKey() {
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = cache.query().getPreviousKey(entities.get(entities.size() - 1), i);
            final FDate expectedValue = entities.get(entities.size() - i - 1);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = cache.query().getPreviousKey(FDates.MAX_DATE, i);
            final FDate expectedValue = entities.get(entities.size() - i - 1);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = cache.query().setFutureEnabled().getPreviousKey(FDates.MIN_DATE, i);
            final FDate expectedValue = FDates.MIN_DATE; //filtering query removes the result because it is not a previous result
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }

        for (int i = 0; i < entities.size(); i++) {
            final FDate value = cache.query().setFutureEnabled().getNextKey(entities.get(0), i);
            final FDate expectedValue = entities.get(i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = cache.query().setFutureEnabled().getNextKey(FDates.MIN_DATE, i);
            final FDate expectedValue = entities.get(i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = cache.query().setFutureEnabled().getNextKey(FDates.MAX_DATE, i);
            final FDate expectedValue = null; //filtering query removes the result because it is not a previous result
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
    }

    @Test
    public void testGetPreviousAndNextEntry() {
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = IHistoricalEntry
                    .unwrapEntryValue(cache.query().getPreviousEntry(entities.get(entities.size() - 1), i));
            final FDate expectedValue = entities.get(entities.size() - i - 1);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = IHistoricalEntry.unwrapEntryValue(cache.query().getPreviousEntry(FDates.MAX_DATE, i));
            final FDate expectedValue = entities.get(entities.size() - i - 1);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = IHistoricalEntry
                    .unwrapEntryValue(cache.query().setFutureEnabled().getPreviousEntry(FDates.MIN_DATE, i));
            final FDate expectedValue = null; //filtering query removes the result because it is not a previous result
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }

        for (int i = 0; i < entities.size(); i++) {
            final FDate value = IHistoricalEntry
                    .unwrapEntryValue(cache.query().setFutureEnabled().getNextEntry(entities.get(0), i));
            final FDate expectedValue = entities.get(i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = IHistoricalEntry
                    .unwrapEntryValue(cache.query().setFutureEnabled().getNextEntry(FDates.MIN_DATE, i));
            final FDate expectedValue = entities.get(i);
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
        for (int i = 0; i < entities.size(); i++) {
            final FDate value = IHistoricalEntry
                    .unwrapEntryValue(cache.query().setFutureEnabled().getNextEntry(FDates.MAX_DATE, i));
            final FDate expectedValue = null; //filtering query removes the result because it is not a previous result
            Assertions.checkEquals(value, expectedValue, i + ": expected [" + expectedValue + "] got [" + value + "]");
        }
    }

    @Test
    public void testInconsistentGapKey() {
        FDate searchedKey = entities.get(0);
        FDate value = cache.query().getValue(searchedKey);
        Assertions.assertThat(value).isEqualTo(searchedKey);

        searchedKey = entities.get(1);
        value = cache.query().getValue(searchedKey.addDays(1));
        Assertions.assertThat(value).isEqualTo(searchedKey);
    }

    @Test
    public void testGaps() {
        //once through the complete list
        final List<FDate> liste = new ArrayList<FDate>();
        for (final FDate entity : entities) {
            final FDate cachedEntity = cache.query().getValue(entity);
            liste.add(cachedEntity);
            Assertions.assertThat(cachedEntity).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(liste).isEqualTo(entities);

        //new maxKey without new db results
        for (final FDate entity : entities) {
            Assertions.assertThat(cache.query().getValue(entity.addDays(5))).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(3);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        //new minKey without new db limit
        for (final FDate entity : entities) {
            Assertions.assertThat(cache.query().setFutureEnabled().getValue(entity.addYears(-5))).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(4);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        //again in the same limit
        for (final FDate entity : entities) {
            Assertions.assertThat(cache.query().getValue(entity.addDays(2))).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        //simulate cache eviction
        cache.clear();
        for (final FDate entity : entities) {
            Assertions.assertThat(cache.query().getValue(entity.addDays(2))).isNotNull();
        }
        cache.clear();
        for (final FDate entity : entities) {
            Assertions.assertThat(cache.query().getValue(entity.addDays(2))).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(9);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(6);

        //random order
        for (final FDate entity : new HashSet<FDate>(entities)) {
            Assertions.assertThat(cache.query().getValue(entity.addDays(2))).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isLessThanOrEqualTo(14);
        Assertions.assertThat(countReadNewestValueTo).isLessThanOrEqualTo(19);
    }

    @Test
    public void testGapsWithReturnMaxResults() {
        returnMaxResults = testReturnMaxResultsValue;

        //once through the complete list
        final List<FDate> liste = new ArrayList<FDate>();
        for (final FDate entity : entities) {
            final FDate cachedEntity = cache.query().getValue(entity);
            liste.add(cachedEntity);
            Assertions.assertThat(cachedEntity).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(3);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(liste).isEqualTo(entities);

        //new maxKey without new db results
        for (final FDate entity : entities) {
            Assertions.assertThat(cache.query().getValue(entity.addDays(5))).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(9);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        //new minKey without new db limit
        for (final FDate entity : entities) {
            Assertions.assertThat(cache.query().setFutureEnabled().getValue(entity.addYears(-5))).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(10);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        //again in the same limit
        for (final FDate entity : entities) {
            Assertions.assertThat(cache.query().getValue(entity.addDays(2))).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(15);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        //simulate cache eviction
        cache.clear();
        for (final FDate entity : entities) {
            Assertions.assertThat(cache.query().getValue(entity.addDays(2))).isNotNull();
        }
        cache.clear();
        for (final FDate entity : entities) {
            Assertions.assertThat(cache.query().getValue(entity.addDays(2))).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(27);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(6);

        //random order
        for (final FDate entity : new HashSet<FDate>(entities)) {
            Assertions.assertThat(cache.query().getValue(entity.addDays(2))).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isLessThanOrEqualTo(37);
        Assertions.assertThat(countReadNewestValueTo).isLessThanOrEqualTo(27);
    }

    @Test
    public void testOneResult() {
        for (final FDate entity : entities) {
            Assertions.assertThat(cache.query().getValue(entity.addYears(5))).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(4);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testNoResultsUp() {
        for (final FDate entity : entities) {
            Assertions.assertThat(cache.query().getValue(entity.addYears(100))).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testNoResultsDown() {
        for (final FDate entity : entities) {
            Assertions.assertThat(cache.query().setFutureEnabled().getValue(entity.addYears(-100))).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    /**
     * for reverse sorting this is less efficient, this costs O(n) for queries.
     */
    @Test
    public void testInverseOrder() {
        final List<FDate> ents = new ArrayList<FDate>(entities);
        Collections.reverse(ents);
        for (final FDate entity : ents) {
            Assertions.assertThat(cache.query().getValue(entity)).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testInverseOrderWithReturnMaxResults() {
        returnMaxResults = testReturnMaxResultsValue;

        final List<FDate> ents = new ArrayList<FDate>(entities);
        Collections.reverse(ents);
        for (final FDate entity : ents) {
            Assertions.assertThat(cache.query().getValue(entity)).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousKey() {
        FDate previousKey = cache.query().getPreviousKey(new FDate(), entities.size());
        Assertions.assertThat(previousKey).isEqualTo(entities.get(0));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        //loading newest entity is faster than always loading all entities
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        previousKey = cache.query().getPreviousKey(new FDate(), 1);
        Assertions.assertThat(previousKey).isEqualTo(entities.get(entities.size() - 2));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isLessThanOrEqualTo(7);
        Assertions.assertThat(countReadNewestValueTo).isLessThanOrEqualTo(9);
    }

    @Test
    public void testNextKey() {
        FDate nextKey = cache.query().setFutureEnabled().getNextKey(FDates.MIN_DATE, entities.size());
        Assertions.assertThat(nextKey).isEqualTo(entities.get(entities.size() - 1));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        //loading newest entity is faster than always loading all entities
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        nextKey = cache.query().setFutureEnabled().getNextKey(FDates.MIN_DATE, 1);
        Assertions.assertThat(nextKey).isEqualTo(entities.get(1));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousValueWithDistance() {
        FDate previousValue = cache.query().getPreviousValue(new FDate(), entities.size());
        Assertions.assertThat(previousValue).isEqualTo(entities.get(0));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        //loading newest entity is faster than always loading all entities
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        previousValue = cache.query().getPreviousValue(new FDate(), 1);
        Assertions.assertThat(previousValue).isEqualTo(entities.get(entities.size() - 2));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isLessThanOrEqualTo(7);
        Assertions.assertThat(countReadNewestValueTo).isLessThanOrEqualTo(9);
    }

    @Test
    public void testNextValueWithDistance() {
        FDate nextValue = cache.query().setFutureEnabled().getNextValue(FDates.MIN_DATE, entities.size());
        Assertions.assertThat(nextValue).isEqualTo(entities.get(entities.size() - 1));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        //loading newest entity is faster than always loading all entities
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        nextValue = cache.query().setFutureEnabled().getNextValue(FDates.MIN_DATE, 1);
        Assertions.assertThat(nextValue).isEqualTo(entities.get(1));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousValueWithoutDistance() {
        FDate previousValue = cache.query()

                .getPreviousValue(entities.get(entities.size() - 1), entities.size());
        Assertions.assertThat(previousValue).isEqualTo(entities.get(0));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        //loading newest entity is faster than always loading all entities
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        previousValue = cache.query().getPreviousValue(entities.get(entities.size() - 1), 1);
        Assertions.assertThat(previousValue).isEqualTo(entities.get(entities.size() - 2));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testNextValueWithoutDistance() {
        FDate nextValue = cache.query().setFutureEnabled().getNextValue(entities.get(0), entities.size());
        Assertions.assertThat(nextValue).isEqualTo(entities.get(entities.size() - 1));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        //loading newest entity is faster than always loading all entities
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        nextValue = cache.query().setFutureEnabled().getNextValue(entities.get(0), 1);
        Assertions.assertThat(nextValue).isEqualTo(entities.get(1));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousKeys() {
        final Collection<FDate> previousKeys = asList(cache.query().getPreviousKeys(new FDate(), entities.size()));
        Assertions.assertThat(previousKeys).isEqualTo(entities);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousKeysBeforeFirst() {
        Collection<FDate> previousKeys = asList(
                cache.query().setFutureEnabled().getPreviousKeys(FDates.MIN_DATE, entities.size()));
        Assertions.assertThat(previousKeys).isEmpty();
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        previousKeys = asList(cache.query().setFutureNullEnabled().getPreviousKeys(FDates.MIN_DATE, entities.size()));
        Assertions.assertThat(previousKeys).isEmpty();
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        previousKeys = asList(cache.query().setFutureEnabled().getPreviousKeys(FDates.MIN_DATE, entities.size()));
        Assertions.assertThat(previousKeys).isEmpty();
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousKeyBeforeFirst() {
        FDate previousKey = cache.query().setFutureEnabled().getPreviousKey(FDates.MIN_DATE, entities.size());
        Assertions.assertThat(previousKey).isEqualTo(FDates.MIN_DATE);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        previousKey = cache.query().setFutureNullEnabled().getPreviousKey(FDates.MIN_DATE, entities.size());
        Assertions.assertThat(previousKey).isEqualTo(FDates.MIN_DATE);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        previousKey = cache.query().setFutureEnabled().getPreviousKey(FDates.MIN_DATE, entities.size());
        Assertions.assertThat(previousKey).isEqualTo(FDates.MIN_DATE);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousKeysBeforeFirstReverse() {
        Collection<FDate> previousKeys = asList(
                cache.query().setFutureNullEnabled().getPreviousKeys(FDates.MIN_DATE, entities.size()));
        Assertions.assertThat(previousKeys).isEmpty();
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        previousKeys = asList(cache.query().setFutureEnabled().getPreviousKeys(FDates.MIN_DATE, entities.size()));
        Assertions.assertThat(previousKeys).isEmpty();
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        previousKeys = asList(cache.query().setFutureNullEnabled().getPreviousKeys(FDates.MIN_DATE, entities.size()));
        Assertions.assertThat(previousKeys).isEmpty();
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousKeyBeforeFirstReverse() {
        FDate previousKey = cache.query().setFutureNullEnabled().getPreviousKey(FDates.MIN_DATE, entities.size());
        Assertions.assertThat(previousKey).isEqualTo(FDates.MIN_DATE);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        previousKey = cache.query().setFutureEnabled().getPreviousKey(FDates.MIN_DATE, entities.size());
        Assertions.assertThat(previousKey).isEqualTo(FDates.MIN_DATE);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        previousKey = cache.query().setFutureNullEnabled().getPreviousKey(FDates.MIN_DATE, entities.size());
        Assertions.assertThat(previousKey).isEqualTo(FDates.MIN_DATE);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testNextKeys() {
        final Collection<FDate> nextKeys = asList(
                cache.query().setFutureEnabled().getNextKeys(FDates.MIN_DATE, entities.size()));
        Assertions.assertThat(nextKeys).isEqualTo(entities);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testKeys() {
        final Iterable<FDate> iterable = cache.query().getKeys(FDates.MIN_DATE, FDates.MAX_DATE);
        final List<FDate> previousKeys = new ArrayList<FDate>();
        for (final FDate d : iterable) {
            previousKeys.add(d);
        }
        Assertions.assertThat(previousKeys).isEqualTo(entities);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testValues() {
        final Iterable<FDate> iterable = cache.query().getValues(entities.get(0).addMilliseconds(1), FDates.MAX_DATE);
        final List<FDate> previousKeys = new ArrayList<FDate>();
        for (final FDate d : iterable) {
            previousKeys.add(d);
        }
        Assertions.assertThat(previousKeys).isEqualTo(entities.subList(1, entities.size()));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testKeysWithReturnMaxResults() {
        returnMaxResults = testReturnMaxResultsValue;

        final Iterable<FDate> iterable = cache.query().getKeys(FDates.MIN_DATE, FDates.MAX_DATE);
        final List<FDate> previousKeys = new ArrayList<FDate>();
        for (final FDate d : iterable) {
            previousKeys.add(d);
        }
        Assertions.assertThat(previousKeys).isEqualTo(entities);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(3);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousValuesWithDistance() {
        final Collection<FDate> previousValues = asList(cache.query().getPreviousValues(new FDate(), entities.size()));
        Assertions.assertThat(previousValues).isEqualTo(entities);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testNextValuesWithDistance() {
        final Collection<FDate> nextValues = asList(
                cache.query().setFutureEnabled().getNextValues(FDates.MIN_DATE, entities.size()));
        Assertions.assertThat(nextValues).isEqualTo(entities);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousValuesWithoutDistance() {
        final Collection<FDate> previousValues = asList(
                cache.query().getPreviousValues(entities.get(entities.size() - 1), entities.size()));
        Assertions.assertThat(previousValues).isEqualTo(entities);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testNextValuesWithoutDistance() {
        final Collection<FDate> nextValues = asList(
                cache.query().setFutureEnabled().getNextValues(entities.get(0), entities.size()));
        Assertions.assertThat(nextValues).isEqualTo(entities);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousValuesGetsFilledDownWithDistance() {
        final Collection<FDate> previousValues = asList(
                cache.query().setFutureEnabled().getPreviousValues(FDates.MIN_DATE, entities.size()));
        Assertions.assertThat(previousValues.size()).isEqualTo(0);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testNextValuesGetsFilledUpWithDistance() {
        final Collection<FDate> nextValues = asList(
                cache.query().setFutureEnabled().getNextValues(FDates.MAX_DATE, entities.size()));
        Assertions.assertThat(nextValues.size()).isEqualTo(0);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousValuesGetsFilledDownWithoutDistance() {
        final Collection<FDate> previousValues = asList(
                cache.query().getPreviousValues(entities.get(0), entities.size()));
        Assertions.assertThat(previousValues.size()).isEqualTo(1);
        for (final FDate d : previousValues) {
            Assertions.assertThat(d).isEqualTo(entities.get(0));
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testNextValuesGetsFilledUpWithoutDistance() {
        final Collection<FDate> nextValues = asList(
                cache.query().setFutureEnabled().getNextValues(entities.get(entities.size() - 1), entities.size()));
        Assertions.assertThat(nextValues.size()).isEqualTo(1);
        for (final FDate d : nextValues) {
            Assertions.assertThat(d).isEqualTo(entities.get(entities.size() - 1));
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testNoDataInReadAllValuesAscendingFrom() {
        returnNullInReadNewestValueTo = true;
        for (final FDate entity : entities) {
            final FDate cachedEntity = cache.query().getValue(entity);
            Assertions.assertThat(cachedEntity).isNotNull();
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        //new minKey limit gets tested
        final Collection<FDate> values = asList(cache.query().setFutureEnabled().getPreviousValues(FDates.MIN_DATE, 5));
        for (final FDate d : values) {
            Assertions.assertThat(d).isEqualTo(entities.get(0));
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousKeyWithAllValues() {
        returnAllInReadAllValuesAscendingFrom = true;

        FDate previousKey = cache.query().getPreviousKey(new FDate(), entities.size());
        Assertions.assertThat(previousKey).isEqualTo(entities.get(0));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        //loading newest entity is faster than always loading all entities
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(3);

        previousKey = cache.query().getPreviousKey(new FDate(), 1);
        Assertions.assertThat(previousKey).isEqualTo(entities.get(entities.size() - 2));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isLessThanOrEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(3);
    }

    @Test
    public void testNextKeyWithAllValues() {
        returnAllInReadAllValuesAscendingFrom = true;

        FDate nextKey = cache.query().setFutureEnabled().getNextKey(FDates.MIN_DATE, entities.size());
        Assertions.assertThat(nextKey).isEqualTo(entities.get(entities.size() - 1));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        //loading newest entity is faster than always loading all entities
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        nextKey = cache.query().setFutureEnabled().getNextKey(FDates.MIN_DATE, 1);
        Assertions.assertThat(nextKey).isEqualTo(entities.get(1));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousKeyWithReturnMaxResults() {
        returnMaxResults = testReturnMaxResultsValue;

        FDate previousKey = cache.query().getPreviousKey(new FDate(), entities.size());
        Assertions.assertThat(previousKey).isEqualTo(entities.get(0));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        //loading newest entity is faster than always loading all entities
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        previousKey = cache.query().getPreviousKey(new FDate(), 1);
        Assertions.assertThat(previousKey).isEqualTo(entities.get(entities.size() - 2));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isLessThanOrEqualTo(8);
        Assertions.assertThat(countReadNewestValueTo).isLessThanOrEqualTo(9);
    }

    @Test
    public void testNextKeyWithReturnMaxResults() {
        returnMaxResults = testReturnMaxResultsValue;

        FDate nextKey = cache.query().setFutureEnabled().getNextKey(FDates.MIN_DATE, entities.size());
        Assertions.assertThat(nextKey).isEqualTo(entities.get(entities.size() - 1));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(3);
        //loading newest entity is faster than always loading all entities
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        nextKey = cache.query().setFutureEnabled().getNextKey(FDates.MIN_DATE, 1);
        Assertions.assertThat(nextKey).isEqualTo(entities.get(1));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(4);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousKeyWithAllValuesAndNullInReadNewestValueTo() {
        returnAllInReadAllValuesAscendingFrom = true;
        returnNullInReadNewestValueTo = true;

        FDate previousKey = cache.query().getPreviousKey(new FDate(), entities.size());
        Assertions.assertThat(previousKey).isEqualTo(entities.get(0));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(3);
        //loading newest entity is faster than always loading all entities
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        previousKey = cache.query().getPreviousKey(new FDate(), 1);
        Assertions.assertThat(previousKey).isEqualTo(entities.get(entities.size() - 2));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isLessThanOrEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testNextKeyWithAllValuesAndNullInReadNewestValueTo() {
        returnAllInReadAllValuesAscendingFrom = true;
        returnNullInReadNewestValueTo = true;

        FDate nextKey = cache.query().setFutureEnabled().getNextKey(FDates.MIN_DATE, entities.size());
        Assertions.assertThat(nextKey).isEqualTo(entities.get(entities.size() - 1));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        //loading newest entity is faster than always loading all entities
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);

        nextKey = cache.query().setFutureEnabled().getNextKey(FDates.MIN_DATE, 1);
        Assertions.assertThat(nextKey).isEqualTo(entities.get(1));
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousKeysFilterDuplicateKeys() {
        Assertions.assertThat(asList(cache.query().getPreviousKeys(new FDate(), 100)).size()).isSameAs(6);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(asList(cache.query().getPreviousKeys(new FDate(), 100)).size())
                .isEqualTo(entities.size());
        Assertions.assertThat(countReadAllValuesAscendingFrom).isLessThanOrEqualTo(11);
        Assertions.assertThat(countReadNewestValueTo).isLessThanOrEqualTo(11);
    }

    @Test
    public void testNextKeysFilterDuplicateKeys() {
        Assertions.assertThat(asList(cache.query().setFutureEnabled().getNextKeys(FDates.MIN_DATE, 100)).size())
                .isSameAs(6);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(asList(cache.query().setFutureEnabled().getNextKeys(FDates.MIN_DATE, 100)).size())
                .isEqualTo(entities.size());
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousValuesFilterDuplicateKeys() {
        Assertions.assertThat(asList(cache.query().getPreviousValues(new FDate(), 100)).size()).isSameAs(6);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(asList(cache.query().getPreviousValues(new FDate(), 100)).size())
                .isEqualTo(entities.size());
        Assertions.assertThat(countReadAllValuesAscendingFrom).isLessThanOrEqualTo(11);
        Assertions.assertThat(countReadNewestValueTo).isLessThanOrEqualTo(11);
    }

    @Test
    public void testNextValuesFilterDuplicateKeys() {
        Assertions.assertThat(asList(cache.query().setFutureEnabled().getNextValues(FDates.MIN_DATE, 100)).size())
                .isSameAs(6);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(asList(cache.query().setFutureEnabled().getNextValues(FDates.MIN_DATE, 100)).size())
                .isEqualTo(entities.size());
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
    }

    @Test
    public void testPreviousValueKeyBetween() {
        for (int i = 0; i < entities.size(); i++) {
            final FDate entity = entities.get(i);
            final FDate foundKey = cache.query()
                    .getPreviousKeyWithSameValueBetween(FDates.MIN_DATE, FDates.MAX_DATE, entity);
            Assertions.assertThat(foundKey).isEqualTo(entity);
        }
    }

    @Test
    public void testPreviousValueKeyBetweenReverse() {
        for (int i = entities.size() - 1; i >= 0; i--) {
            final FDate entity = entities.get(i);
            final FDate foundKey = cache.query()
                    .getPreviousKeyWithSameValueBetween(FDates.MIN_DATE, FDates.MAX_DATE, entity);
            Assertions.assertThat(foundKey).isEqualTo(entity);
        }
    }

    @Test
    public void testNotCorrectTime() {
        for (final FDate entity : entities.subList(1, entities.size() - 1)) {
            final FDate valueBefore = cache.query().getValue(entity.addHours(-3));
            Assertions.assertThat(valueBefore).isEqualTo(entity.addYears(-1));
            final FDate value = cache.query().getValue(entity);
            Assertions.assertThat(value).isEqualTo(entity);
            final FDate valueAfter = cache.query().getValue(entity.addHours(2));
            Assertions.assertThat(valueAfter).isEqualTo(entity);
        }
    }

    @Test
    public void testPreviousValuesWithQueryCacheWithAlwaysSameKey() {
        for (int size = 1; size < entities.size(); size++) {
            final Collection<FDate> previousValues = asList(
                    cache.query().getPreviousValues(entities.get(entities.size() - 1), size));
            final List<FDate> expectedValues = entities.subList(entities.size() - size, entities.size());
            Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(4);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(4);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testPreviousValuesWithQueryCacheWithIncrementingKey() {
        for (int index = 0; index < entities.size(); index++) {
            final Collection<FDate> previousValues = asList(
                    cache.query().getPreviousValues(entities.get(index), index + 1));
            final List<FDate> expectedValues = entities.subList(0, index + 1);
            Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(3);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(1);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testPreviousValuesWithQueryCacheWithIncrementingAlwaysOneValue() {
        for (int index = 0; index < entities.size(); index++) {
            final Collection<FDate> previousValues = asList(cache.query().getPreviousValues(entities.get(index), 1));
            final List<FDate> expectedValues = entities.subList(index, index + 1);
            Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(1);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testPreviousValuesWithQueryCacheWithDecrementingKey() {
        for (int index = entities.size() - 1; index >= 0; index--) {
            final Collection<FDate> previousValues = asList(
                    cache.query().getPreviousValues(entities.get(index), index + 1));
            final List<FDate> expectedValues = entities.subList(0, index + 1);
            Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(5);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testPreviousValuesWithQueryCacheWithDecrementingKeyAlwaysOne() {
        for (int index = entities.size() - 1; index >= 0; index--) {
            final Collection<FDate> previousValues = asList(cache.query().getPreviousValues(entities.get(index), 1));
            final List<FDate> expectedValues = entities.subList(index, index + 1);
            Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(5);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testPreviousValuesWithQueryCacheWithJumpingAround() {
        //first
        Collection<FDate> previousValues = asList(cache.query().getPreviousValues(entities.get(0), 1));
        List<FDate> expectedValues = entities.subList(0, 1);
        Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(0);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        //last
        previousValues = asList(cache.query().getPreviousValues(entities.get(entities.size() - 1), 1));
        expectedValues = entities.subList(entities.size() - 1, entities.size());
        Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(0);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        //first +1
        previousValues = asList(cache.query().getPreviousValues(entities.get(1), 1));
        expectedValues = entities.subList(1, 2);
        Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(1);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        //last -1
        previousValues = asList(cache.query().getPreviousValues(entities.get(entities.size() - 2), 1));
        expectedValues = entities.subList(entities.size() - 2, entities.size() - 1);
        Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(1);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testPreviousValuesWithQueryCacheWithJumpingAroundTwoValues() {
        //first
        Collection<FDate> previousValues = asList(cache.query().getPreviousValues(entities.get(1), 2));
        List<FDate> expectedValues = entities.subList(0, 2);
        Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(2);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        //last
        previousValues = asList(cache.query().getPreviousValues(entities.get(entities.size() - 1), 2));
        expectedValues = entities.subList(entities.size() - 2, entities.size());
        Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(2);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        //first +1
        previousValues = asList(cache.query().getPreviousValues(entities.get(2), 2));
        expectedValues = entities.subList(1, 3);
        Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(2);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        //last -1
        previousValues = asList(cache.query().getPreviousValues(entities.get(entities.size() - 2), 2));
        expectedValues = entities.subList(entities.size() - 3, entities.size() - 1);
        Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(2);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testPreviousValueWithQueryCacheWithAlwaysSameKey() {
        for (int size = 1; size < entities.size(); size++) {
            final FDate previousValue = cache.query().getPreviousValue(entities.get(entities.size() - 1), size);
            final FDate expectedValue = entities.get(entities.size() - size - 1);
            Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(5);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testPreviousValueWithQueryCacheWithIncrementingKey() {
        for (int index = 0; index < entities.size(); index++) {
            final FDate previousValue = cache.query().getPreviousValue(entities.get(index), index + 1);
            final FDate expectedValue = entities.get(0);
            Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(3);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(1);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testPreviousValueWithQueryCacheWithIncrementingAlwaysOneValue() {
        for (int index = 1; index < entities.size(); index++) {
            final FDate previousValue = cache.query().getPreviousValue(entities.get(index), 1);
            final FDate expectedValue = entities.get(index - 1);
            Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(2);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testPreviousValueWithQueryCacheWithDecrementingKey() {
        for (int index = entities.size() - 1; index >= 0; index--) {
            final FDate previousValue = cache.query().getPreviousValue(entities.get(index), index + 1);
            final FDate expectedValue = entities.get(0);
            Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(5);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testPreviousValueWithQueryCacheWithDecrementingKeyAlwaysOne() {
        for (int index = entities.size() - 1; index > 0; index--) {
            final FDate previousValue = cache.query().getPreviousValue(entities.get(index), 1);
            final FDate expectedValue = entities.get(index - 1);
            Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        }
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(5);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testPreviousValueWithQueryCacheWithJumpingAround() {
        //first
        FDate previousValue = cache.query().getPreviousValue(entities.get(0), 1);
        FDate expectedValue = entities.get(0);
        Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(0);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(0);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        //last
        previousValue = cache.query().getPreviousValue(entities.get(entities.size() - 1), 1);
        expectedValue = entities.get(entities.size() - 2);
        Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(1);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(1);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        //first +1
        previousValue = cache.query().getPreviousValue(entities.get(1), 1);
        expectedValue = entities.get(0);
        Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(3);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(2);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        //last -1
        previousValue = cache.query().getPreviousValue(entities.get(entities.size() - 2), 1);
        expectedValue = entities.get(entities.size() - 3);
        Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(3);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(2);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testPreviousValueWithQueryCacheWithJumpingAroundTwoValues() {
        //first
        FDate previousValue = cache.query().getPreviousValue(entities.get(1), 2);
        FDate expectedValue = entities.get(0);
        Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(2);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(2);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        //last
        previousValue = cache.query().getPreviousValue(entities.get(entities.size() - 1), 2);
        expectedValue = entities.get(entities.size() - 3);
        Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isLessThanOrEqualTo(3);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(2);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        //first +1
        previousValue = cache.query().getPreviousValue(entities.get(2), 2);
        expectedValue = entities.get(0);
        Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isLessThanOrEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(2);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        //last -1
        previousValue = cache.query().getPreviousValue(entities.get(entities.size() - 2), 2);
        expectedValue = entities.get(entities.size() - 4);
        Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isLessThanOrEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(2);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testSubListWhenSwitchingFromNonFilterToFilter() {
        final FDate key = new FDate();
        final FDate previousValue = cache.query().getPreviousValue(key, 4);
        final FDate expectedValue = entities.get(entities.size() - 5);
        Assertions.assertThat(previousValue).isEqualTo(expectedValue);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(4);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(4);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        final Collection<FDate> previousValues = asList(cache.query().getPreviousValues(key, 4));
        final List<FDate> expectedValues = entities.subList(2, 6);
        Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(4);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(4);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    @Test
    public void testSubListWhenSwitchingFromFilterToNonFilter() {
        final FDate key = new FDate();
        final Collection<FDate> previousValues = asList(cache.query().getPreviousValues(key, 10));
        final List<FDate> expectedValues = entities;
        Assertions.assertThat(previousValues).isEqualTo(expectedValues);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(5);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);

        final Collection<FDate> previousValuesCached = asList(cache.query().getPreviousValues(key, 10));
        Assertions.assertThat(previousValuesCached).isEqualTo(entities);
        Assertions.assertThat(countReadAllValuesAscendingFrom).isEqualTo(5);
        Assertions.assertThat(countReadNewestValueTo).isEqualTo(2);
        Assertions.assertThat(countInnerExtractKey).isEqualTo(5);
        Assertions.assertThat(countAdjustKey).isEqualTo(0);
    }

    private final class TestGapHistoricalCache extends ATestGapHistoricalCache {

        {
            enableTrailingQueryCore();
        }

        @Override
        protected de.invesdwin.util.collections.loadingcache.historical.internal.IValuesMap<FDate> newValuesMap() {
            return new ValuesMap();
        }

        @Override
        protected Integer getInitialMaximumSize() {
            return 1;
        }

    }

}
