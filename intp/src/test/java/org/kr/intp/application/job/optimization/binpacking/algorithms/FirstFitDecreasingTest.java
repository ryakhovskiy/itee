package org.kr.intp.application.job.optimization.binpacking.algorithms;

import org.kr.intp.application.job.optimization.binpacking.Bin;
import org.kr.intp.application.job.optimization.binpacking.BinItem;
import org.kr.intp.application.job.optimization.binpacking.ItemExceedsCapacityException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 */
public class FirstFitDecreasingTest {
    @Test
    public void execute_ItemsSorted_Executed() throws Exception {
        BinPackingAlgorithm<Object[]> algorithm = new FirstFitDecreasing<>();

        BinItem<Object[]> item1 = new BinItem<Object[]>(5, new Object[]{"2014", "001", "A"});
        BinItem<Object[]> item2 = new BinItem<Object[]>(4, new Object[]{"2014", "001", "B"});
        BinItem<Object[]> item3 = new BinItem<Object[]>(3, new Object[]{"2014", "001", "C"});
        BinItem<Object[]> item4 = new BinItem<Object[]>(3, new Object[]{"2014", "001", "D"});
        List<BinItem<Object[]>> items = new ArrayList<>();
        items.add(item1);
        items.add(item2);
        items.add(item3);
        items.add(item4);

        List<Bin<Object[]>> result = algorithm.execute(7, items);
        int numOfBins = result.size();
        assertEquals(3, numOfBins);
    }

    @Test
    public void execute_ItemsNotSorted_Executed() throws Exception {
        BinPackingAlgorithm<Object[]> algorithm = new FirstFitDecreasing<>();

        BinItem<Object[]> item1 = new BinItem<Object[]>(4, new Object[]{"2014", "001", "A"});
        BinItem<Object[]> item2 = new BinItem<Object[]>(3, new Object[]{"2014", "001", "B"});
        BinItem<Object[]> item3 = new BinItem<Object[]>(4, new Object[]{"2014", "001", "C"});
        BinItem<Object[]> item4 = new BinItem<Object[]>(5, new Object[]{"2014", "001", "D"});
        List<BinItem<Object[]>> items = new ArrayList<>();
        items.add(item1);
        items.add(item2);
        items.add(item3);
        items.add(item4);

        List<Bin<Object[]>> result = algorithm.execute(7, items);
        int numOfBins = result.size();
        assertEquals(3, numOfBins);
    }

    @Test
    public void execute_CapacityNegativeValue_MaxItemWeightUsedAsCapacity() throws Exception {
        BinPackingAlgorithm<Object[]> algorithm = new FirstFitDecreasing<>();

        BinItem<Object[]> item1 = new BinItem<Object[]>(8, new Object[]{"2014", "001", "A"});
        BinItem<Object[]> item2 = new BinItem<Object[]>(3, new Object[]{"2014", "001", "B"});
        BinItem<Object[]> item3 = new BinItem<Object[]>(4, new Object[]{"2014", "001", "C"});
        BinItem<Object[]> item4 = new BinItem<Object[]>(5, new Object[]{"2014", "001", "D"});
        List<BinItem<Object[]>> items = new ArrayList<>();
        items.add(item1);
        items.add(item2);
        items.add(item3);
        items.add(item4);

        List<Bin<Object[]>> result = algorithm.execute(-1, items);
        int numOfBins = result.size();
        assertEquals(3, numOfBins);
        for(Bin<Object[]> bin : result){
            assertEquals(0, Double.compare(8d, bin.getCapacity()));
        }

    }

    @Test(expected=ItemExceedsCapacityException.class)
    public void execute_ItemTooBig_ThrowsItemExceedsCapacityException() throws Exception {
        BinPackingAlgorithm<Object[]> algorithm = new FirstFitDecreasing<>();

        BinItem<Object[]> item1 = new BinItem<Object[]>(8, new Object[]{"2014", "001", "A"});
        BinItem<Object[]> item2 = new BinItem<Object[]>(3, new Object[]{"2014", "001", "B"});
        List<BinItem<Object[]>> items = new ArrayList<>();
        items.add(item1);
        items.add(item2);

        List<Bin<Object[]>> result = algorithm.execute(7, items);
    }

    @Test
    public void execute_DuplicateItems_Executed() throws Exception {
        BinPackingAlgorithm<Object[]> algorithm = new FirstFitDecreasing<>();

        BinItem<Object[]> item1 = new BinItem<Object[]>(2, new Object[]{"2014", "001", "A"});
        BinItem<Object[]> item2 = new BinItem<Object[]>(2, new Object[]{"2014", "001", "A"});
        BinItem<Object[]> item3 = new BinItem<Object[]>(5, new Object[]{"2014", "001", "B"});
        BinItem<Object[]> item4 = new BinItem<Object[]>(5, new Object[]{"2014", "001", "B"});
        List<BinItem<Object[]>> items = new ArrayList<>();
        items.add(item1);
        items.add(item2);
        items.add(item3);
        items.add(item4);

        List<Bin<Object[]>> result = algorithm.execute(7, items);
        int numOfBins = result.size();
        assertEquals(2, numOfBins);
    }

    @Test
    public void execute_MultipleExecutes_Executed() throws Exception {
        BinPackingAlgorithm<Object[]> algorithm = new FirstFitDecreasing<>();

        // First Execute
        BinItem<Object[]> item1 = new BinItem<Object[]>(1, new Object[]{"2014", "001", "A"});
        BinItem<Object[]> item2 = new BinItem<Object[]>(7, new Object[]{"2014", "001", "B"});
        BinItem<Object[]> item3 = new BinItem<Object[]>(3, new Object[]{"2014", "001", "C"});
        BinItem<Object[]> item4 = new BinItem<Object[]>(4, new Object[]{"2014", "001", "D"});
        List<BinItem<Object[]>> items = new ArrayList<>();
        items.add(item1);
        items.add(item2);
        items.add(item3);
        items.add(item4);

        List<Bin<Object[]>> result = algorithm.execute(7, items);
        int numOfBins = result.size();
        assertEquals(3, numOfBins);

        // Second Execute
        BinItem<Object[]> item5 = new BinItem<Object[]>(2, new Object[]{"2014", "001", "E"});
        BinItem<Object[]> item6 = new BinItem<Object[]>(2, new Object[]{"2014", "001", "F"});
        BinItem<Object[]> item7 = new BinItem<Object[]>(2, new Object[]{"2014", "001", "G"});
        items.add(item5);
        items.add(item6);
        items.add(item7);

        result = algorithm.execute(7, items);
        numOfBins = result.size();
        assertEquals(3, numOfBins);
    }
}