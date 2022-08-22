package org.kr.intp.application.job.optimization.binpacking.algorithms;

import org.kr.intp.application.job.optimization.binpacking.Bin;
import org.kr.intp.application.job.optimization.binpacking.BinItem;
import org.kr.intp.application.job.optimization.binpacking.ItemExceedsCapacityException;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 */
public abstract class BinPackingAlgorithm<T> {

    /**
     * Executes the algorithm based on a list of given items.
     * @param capacity the capacity of the bins.
     * @param items a list of items toFIRS
     *              be packed.
     * @return the bin packing result.
     * @throws ItemExceedsCapacityException if an item exceeds the bin capacity this exception will be thrown.
     */
    public abstract List<Bin<T>> execute(int capacity, List<BinItem<T>> items) throws ItemExceedsCapacityException;

    /**
     * Returns the number of bins used.
     * @return the number of bins.
     */
    public abstract Integer getNumberOfBins();

    /**
     * Sorts a given array in decreasing order based on the item weights.
     * @param items the items to be sorted.
     */
    public void sortItemsInDecreasingOrder(List<BinItem<T>> items){
        Collections.sort(items, new Comparator<BinItem<T>>() {
            public int compare(BinItem<T> item1, BinItem<T> item2) {
                Integer item1Weight = item1.getWeight();
                Integer item2Weight = item2.getWeight();
                return item2Weight.compareTo(item1Weight);
            }
        });
    }

}