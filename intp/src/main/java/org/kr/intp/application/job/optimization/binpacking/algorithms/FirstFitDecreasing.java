package org.kr.intp.application.job.optimization.binpacking.algorithms;

import org.kr.intp.application.job.optimization.binpacking.Bin;
import org.kr.intp.application.job.optimization.binpacking.BinItem;
import org.kr.intp.application.job.optimization.binpacking.ItemExceedsCapacityException;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class FirstFitDecreasing<T> extends BinPackingAlgorithm<T>{

    private List<Bin<T>> bins;
    private List<BinItem<T>> items;
    private int binCapacity;

    /**
     * Executes the First-Fit Decreasing algorithm.
     * @param binCapacity the capacity of the bins. If a negative number is given, the largest weight is used as capacity.
     * @param binItems the items to be packed.
     * @return the bins with the packed items.
     * @throws ItemExceedsCapacityException If an item exceeds the bin capacity this exception will be thrown.
     */
    @Override
    public List<Bin<T>> execute(int binCapacity, List<BinItem<T>> binItems) throws ItemExceedsCapacityException {
        bins = new ArrayList<Bin<T>>();
        items = new ArrayList<BinItem<T>>();
        items.addAll(binItems);
        sortItemsInDecreasingOrder(items);

        if(binCapacity > 0){
            this.binCapacity = binCapacity;
        }
        else{
            this.binCapacity = items.get(0).getWeight();
        }

        // Set bin capacity
        bins.add(new Bin<T>(this.binCapacity));

        for(BinItem<T> item : items){
            if(item.getWeight()>this.binCapacity)
                throw new ItemExceedsCapacityException("Item "+ item + " exceeds bin capacity of " + binCapacity + ".");
            assignItemToBin(item);
        }

        return bins;
    }

    /**
     * This method assigns an item to a bin.
     * @param item the given item.
     * @return <tt>true</tt> if item has been assigned.
     */
    private boolean assignItemToBin(BinItem<T> item){
        // Try to assign item to existing bin
        for(Bin<T> bin : bins){
            if(bin.itemFits(item)){
                bin.addItem(item);
                return true;
            }
        }
        // Try to assign item to new bin
        bins.add(new Bin<T>(binCapacity));
        if(bins.get(bins.size()-1).addItem(item)) return true;

        return false;
    }

    public Integer getNumberOfBins() {
        return bins.size();
    }

}
