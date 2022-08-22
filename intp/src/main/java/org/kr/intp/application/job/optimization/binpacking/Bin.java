package org.kr.intp.application.job.optimization.binpacking;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class Bin<T> {

    private int capacity;
    private int weight;
    private List<BinItem<T>> items = new ArrayList<BinItem<T>>();


    private Bin(){}

    /**
     * Initializes a bin with a given capacity.
     * @param capacity the bin capacity.
     */
    public Bin(int capacity){
        this.capacity = capacity;
    }

    /**
     * Getter for the bin capacity.
     * @return the bin capacity.
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Getter for the items in the bin.
     * @return the list of bin items.
     */
    public List<BinItem<T>> getItems() {
        return items;
    }

    /**
     * Getter for the current weight of the bin.
     * @return the bin weight.
     */
    public int getWeight() {
        return weight;
    }

    /**
     * Adds the given item to the bin.
     * @param item the item to be added.
     * @return <tt>true</tt> if item has been added.
     */
    public boolean addItem(BinItem<T> item){
        boolean result = false;
        if(itemFits(item)){
            result = items.add(item);
            if(result) weight += item.getWeight();
        }
        return result;
    }

    /**
     * Removes the given iteam of the bin.
     * @param item the item to be removed.
     * @return <tt>true</tt> if item has been removed.
     */
    public boolean removeItem(BinItem<T> item){
        boolean result = false;
        result = items.remove(item);
        if(result){
            if(result) weight -= item.getWeight();
        }
        return result;
    }

    /**
     * Checks whether an item fits in the bin.
     * @param item the given item.
     * @return <tt>true</tt> if item fits.
     */
    public boolean itemFits(BinItem<T> item){
        if(item.getWeight()+weight<=capacity){
            return true;
        }
        return false;
    }
}
