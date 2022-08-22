package org.kr.intp.application.job.optimization.binpacking;

import java.util.Arrays;

/**
 * This class represents a bin item which has a specific weight.
 */
public class BinItem<T> {

    private int weight;
    private T item;

    private BinItem(){}

    /**
     * Constructor for a BinItem.
     * @param weight the weight of the item.
     * @param item the item itself.
     */
    public BinItem(int weight, T item){
        this.weight = weight;
        this.item = item;
    }

    /**
     * Getter for the weight.
     * @return the weight.
     */
    public int getWeight() {
        return weight;
    }

    /**
     * Setter for the weight.
     * @param weight the weight of the item.
     */
    public void setWeight(int weight) {
        this.weight = weight;
    }

    /**
     * Getter for the bin item.
     * @return the bin item.
     */
    public T getItem() {
        return item;
    }

    /**
     * Setter for the bin item.
     * @param item the bin item.
     */
    public void setItem(T item) {
        this.item = item;
    }

    @Override
    public String toString() {
        return "BinItem{" +
                "weight=" + weight +
                ", item=" + item +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BinItem<?> binItem = (BinItem<?>) o;

        if (Double.compare(binItem.weight, weight) != 0) return false;
        // Since the equal method of an array checks for references, use the Arrays.equals method if the object is an array
        if(item instanceof Object[]&& ((BinItem<?>) o).getItem() instanceof Object[])
            return Arrays.equals((Object[])this.item, (Object[]) ((BinItem<?>) o).getItem());
        return item.equals(binItem.item);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(weight);
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + item.hashCode();
        return result;
    }
}
