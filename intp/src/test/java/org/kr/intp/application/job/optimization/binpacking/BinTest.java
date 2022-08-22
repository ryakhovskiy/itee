package org.kr.intp.application.job.optimization.binpacking;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 */
public class BinTest {

    @Test
    public void itemFits_ItemSmallEnough_True() throws Exception {
        Bin<Object[]> bin = new Bin<Object[]>(20);
        BinItem<Object[]> binItem1 = new BinItem(20, new Object[]{3,4,5});
        assertTrue(bin.addItem(binItem1));
    }

    @Test
    public void itemFits_ItemTooBig_False() throws Exception {
        Bin<Object[]> bin = new Bin<Object[]>(20);
        BinItem<Object[]> binItem1 = new BinItem(30, new Object[]{3,4,5});
        assertFalse(bin.addItem(binItem1));
    }

    @Test
    public void addItem_SingleItemTooBig_False() throws Exception {
        Bin<Object[]> bin = new Bin<Object[]>(20);
        BinItem<Object[]> binItem1 = new BinItem(30, new Object[]{3,4,5});
        assertFalse(bin.addItem(binItem1));
    }

    @Test
    public void addItem_SingleItemFits_True() throws Exception {
        Bin<Object[]> bin = new Bin<Object[]>(20);
        BinItem<Object[]> binItem1 = new BinItem(20, new Object[]{3,4,5});
        assertTrue(bin.addItem(binItem1));
    }

    @Test
    public void addItem_MultipleItemsTooBig_OneTrueOneFalse() throws Exception {
        Bin<Object[]> bin = new Bin<Object[]>(20);
        BinItem<Object[]> binItem1 = new BinItem(15, new Object[]{3,4,5});
        BinItem<Object[]> binItem2 = new BinItem(6, new Object[]{3,4,5});
        assertTrue(bin.addItem(binItem1));
        assertFalse(bin.addItem(binItem2));
    }

    @Test
    public void addItem_MultipleItemsFits_TwoTrue() throws Exception {
        Bin<Object[]> bin = new Bin<Object[]>(20);
        BinItem<Object[]> binItem1 = new BinItem(14, new Object[]{3,4,5});
        BinItem<Object[]> binItem2 = new BinItem(6, new Object[]{3,4,5});
        assertTrue(bin.addItem(binItem1));
        assertTrue(bin.addItem(binItem2));
    }

    @Test
    public void removeItem_FullBin20RemoveItem6_14Weight() throws Exception {
        Bin<Object[]> bin = new Bin<Object[]>(20);
        BinItem<Object[]> binItem1 = new BinItem(14, new Object[]{3,4,5});
        BinItem<Object[]> binItem2 = new BinItem(6, new Object[]{3,4,5});
        assertTrue(bin.addItem(binItem1));
        assertTrue(bin.addItem(binItem2));

        bin.removeItem(binItem2);
        assertEquals(14d, bin.getWeight(), 0.01);
    }

    @Test
    public void removeItem_FullBin20RemoveNonExistingItem_20Weight() throws Exception {
        Bin<Object[]> bin = new Bin<Object[]>(20);
        BinItem<Object[]> binItem1 = new BinItem(14, new Object[]{3,4,5});
        BinItem<Object[]> binItem2 = new BinItem(6, new Object[]{3,4,5});
        BinItem<Object[]> binItem3 = new BinItem(7, new Object[]{3,4,5});
        assertTrue(bin.addItem(binItem1));
        assertTrue(bin.addItem(binItem2));

        bin.removeItem(binItem3);
        assertEquals(20d, bin.getWeight(), 0.01);
    }

}