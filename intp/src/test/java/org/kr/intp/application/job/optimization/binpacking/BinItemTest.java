package org.kr.intp.application.job.optimization.binpacking;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 */
public class BinItemTest {

    @Test
    public void equals_BinItemsWithSameContent_True() throws Exception {
        BinItem<Object[]> binItem1 = new BinItem(30, new Object[]{3,4,5});
        BinItem<Object[]> binItem2 = new BinItem(30, new Object[]{3,4,5});
        boolean result = binItem1.equals(binItem2);
        assertTrue(binItem1.equals(binItem2));
    }

    @Test
    public void equals_WeightSameItemDifferent_False() throws Exception {
        BinItem<Object[]> binItem1 = new BinItem(30, new Object[]{3,4,5});
        BinItem<Object[]> binItem2 = new BinItem(30, new Object[]{3,4,6});
        assertFalse(binItem1.equals(binItem2));
    }

    @Test
    public void equals_WeightDifferentItemSame_False() throws Exception {
        Object[] o = new Object[]{3,4,5};
        BinItem<Object[]> binItem1 = new BinItem(29, o);
        BinItem<Object[]> binItem2 = new BinItem(30, o);
        assertFalse(binItem1.equals(binItem2));
    }
}