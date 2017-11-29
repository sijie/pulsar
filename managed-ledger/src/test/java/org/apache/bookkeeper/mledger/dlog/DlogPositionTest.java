package org.apache.bookkeeper.mledger.dlog;

import org.apache.bookkeeper.mledger.dlog.DlogBasedPosition;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;
import org.apache.distributedlog.DLSN;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class DlogPositionTest {
    @Test(expectedExceptions = NullPointerException.class)
    public void nullParam() {
        new DlogBasedPosition((DLSN) null);
    }

    @Test
    public void simpleTest() {
        DlogBasedPosition pos = new DlogBasedPosition(1, 2, 3);

        assertEquals(pos, new DlogBasedPosition(1, 2, 3));
        assertEquals(pos, new DlogBasedPosition(new DLSN(1, 2, 3)));
        assertFalse(pos.equals(new DlogBasedPosition(1, 3, 2)));
        assertFalse(pos.equals(new DlogBasedPosition(3, 2, 4)));
        assertFalse(pos.equals("1:2"));
    }

    @Test
    public void comparisons() {
        DlogBasedPosition pos1_1_2 = new DlogBasedPosition(1, 1, 2);
        DlogBasedPosition pos10_0_1 = new DlogBasedPosition(10, 0, 1);
        DlogBasedPosition pos10_5_5 = new DlogBasedPosition(10, 5, 5);
        DlogBasedPosition pos10_5_6 = new DlogBasedPosition(10, 5, 6);

        assertEquals(0, pos1_1_2.compareTo(pos1_1_2));
        assertEquals(-1, pos1_1_2.compareTo(pos10_0_1));
        assertEquals(-1, pos1_1_2.compareTo(pos10_5_5));
        assertEquals(-1, pos1_1_2.compareTo(pos10_5_6));

        assertEquals(+1, pos10_0_1.compareTo(pos1_1_2));
        assertEquals(0, pos10_0_1.compareTo(pos10_0_1));
        assertEquals(-1, pos10_0_1.compareTo(pos10_5_5));
        assertEquals(-1, pos10_0_1.compareTo(pos10_5_6));

        assertEquals(+1, pos10_5_5.compareTo(pos1_1_2));
        assertEquals(+1, pos10_5_5.compareTo(pos10_0_1));
        assertEquals(0, pos10_5_5.compareTo(pos10_5_5));
        assertEquals(-1, pos10_5_5.compareTo(pos10_5_6));

        assertEquals(+1, pos10_5_6.compareTo(pos1_1_2));
        assertEquals(+1, pos10_5_6.compareTo(pos10_0_1));
        assertEquals(+1, pos10_5_6.compareTo(pos10_5_5));
        assertEquals(0, pos10_5_6.compareTo(pos10_5_6));

    }

    @Test
    public void hashes() throws Exception {
        DlogBasedPosition p1 = new DlogBasedPosition(5, 15, 6);
        DlogBasedPosition p2 = new DlogBasedPosition(DLSN.deserializeBytes(p1.getDlsn().serializeBytes()));
        assertEquals(p2.getDlsn(), new DLSN(5, 15, 6));
        assertEquals(new DlogBasedPosition(5, 15, 6).hashCode(), p2.hashCode());
    }
}
