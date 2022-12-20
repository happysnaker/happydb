package happydb.common;

import com.sun.source.tree.IfTree;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.fail;

/**
 * @Author happysnaker
 * @Date 2022/11/15
 * @Email happysnaker@foxmail.com
 */
public class ByteArrayTest {
    ByteArray byteAr;
    ByteList byteList;

    int valsSize = 2500;

    int[] intVals = new int[valsSize];
    double[] doubleVals = new double[valsSize];
    String[] stringVals = new String[valsSize];

    Random random = new Random();

    public void writeData(ByteArray ba) {
        for (int intVal : intVals) {
            ba.writeInt(intVal);
        }
        for (double doubleVal : doubleVals) {
            ba.writeDouble(doubleVal);
        }
        for (String stringVal : stringVals) {
            ba.writeString(stringVal);
        }
    }

    @Before
    public void setUp() {
        Assert.assertTrue(valsSize > 0);
        int totalSize = valsSize * (Integer.BYTES + Double.BYTES);
        for (int i = 0; i < valsSize; i++) {
            intVals[i] = random.nextInt();
            doubleVals[i] = random.nextDouble();
            stringVals[i] = UUID.randomUUID().toString();
            totalSize += stringVals[i].getBytes(StandardCharsets.UTF_8).length;
        }

        byteAr = ByteArray.allocate(totalSize);
        byteList = new ByteList();
        writeData(byteAr);
        writeData(byteList);
        Assert.assertEquals(byteAr.length(), totalSize);
        Assert.assertEquals(byteList.length(), totalSize);
    }


    @Test
    public void testReadWrite() {
        for (int intVal : intVals) {
            Assert.assertEquals(intVal, byteAr.readInt());
            Assert.assertEquals(intVal, byteList.readInt());
        }
        for (double doubleVal : doubleVals) {
            Assert.assertEquals(doubleVal, byteAr.readDouble(), 0);
            Assert.assertEquals(doubleVal, byteList.readDouble(), 0);
        }
        for (String stringVal : stringVals) {
            Assert.assertEquals(stringVal, byteAr.readString(stringVal.getBytes(StandardCharsets.UTF_8).length));
            Assert.assertEquals(stringVal, byteList.readString(stringVal.getBytes(StandardCharsets.UTF_8).length));
        }

        byteAr.rewindReadPos();
        byteList.rewindReadPos();

        for (int intVal : intVals) {
            Assert.assertEquals(intVal, byteAr.readInt());
            Assert.assertEquals(intVal, byteList.readInt());
        }
        for (double doubleVal : doubleVals) {
            Assert.assertEquals(doubleVal, byteAr.readDouble(), 0);
            Assert.assertEquals(doubleVal, byteList.readDouble(), 0);
        }
        for (String stringVal : stringVals) {
            Assert.assertEquals(stringVal, byteAr.readString(stringVal.getBytes(StandardCharsets.UTF_8).length));
            Assert.assertEquals(stringVal, byteList.readString(stringVal.getBytes(StandardCharsets.UTF_8).length));
        }

        byte[] bytes = new byte[10000];
        ByteArray ba = new ByteArray(bytes);

        ByteArray subArray = byteAr.subArray(0, 200);
        ba.update(200, subArray);
        for (int i = 0; i < 200; i++) {
            ba.readByte();
        }
        Assert.assertEquals(subArray, ba.readByteArray(200));
    }


    @Test
    public void testEquals() {
        Assert.assertEquals(byteAr, byteList);
        Assert.assertEquals(byteAr.subArray(valsSize), byteList.subArray(valsSize));
        Assert.assertEquals(byteAr.subArray(0, valsSize), byteList.subArray(0, valsSize));
        Assert.assertEquals(new ByteArray(25L), new ByteList().writeLong(25L));
        Assert.assertNotEquals(new ByteArray(25L), new ByteList().writeLong(52L));
        Assert.assertEquals(new ByteArray(25.5), new ByteList().writeDouble(25.5));
        Assert.assertNotEquals(new ByteArray(25.5), new ByteList().writeDouble(25.55));
        Assert.assertEquals(new ByteArray("Hello World!"), new ByteList().writeString("Hello World!"));
        Assert.assertEquals(new ByteArray(byteAr, byteList), byteList.writeByteArray(byteAr));
        Assert.assertEquals(new ByteArray(byteAr, byteList), new ByteList().writeByteArray(byteAr).writeByteArray(byteList));
    }


    @Test
    public void testReadWriteOffset() {
        int offset = 0;
        for (int intVal : intVals) {
            Assert.assertEquals(intVal, byteAr.readInt(offset));
            Assert.assertEquals(intVal, byteList.readInt(offset));
            offset += Integer.BYTES;
        }
        for (double doubleVal : doubleVals) {
            Assert.assertEquals(doubleVal, byteAr.readDouble(offset), 0);
            Assert.assertEquals(doubleVal, byteList.readDouble(offset), 0);
            offset += Double.BYTES;
        }
        for (String stringVal : stringVals) {
            int length = stringVal.getBytes(StandardCharsets.UTF_8).length;
            Assert.assertEquals(stringVal, byteAr.readString(offset, length));
            Assert.assertEquals(stringVal, byteList.readString(offset, length));
            offset += length;
        }
        int totalSize = offset;
        for (int i = 0; i < valsSize; i++) {
            offset = random.nextInt(totalSize - Long.BYTES);
            long val = random.nextLong();
            byteList.update(offset, val);
            byteAr.update(offset, new ByteArray(val));
            Assert.assertEquals(val, byteList.readLong(offset));
            Assert.assertEquals(val, byteAr.readLong(offset));
        }

        Assert.assertEquals(byteList, byteAr);

        byteList.update(0, new ByteArray(250f));
        byteAr.update(0, byteList);
        Assert.assertEquals(byteList, byteAr);
        Assert.assertEquals(250F, byteAr.readDouble(0), 0);
        Assert.assertEquals(250F, byteList.readDouble(0), 0);
    }


    @Test
    public void testSubList() {
        Assert.assertSame(byteAr.getRawByteArray(), byteAr.getRawByteArray());
        Assert.assertSame(byteAr.getRawByteArray(), byteAr.subArray(0, valsSize).getRawByteArray());
        Assert.assertSame(byteList.getRawByteArray(), byteList.getRawByteArray());
        Assert.assertSame(byteList.getRawByteArray(), byteList.subArray(0, valsSize).getRawByteArray());

        ByteArray subArray = byteAr.subArray(0, intVals.length * Integer.BYTES);
        for (int i = 0; i < intVals.length; i++) {
            subArray.writeInt(i);
            Assert.assertEquals(i, subArray.readInt());
            Assert.assertEquals(i, byteAr.readInt());
        }
        Assert.assertEquals(intVals.length * Integer.BYTES, byteAr.getReadPos());


        byte[] bytes = byteAr.getRawByteArray();
        ByteArray byteArray = new ByteArray(bytes);
        byteArray.update(0, UUID.randomUUID().toString());
        Assert.assertTrue(byteArray.equals(bytes));
    }


    @Test
    public void testIndexOutOfBound() {
        byte[] bytes = new byte[1024];
        var ba = new ByteArray(bytes, 128, 256);
        int length = ba.length();
        Assert.assertEquals(256 - 128, length);
        try {
            ba.update(length, 1);
            fail("should out of the bound");
        } catch (IndexOutOfBoundsException | NoSuchElementException ignore) {
            // ignore
        }

        try {
            ba.readInt(length - 3);
            fail("should out of the bound");
        } catch (IndexOutOfBoundsException | NoSuchElementException ignore) {
            // ignore
        }

        try {
            ba.set(255, (byte) 0x1);
            fail("should out of the bound");
        } catch (IndexOutOfBoundsException | NoSuchElementException ignore) {
            // ignore
        }
    }
}
