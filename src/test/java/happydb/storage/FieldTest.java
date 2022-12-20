package happydb.storage;

import happydb.common.ByteArray;
import happydb.common.ByteList;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;

import static happydb.execution.Predicate.OP_MAP;
import static happydb.storage.Type.*;

/**
 * @Author happysnaker
 * @Date 2022/11/16
 * @Email happysnaker@foxmail.com
 */
public class FieldTest {

    @Test
    public void testIntField() throws ParseException {
        ByteArray ba = new ByteList()
                .writeByteArray(new happydb.storage.IntField(0).serialized())
                .writeByteArray(new happydb.storage.IntField(1).serialized())
                .writeByteArray(new happydb.storage.IntField(1).serialized());
        Field v1 = INT_TYPE.parse(ba);
        Field v2 = INT_TYPE.parse(ba);
        Field v3 = INT_TYPE.parse(ba);

        Assert.assertEquals((int) v1.getObject(), 0);
        Assert.assertEquals((int) v2.getObject(), 1);
        Assert.assertEquals((int) v3.getObject(), 1);

        Assert.assertTrue(v1.compare(OP_MAP.get("<="), v2));
        Assert.assertTrue(v1.compare(OP_MAP.get("<"), v2));
        Assert.assertTrue(v1.compare(OP_MAP.get("<="), v3));
        Assert.assertTrue(v2.compare(OP_MAP.get("<="), v3));
        Assert.assertTrue(v3.compare(OP_MAP.get("<="), v2));
        Assert.assertTrue(v3.compare(OP_MAP.get("LIKE"), v2));
        Assert.assertFalse(v1.compare(OP_MAP.get("="), v2));
        Assert.assertFalse(v1.compare(OP_MAP.get("="), v3));
        Assert.assertFalse(v1.compare(OP_MAP.get(">"), v3));
        Assert.assertFalse(v1.compare(OP_MAP.get("LIKE"), v3));
        Assert.assertFalse(v2.compare(OP_MAP.get("<"), v3));
        Assert.assertFalse(v2.compare(OP_MAP.get(">"), v3));
    }


    @Test
    public void testDoubleField() throws ParseException {
        ByteArray ba = new ByteList()
                .writeByteArray(new happydb.storage.DoubleField(1.2f).serialized())
                .writeByteArray(new happydb.storage.DoubleField(1.3f).serialized())
                .writeByteArray(new happydb.storage.DoubleField(1.3f).serialized());
        Field v1 = DOUBLE_TYPE.parse(ba);
        Field v2 = DOUBLE_TYPE.parse(ba);
        Field v3 = DOUBLE_TYPE.parse(ba);

        Assert.assertEquals((Double) v1.getObject(), 1.2f, 0f);
        Assert.assertEquals((Double) v2.getObject(), 1.3f, 0f);
        Assert.assertEquals((Double) v3.getObject(), 1.3f, 0f);

        Assert.assertTrue(v1.compare(OP_MAP.get("<="), v2));
        Assert.assertTrue(v1.compare(OP_MAP.get("<"), v2));
        Assert.assertTrue(v1.compare(OP_MAP.get("<="), v3));
        Assert.assertTrue(v2.compare(OP_MAP.get("<="), v3));
        Assert.assertTrue(v3.compare(OP_MAP.get("<="), v2));
        Assert.assertTrue(v3.compare(OP_MAP.get("LIKE"), v2));
        Assert.assertFalse(v1.compare(OP_MAP.get("="), v2));
        Assert.assertFalse(v1.compare(OP_MAP.get("="), v3));
        Assert.assertFalse(v1.compare(OP_MAP.get(">"), v3));
        Assert.assertFalse(v1.compare(OP_MAP.get("LIKE"), v3));
        Assert.assertFalse(v2.compare(OP_MAP.get("<"), v3));
        Assert.assertFalse(v2.compare(OP_MAP.get(">"), v3));
    }


    @Test
    public void testStringField() throws ParseException {

        ByteArray ba = new ByteList()
                .writeByteArray(new happydb.storage.StringField("abc").serialized())
                .writeByteArray(new happydb.storage.StringField("abd").serialized())
                .writeByteArray(new happydb.storage.StringField("abd").serialized());
        Field v1 = STRING_TYPE.parse(ba);
        Field v2 = STRING_TYPE.parse(ba);
        Field v3 = STRING_TYPE.parse(ba);

        Assert.assertEquals(v1.getObject(), "abc");
        Assert.assertEquals(v2.getObject(), "abd");
        Assert.assertEquals(v3.getObject(), "abd");

        Assert.assertTrue(v1.compare(OP_MAP.get("<="), v2));
        Assert.assertTrue(v1.compare(OP_MAP.get("<"), v2));
        Assert.assertTrue(v1.compare(OP_MAP.get("<="), v3));
        Assert.assertTrue(v2.compare(OP_MAP.get("<="), v3));
        Assert.assertTrue(v3.compare(OP_MAP.get("<="), v2));
        Assert.assertTrue(v3.compare(OP_MAP.get("LIKE"), new StringField("bd")));
        Assert.assertFalse(v1.compare(OP_MAP.get("="), v2));
        Assert.assertFalse(v1.compare(OP_MAP.get("="), v3));
        Assert.assertFalse(v1.compare(OP_MAP.get(">"), v3));
        Assert.assertFalse(v1.compare(OP_MAP.get("LIKE"), v3));
        Assert.assertFalse(v2.compare(OP_MAP.get("<"), v3));
        Assert.assertFalse(v2.compare(OP_MAP.get(">"), v3));
    }
}
