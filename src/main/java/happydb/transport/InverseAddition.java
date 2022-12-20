package happydb.transport;

/**
 * 反码加法检验和
 * @author Happysnaker
 * @description
 * @date 2022/3/24
 * @email happysnaker@foxmail.com
 */
public class InverseAddition {
    private static int inverseSummation(int b1, int b2) {
        int s = b1 + b2;
        if (s > 0xff) {
            return inverseSummation(s & 0xff, 0x1);
        }
        return s;
    }

    public static byte inverseSummation(byte b1, byte b2) {
        int i1 = b1 & 0xff;
        int i2 = b2 & 0xff;
        int i = inverseSummation(i1, i2);
        return (byte) i;
    }

    public static byte inverseSummation(byte b, int i) {
        byte checkSum = b;
        checkSum = inverseSummation(checkSum, (byte) (i & 0xff));
        checkSum = inverseSummation(checkSum, (byte) ((i >> 8) & 0xff));
        checkSum = inverseSummation(checkSum, (byte) ((i >> 16) & 0xff));
        checkSum = inverseSummation(checkSum, (byte) ((i >> 24) & 0xff));
        return checkSum;
    }
}
