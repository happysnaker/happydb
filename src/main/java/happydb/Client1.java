package happydb;

import happydb.transport.client.ClientBootStrap;

/**
 * @Author happysnaker
 * @Date 2022/12/6
 * @Email happysnaker@foxmail.com
 */
public class Client1 {
    public static void main(String[] args) throws Exception {
        var s = """
                    CREATE TABLE `a` (
                        x int,
                        y double,
                        z char,
                        PRIMARY KEY(x) USING BTREE
                        );
                """;
        ClientBootStrap.start(2048);
    }
}
