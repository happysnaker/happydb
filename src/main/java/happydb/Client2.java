package happydb;

import happydb.transport.client.ClientBootStrap;

/**
 * @Author happysnaker
 * @Date 2022/12/6
 * @Email happysnaker@foxmail.com
 */
public class Client2 {
    public static void main(String[] args) throws Exception {
        ClientBootStrap.start(2048);
    }
}
