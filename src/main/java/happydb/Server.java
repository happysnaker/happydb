package happydb;

import happydb.common.Database;
import happydb.transport.server.ServerBootStrap;

/**
 * @Author happysnaker
 * @Date 2022/12/6
 * @Email happysnaker@foxmail.com
 */
public class Server {

    public static void main(String[] args) throws InterruptedException {
        Database.run();

        ServerBootStrap.start("127.0.0.1", 2048);
    }
}
