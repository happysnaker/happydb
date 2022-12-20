package happydb.transport.client;


import happydb.SqlMessage;
import happydb.common.Pair;
import happydb.parser.Parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

/**
 * @author Happysnaker
 * @description
 * @date 2022/5/16
 * @email happysnaker@foxmail.com
 */
public class Shell {

    static SimpleDbClientHandler handler = SimpleDbClientHandler.getInstance();

    /**
     * 简单的 shell 程序，遇到 ';' 执行，并支持 source 命令处理 SQL 文件
     */
    public void run() {
        try (Scanner sc = new Scanner(System.in)) {
            StringBuilder command = new StringBuilder();
            long xid = -1;
            String serverId = null;
            while (true) {
                System.out.print(":> ");
                String str = sc.nextLine();
                if ("exit".equals(str) || "quit".equals(str)) {
                    break;
                } else if (str.startsWith("source")) {
                    String fileName = str.replace("source", "").replace(";", "").trim();
                    try {
                        var p = runFile(fileName, xid, serverId);
                        xid = p.key;
                        serverId = p.val;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    int i = str.indexOf(';');
                    if (i != -1) {
                        command.append(" ").append(str, 0, i);
                        SqlMessage message = handler.sendMessage(
                                SqlMessage.builder()
                                        .message(command.toString())
                                        .serverId(serverId)
                                        .xid(xid).build()
                        );
                        if (message.getError() != null) {
                            System.out.println("Fail to execute command: " + command);
                            System.out.println("Cause by " + message.getError());
                        } else {
                            System.out.println("Ok, " + message.getNumRows() + " rows affected" + " in " + message.getExecutionTime() + "ms");
                            if (message.getMessage() != null
                                    && !message.getMessage().isEmpty()) {
                                System.out.println(message.getMessage());
                            }
                            xid = message.getXid();
                            serverId = message.getServerId();
                        }
                        command = new StringBuilder(str.substring(i + 1));
                    } else {
                        command.append(" ").append(str);
                    }
                }
            }
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            handler.close();
        }
    }

    public Pair<Long, String> runFile(String fileName, long xid, String serverId) throws Exception {
        File file = new File(fileName);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        StringBuilder command = new StringBuilder();
        String str = null;
        while ((str = reader.readLine()) != null) {
            int i = str.indexOf(';');
            if (i != -1) {
                command.append(" ").append(str, 0, i);
                SqlMessage message = handler.sendMessage(
                        SqlMessage.builder()
                                .message(command.toString())
                                .serverId(serverId)
                                .xid(xid).build()
                );
                if (message.getError() != null && !message.getError().isEmpty()) {
                    System.out.println("fail to execute command " + command);
                    System.out.println("Cause by " + message.getError());
                } else {
                    System.out.print("Ok, " + message.getNumRows() + " affected");
                    System.out.println(" in " + message.getExecutionTime() + "ms");
                    if (message.getMessage() != null
                            && !message.getMessage().isEmpty()) {
                        System.out.println(message.getMessage());
                    }
                    xid = message.getXid();
                    serverId = message.getServerId();
                }
                command = new StringBuilder(str.substring(i + 1));
            } else {
                command.append(" ").append(str);
            }
        }
        return Pair.create(xid, serverId);
    }
}
