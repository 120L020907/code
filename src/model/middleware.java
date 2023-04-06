package model;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class middleware {
    private ServerSocket serversocket = null;
    private Executor executor = Executors.newCachedThreadPool();//线程池;
    private HashMap<String, ReceiveRunnable> consumer_hashMap = new HashMap();//存储着当前连接着的所有的consumer的socket
    private DBBean dbBean = new DBBean();


    public middleware() {
        try {
            //在本机8888端口创建套接字，等待客户端连接
            serversocket = new ServerSocket(8888);
            while (!serversocket.isClosed()) {
                Socket acceptedSocket_tmp = serversocket.accept();
                System.out.println("连接成功");
                //启动接收客户端数据的线程
                executor.execute(new ReceiveRunnable(acceptedSocket_tmp));
                Thread.sleep(1000);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (serversocket != null) {
                try {
                    serversocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    //服务端接收数据线程
    private class ReceiveRunnable implements Runnable {
        private Socket mySocket;
        private DataInputStream dataInput;
        private DataOutputStream dataOutPut;
        private String message_type;//从收到的Message中提取出消息类型
        private String sender_name;//从收到的Message中提取出发送者的名字
        private Boolean send_little_flag = true;
        private MyMessage myMessage;

        public ReceiveRunnable(Socket s) throws IOException {
            this.mySocket = s;
            //客户端接收服务端发送的数据的缓冲区
            dataInput = new DataInputStream(s.getInputStream());
            dataOutPut = new DataOutputStream(mySocket.getOutputStream());
        }

        /***
         * -判断socket是否还在连接
         * @return
         */
        public Boolean isSockedClosed() {
            return mySocket.isClosed();
        }

        public void setMyMessage(MyMessage myMessage) {
            this.myMessage = myMessage;
        }

        @Override
        public void run() {
            while (!mySocket.isClosed()) {
                try {
                    int size = 0;
                    try {
                        //接收数据
                        size = dataInput.readInt();//获取服务端发送的数据的大小
                    } catch (EOFException e) {
                        //e.printStackTrace();
                        mySocket.close();
                    } catch (SocketException e) {
                        consumer_hashMap.remove(sender_name);//consumer下线
                        mySocket.close();
                    }
                    byte[] data = new byte[size];
                    int len = 0;
                    //将二进制数据写入data数组
                    while (len < size) {
                        len += dataInput.read(data, len, size - len);
                    }
                    if (data.length >= 1) {
                        //获取消息
                        MyMessage message = analysis_receiveData(data);
                        sender_name = message.getSenderName();
                        message_type = message.getMessage_type();
                        //判断消息类型做不同处理
                        if (message_type.equals("consumer_ask")) {
                            //consumer连接
                            addConsumer(sender_name);
                            consumer_hashMap.put(sender_name, this);
                            System.out.println(sender_name + "_consumer已连接");
                            send_clashMessage();
                            //为了保持连接 所以每次发一个小的字符串
                            new Thread(new Runnable() {
                                @Override
                                public void run() {
                                    while (send_little_flag && !mySocket.isClosed()) {
                                        sendMessage(new MyMessage(sender_name + "littleString", "", ""));
                                        try {
                                            Thread.sleep(2000);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            }).start();
                            continue;
                        } else if (message_type.equals("producer_send_database")) {
                            Object o = operateDataBase(message);
                            if (o instanceof Integer) {
                                int value = ((Integer) o).intValue();
                                System.out.println(value);
                            } else {
                                ResultSet rs = (ResultSet) o;
                                ResultSetMetaData resultSetMetaData = rs.getMetaData();
                                int ColumnCount = resultSetMetaData.getColumnCount();
                                while (rs.next()) {
                                    for (int i = 0; i < ColumnCount; i++) {
                                        String s = rs.getString(1 + i);
                                        System.out.print(s + ",");
                                    }
                                    System.out.println();
                                }
                            }
                        } else if (message_type.equals("producer_broadcast")) {
                            //全广播
                            File file = new File("src\\txt\\好友\\注册列表.txt");
                            if (!file.exists()) {
                                file.createNewFile();
                            }
                            FileInputStream fileInputStream = new FileInputStream(file);
                            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                            String text_line;
                            while ((text_line = bufferedReader.readLine()) != null) {
                                System.out.println(text_line);
                                if (consumer_hashMap.containsKey(text_line)) {
                                    consumer_hashMap.get(text_line).sendMessage(new MyMessage(sender_name, "producer_message", message.getMessage_content()));
                                } else {
                                    //如果consumer不在线 将其存入到缓存之中
                                    File file1 = new File("src\\txt\\缓存\\" + text_line + "_缓存.txt");
                                    if (!file1.exists()) {
                                        file1.createNewFile();
                                    }
                                    FileOutputStream clash_fileOutputStream = new FileOutputStream(file1, true);//每次接着原来的文件写
                                    clash_fileOutputStream.write(message.getBytes());
                                    clash_fileOutputStream.write("\r\n".getBytes());
                                    clash_fileOutputStream.flush();
                                    clash_fileOutputStream.close();
                                }
                            }
                        } else if (message_type.equals("consumer_topic")) {
                            getTopic(message);
                        } else if (message_type.equals("producer_topic")) {
                            //Producer注册
                            String object = message.getMessage_content();
                            File file = new File("src\\txt\\好友\\" + object + "的topic注册列表.txt");
                            file.createNewFile();
                        } else if (message_type.equals("producer_topic_update")) {
                            File file = new File("src\\txt\\好友\\" + sender_name + "的topic注册列表.txt");
                            if (!file.exists()) {
                                file.createNewFile();
                            }
                            FileInputStream fileInputStream = new FileInputStream(file);
                            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                            String text_line;
                            while ((text_line = bufferedReader.readLine()) != null) {
                                System.out.println(text_line);
                                if (consumer_hashMap.containsKey(text_line)) {
                                    consumer_hashMap.get(text_line).sendMessage(new MyMessage(sender_name, "producer_message", message.getMessage_content()));
                                } else {
                                    //如果consumer不在线 将其存入到缓存之中
                                    File file1 = new File("src\\txt\\缓存\\" + text_line + "_缓存.txt");
                                    if (!file1.exists()) {
                                        file1.createNewFile();
                                    }
                                    FileOutputStream clash_fileOutputStream = new FileOutputStream(file1, true);//每次接着原来的文件写
                                    clash_fileOutputStream.write(message.getBytes());
                                    clash_fileOutputStream.write("\r\n".getBytes());
                                    clash_fileOutputStream.flush();
                                    clash_fileOutputStream.close();
                                }
                            }
                        }
                    }
                } catch (IOException | SQLException e) {
                    e.printStackTrace();
                }
            }
            try {
                mySocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * 一旦consumer建立连接之后 将其缓存的消息全部发送过去
         */
        private void send_clashMessage() {
            File senderCache = new File("src\\txt\\缓存\\" + sender_name + "_缓存.txt");
            System.out.println("处理缓存消息");
            if (senderCache.exists()) {
                try {
                    FileInputStream fileInputStream = new FileInputStream(senderCache);
                    InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String text = null;
                    while ((text = bufferedReader.readLine()) != null) {
                        System.out.println("text:" + text);
                        MyMessage thisMessage = analysis_StringMessage(text);
                        sendMessage(thisMessage);
                    }
                    FileWriter fileWriter = new FileWriter(senderCache);
                    fileWriter.write("");
                    fileWriter.flush();
                    fileWriter.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        /**
         * 处理consumer发送的获取话题请求
         *
         * @param message 发来的topic
         */

        private void getTopic(MyMessage message) {
            String sender = message.getSenderName();
            String object = message.getMessage_content();
            System.out.println("sender:" + sender);
            System.out.println("object:" + object);
            boolean b = true;
            File file = new File("src\\txt\\好友\\" + object + "的topic注册列表.txt");
            if (!file.exists()) {
                b = false;
                sendMessage(new MyMessage("model.middleware", "error", "no topic"));
            } else {
                try {
                    FileInputStream fileInputStream = new FileInputStream(file);
                    InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String text = null;
                    while ((text = bufferedReader.readLine()) != null) {
                        System.out.println("text:" + text);
                        if (text.equals(sender)) {
                            System.out.println(object + "已和" + sender + "注册了关系");
                            b = false;
                            break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (b) {
                try {
                    String s = sender + "\r\n";
                    byte[] buff = s.getBytes();
                    FileOutputStream o = new FileOutputStream(file, true);
                    o.write(buff);
                    o.flush();
                    o.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        /**
         * 发送消息
         *
         * @param message 发送的消息
         */
        private void sendMessage(MyMessage message) {
            try {
//                System.out.println("准备发送");
                byte[] tmp = message.getBytes();
                dataOutPut.writeInt(tmp.length);
                dataOutPut.write(tmp, 0, tmp.length);
                dataOutPut.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * 解析收到的数据   构造MyMessage类型的对象
         *
         * @param result 缓存的数据
         * @return model.MyMessage
         */
        private MyMessage analysis_StringMessage(String result) {
            String[] senderName = result.split("\\$");
            String senderNameString = senderName[0];
            String[] message_type = senderName[1].split("\\#");
            String message_typeString = null;
            String message_content = null;
            if (message_type.length == 1) {
                message_typeString = message_type[0];
                message_content = "";
            } else if (message_type.length == 2) {
                message_typeString = message_type[0];
                message_content = message_type[1];
            }
            return new MyMessage(senderNameString, message_typeString, message_content);
        }

        /**
         * 解析收到的数据   构造MyMessage类型的对象
         *
         * @param data 收到的数据
         * @return model.MyMessage
         */
        private MyMessage analysis_receiveData(byte[] data) {
            String result = new String(data);
            String[] senderName = result.split("\\$");
            String senderNameString = senderName[0];
            String[] message_type = senderName[1].split("\\#");
            String message_typeString = null;
            String message_content = null;
            if (message_type.length == 1) {
                message_typeString = message_type[0];
                message_content = "";
            } else if (message_type.length == 2) {
                message_typeString = message_type[0];
                message_content = message_type[1];
            }
            return new MyMessage(senderNameString, message_typeString, message_content);
        }
    }

    private void addConsumer(String targetString) {
        String filePath = "src\\txt\\好友\\注册列表.txt";  // 文件路径
        // 读取文件内容并判断是否存在目标字符串
        File file = new File(filePath);
        boolean isTargetStringExist = false;  // 标识目标字符串是否存在
        try (FileReader fileReader = new FileReader(file);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.equals(targetString)) {
                    isTargetStringExist = true;
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 如果目标字符串不存在，则将其写入文件末尾
        if (!isTargetStringExist) {
            try (FileWriter fileWriter = new FileWriter(file, true);
                 BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
                bufferedWriter.newLine();  // 先换行
                bufferedWriter.write(targetString);  // 写入字符串
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 分析并运行MyMessage里要求的函数
     *
     * @param message 传入的信息
     * @return 运行函数得到的结果
     */
    private Object operateDataBase(MyMessage message) {
        String message_content = message.getMessage_content();
        String[] operate = message_content.split("%");
        String[] funcation = operate[1].split("\\(|\\)|,");
        ResultSet resultSet;
        int answer;
        switch (funcation[0]) {
            case "executeDelete":
                answer = dbBean.executeDelete(funcation[1], funcation[2], funcation[3]);
                return answer;
            case "executeUpdate":
                answer = dbBean.executeUpdate(funcation[1], funcation[2], funcation[3], funcation[4], funcation[5]);
                return answer;
            case "executeFind":
                resultSet = dbBean.executeFind(funcation[1], funcation[2], funcation[3]);
                return resultSet;
            case "executeFindAll":
                resultSet = dbBean.executeFindAll(funcation[1]);
                return resultSet;
            case "executeQuery":
                answer = dbBean.executeQuery(funcation[1], funcation[2]);
                return answer;
            case "executeFindMAXID":
                resultSet = dbBean.executeFindMAXID(funcation[1], funcation[2]);
                return resultSet;
            default:
                return null;
        }
    }

    public static void main(String[] args) {
        new middleware();
    }

}
