package model;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class Consumer {
    private String host;//主机地址s
    private int port;
    private String name;
    private Socket sendStringClientSocket = null;
    private Executor executor = Executors.newCachedThreadPool();//线程池;
    private SendString_ReceiveMessage_Runnable sendString_receiveMessage_runnable;

    private MessageQueue<MyMessage> messageQueue;
    /**
     * @param name producer的名字
     * @param host 中间件的连接地址
     * @param port 中间件连接端口号
     */
    public Consumer(String name, String host, int port) {
        this.name = name;
        this.host = host;
        this.port = port;
        receiveMessage();
    }

    private void receiveMessage() {
        try {
            sendStringClientSocket = new Socket(host, port);
            sendString_receiveMessage_runnable=new SendString_ReceiveMessage_Runnable(sendStringClientSocket, new MyMessage(name,
                    "consumer_ask", ""));
            executor.execute(sendString_receiveMessage_runnable);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private class Topic extends MyMessage {
        public Topic(String senderName, String topic_name) {
            super(senderName, "consumer_topic", topic_name);
        }
    }
    private void deleteQueue(){
        try {
            executor.execute(new SendString_Runnable(new Socket(host,port),new MyMessage(name,"consumer_delete_queue","")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void receiveQueue() {
        try {
            executor.execute(new SendString_Runnable(new Socket(host,port),new MyMessage(name,"consumer_queue","")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void receiveTopic(String topicName) {
        try {
            executor.execute(new SendString_Runnable(new Socket(host,port), new Topic(name, topicName)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeConnection() {
        try {
            sendStringClientSocket.shutdownInput();
            sendStringClientSocket.shutdownOutput();
            sendStringClientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class SendString_ReceiveMessage_Runnable implements Runnable {
        private Socket writeImageSocket;
        private MyMessage myMessage;
        private String receiveString;
        private DataOutputStream dos = null;
        private DataInputStream dataInput;

        public SendString_ReceiveMessage_Runnable(Socket socket, MyMessage message) {
            this.myMessage = message;
            this.writeImageSocket = socket;
        }

        public void setMyMessage(MyMessage myMessage) {
            this.myMessage = myMessage;
        }

        public void sendTopic(MyMessage message){
            if (!writeImageSocket.isClosed()) {
                byte[] tmp = myMessage.getBytes();
                try {
                    dos.writeInt(tmp.length);
                    dos.write(tmp, 0, tmp.length);
                    dos.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void run() {
            try {
                dos = new DataOutputStream(writeImageSocket.getOutputStream());
                dataInput = new DataInputStream(writeImageSocket.getInputStream());
                if (!writeImageSocket.isClosed()) {
                    byte[] tmp = myMessage.getBytes();
                    dos.writeInt(tmp.length);
                    dos.write(tmp, 0, tmp.length);
                    dos.flush();
                }
                while (!writeImageSocket.isClosed()) {
                    int size = 0;
                    //客户端接收服务端发送的数据的缓冲区
                    try {
                        size = dataInput.readInt();
                    } catch (IOException e) {
                        writeImageSocket.close();
                        e.printStackTrace();
                    }
                    byte[] data = new byte[size];
                    int len = 0;
                    while (len < size) {
                        len += dataInput.read(data, len, size - len);
                    }
                    ByteArrayOutputStream outPut = new ByteArrayOutputStream();
                    receiveString = new String(data);
//                    System.out.println(System.currentTimeMillis());
                    System.out.println(receiveString);
//                    Thread.sleep(100);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (dos != null) {
                        dos.close();
                        writeImageSocket.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class SendString_Runnable implements Runnable {
        private Socket writeImageSocket;
        private MyMessage myMessage;
        private String receiveString;
        private DataOutputStream dos = null;
        private DataInputStream dataInput;

        public SendString_Runnable(Socket socket, MyMessage message) {
            this.myMessage = message;
            this.writeImageSocket = socket;
        }

        public void setMyMessage(MyMessage myMessage) {
            this.myMessage = myMessage;
        }

        public void run() {
            try {
                dos = new DataOutputStream(writeImageSocket.getOutputStream());
                dataInput = new DataInputStream(writeImageSocket.getInputStream());
                if (!writeImageSocket.isClosed()) {
                    byte[] tmp = myMessage.getBytes();
                    dos.writeInt(tmp.length);
                    dos.write(tmp, 0, tmp.length);
                    dos.flush();
                }
            } catch (IOException  e) {
                e.printStackTrace();
            } finally {
                try {
                    if (dos != null) {
                        dos.close();
                        writeImageSocket.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        Consumer consumer1=new Consumer("ConsumerGR", "172.20.144.193", 8888);
        consumer1.receiveTopic("话题a");
//        consumer1.receiveQueue();
//        consumer1.deleteQueue();//解除对消息队列的监听
//        Consumer consumer2=new Consumer("ConsumerZXP", "172.20.144.193", 8888);
//        consumer2.receiveQueue();

    }
}
