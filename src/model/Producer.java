package model;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class Producer {
    private String host ;//主机地址s
    private int port ;
    private String name;
    private Socket sendStringClientSocket = null;
    private Executor executor = Executors.newCachedThreadPool();//线程池;

    /**
     *
     * @param name   producer的名字
     * @param host  中间件的连接地址
     * @param port  中间件连接端口号
     */
    public Producer(String name,String host,int port) {
        this.name=name;
        this.host=host;
        this.port=port;
    }

    private class Topic extends MyMessage {
        public Topic(String senderName, String topic_name) {
            super(senderName, "producer_topic", topic_name);
        }
    }

    public void  registerTopic(String topicName){
        Topic t = new Topic(this.name, topicName);
        sendMessage(t);
    }
    public void sendMessage(MyMessage message){
        try {
            sendStringClientSocket = new Socket(host, port);
        } catch (IOException e) {
            e.printStackTrace();
        }
        executor.execute(new SendRunnable(sendStringClientSocket,message));
        return;
    }

    private class SendRunnable implements Runnable {
        private Socket writeImageSocket;
        private MyMessage myMessage;

        public SendRunnable(Socket socket, MyMessage message) {
            this.myMessage=message;
            this.writeImageSocket = socket;
        }


        public void run() {
            DataOutputStream dos = null;
            try {
                dos = new DataOutputStream(writeImageSocket.getOutputStream());
                if (writeImageSocket.isConnected()) {
                    byte[] tmp=myMessage.getBytes();
                    dos.writeInt(tmp.length);
                    dos.write(tmp, 0, tmp.length);
                    dos.flush();
                    dos.close();
                }
                writeImageSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (dos != null) dos.close();
                    writeImageSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.exit(0);
        }


    }

    public static void main(String[] args) {
        Producer producer=new Producer( "ProducerGR","172.20.144.193",8888);
//        producer.registerTopic("话题a");
//        MyMessage message = new MyMessage("话题a", "producer_topic_update", "你好asdas");
//        producer.sendMessage(message);
        MyMessage message2 = new MyMessage("ProducerGR", "producer_broadcast", "广播消息1");
        producer.sendMessage(message2);
//        MyMessage message3 = new MyMessage("ProducerGR", "producer_queue", "消息队列消息1");
//        producer.sendMessage(message3);
//        MyMessage message4 = new MyMessage("ProducerGR", "producer_queue", "消息队列消息2");
//        producer.sendMessage(message4);
//        Producer producer2=new Producer( "ProducerZXP","172.20.144.193",8888);
//        MyMessage message5 = new MyMessage("ProducerZXP", "producer_queue", "消息队列消息3");
//        producer2.sendMessage(message5);
//        MyMessage message6 = new MyMessage("ProducerZXP", "producer_queue", "消息队列消息4");
//        producer2.sendMessage(message6);
    }
}
