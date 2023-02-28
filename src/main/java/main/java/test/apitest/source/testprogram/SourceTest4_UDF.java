package main.java.test.apitest.source.testprogram;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.CloseableIterator;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Random;

public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Sensor> dataStream = env.addSource( new MysensorSource());
        dataStream.print("result");
        env.execute();
    }

    public static class MysensorSource implements SourceFunction<Sensor>{
        private boolean flag = true;
        @Override
        public void run(SourceContext<Sensor> sourceContext) throws Exception {
            Random random = new Random();
            HashMap<String, Double> map = new HashMap<>();
            for(int i =0; i< 10; i++){
                map.put("sensor_"+(i+1), 60 + random.nextGaussian() * 20);
            }

            while (flag){
                for(String sensorId : map.keySet()){
                    String info = null;
                    try {
                        //创建一个socket对象，8888为监听端口
                        System.out.println("heihei2");
                        ServerSocket s = new ServerSocket(8888);
                        System.out.println("heihei");
                        //socket对象调用accept方法，等待连接请求
                        Socket s1 = s.accept();
                        //打开输出流
                        OutputStream os = s1.getOutputStream();
                        //封装输出流
                        DataOutputStream dos = new DataOutputStream(os);
                        //打开输入流
                        InputStream is = s1.getInputStream();
                        //封装输入流
                        DataInputStream dis = new DataInputStream(is);
                        //读取键盘输入流
                        InputStreamReader isr = new InputStreamReader(System.in);
                        //封装键盘输入流
                        BufferedReader br = new BufferedReader(isr);
                        boolean flag = true;
                        while (flag) {
                            //接受客户端发送过来的信息
                            info = dis.readUTF();
                            //打印接受的信息
                            flag = false;
                            System.out.println("对方说: " + info);
                            //如果发现接受的信息为：bye，那么就结束对话
                            if (info.equals("bye"))
                                break;
                            //读取键盘的输入流
                            info = br.readLine();
                            //写入到网络连接的另一边，即客户端
                            dos.writeUTF(info);
                            //如果服务器自己说：bye，也是结束对话
                            if (info.equals("bye"))
                                break;
                        }
                        //关闭输入流
                        dis.close();
                        //关闭输出流
                        dos.close();
                        //关闭socket对象
                        s1.close();
                        s.close();
                    } catch (Exception e) {
                        System.out.println("网络连接异常，程序退出!");
                    }
                        String[] strings = info.split(" ");
                        map.put(strings[0], Double.valueOf(strings[1]));
//                    Double newtemp = map.get(sensorId)+ random.nextGaussian();
//                    map.put(sensorId, newtemp);
//                    System.out.println(System.currentTimeMillis());
                    sourceContext.collect(new Sensor(sensorId, System.currentTimeMillis(), map.get(sensorId)));
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

    public static String getDataFromSocket(){
        String info = null;
        try {
            //创建一个socket对象，8888为监听端口
            System.out.println("heihei2");
            ServerSocket s = new ServerSocket(8888);
            System.out.println("heihei");
            //socket对象调用accept方法，等待连接请求
            Socket s1 = s.accept();
            //打开输出流
            OutputStream os = s1.getOutputStream();
            //封装输出流
            DataOutputStream dos = new DataOutputStream(os);
            //打开输入流
            InputStream is = s1.getInputStream();
            //封装输入流
            DataInputStream dis = new DataInputStream(is);
            //读取键盘输入流
            InputStreamReader isr = new InputStreamReader(System.in);
            //封装键盘输入流
            BufferedReader br = new BufferedReader(isr);
            boolean flag = true;
            while (flag) {
                //接受客户端发送过来的信息
                info = dis.readUTF();
                //打印接受的信息
                flag = false;
                System.out.println("对方说: " + info);
                //如果发现接受的信息为：bye，那么就结束对话
                if (info.equals("bye"))
                    break;
                //读取键盘的输入流
                info = br.readLine();
                //写入到网络连接的另一边，即客户端
                dos.writeUTF(info);
                //如果服务器自己说：bye，也是结束对话
                if (info.equals("bye"))
                    break;
            }
            //关闭输入流
            dis.close();
            //关闭输出流
            dos.close();
            //关闭socket对象
            s1.close();
            s.close();
        } catch (Exception e) {
            System.out.println("网络连接异常，程序退出!");
        }
        return info;
    }
}
