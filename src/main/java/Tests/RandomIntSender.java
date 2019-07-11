package Tests;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;

public class RandomIntSender {
    public static void main(String[] args) {

        //        System.out.println(saySomething());
        // TODO 增加 HTTP 服务端，实现分辨率、帧率的客户端读取
        // 能设置更好
        int serverPort = 9999;
        try {
            ServerSocket ss = new ServerSocket(serverPort);
            System.out.println("启动服务器....");
            while (true) {
                System.out.println("等待连接");
                Socket s = ss.accept();
                System.out.println("客户端:" + s.getInetAddress().getLocalHost() + "已连接到服务器");

                BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
                //读取客户端发送来的消息
                //System.out.println("[Receive]:");
                // 这里不能尝试读取参数，根本不会发过来，只会阻塞进程。
//                String propLine = br.readLine();
//                System.out.println(propLine==null?"[empty]":propLine);
//                String[] props = propLine.split(",");
//                int width = Integer.parseInt(props[0]);
//                int height = Integer.parseInt(props[1]);
                System.out.println("Sending...");
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
                while (true) {
                    try {
                        Date date = new Date();
                        //SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                        String num = String.valueOf((int)(1+Math.random()*(10-1+1)));
                        bw.write(num + "\n");
                        //bw.write(dateFormat.format(date)+","+ num + "\n");
                        bw.flush();
                        Thread.sleep(10);
                    } catch (Exception e) {
                        // 连接被客户端中断了
                        System.out.println(e.getLocalizedMessage());
                        break;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
