package Tests;


import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

import Util.ImageViewer;
import Util.StringUtil;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.Java2DFrameConverter;
import org.bytedeco.javacv.OpenCVFrameGrabber;
import org.opencv.core.Mat;

import javax.imageio.ImageIO;

public class CameraSender {
    private static int count = 0;
    public static OpenCVFrameGrabber grabber;
    static ImageViewer viewer = new ImageViewer("Server");

    static {
        try {
            grabber = new OpenCVFrameGrabber(0);
            grabber.start();   //开始获取摄像头数据

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("调用摄像头失败！");
        }
    }


    private static Image toBufferedImage(Mat matrix) {
        int type = BufferedImage.TYPE_BYTE_GRAY;
        if (matrix.channels() > 1) {
            type = BufferedImage.TYPE_3BYTE_BGR;
        }
        int bufferSize = matrix.channels() * matrix.cols() * matrix.rows();
        byte[] buffer = new byte[bufferSize];
        matrix.get(0, 0, buffer); // 获取所有的像素点
        BufferedImage image = new BufferedImage(matrix.cols(), matrix.rows(), type);
        final byte[] targetPixels = ((DataBufferByte) image.getRaster().getDataBuffer()).getData();
        System.arraycopy(buffer, 0, targetPixels, 0, buffer.length);

        return image;
    }

    public static BufferedImage frameToImage(Frame frame) {
        Java2DFrameConverter converter = new Java2DFrameConverter();

        BufferedImage bufferedImage = converter.getBufferedImage(frame);
        return bufferedImage;
    }

    public static String getFrame() {
        try {
            Frame frame = grabber.grab();
            BufferedImage bimg = frameToImage(frame);
            int[] arr = new int[Property.WIDTH * Property.HEIGHT];
            //int[][] data = new int[Property.WIDTH][Property.HEIGHT];
            StringBuilder sb = new StringBuilder();

            int startX = (bimg.getWidth() - Property.WIDTH) / 2;
            int startY = (bimg.getHeight() - Property.HEIGHT) / 2;
            arr = bimg.getRGB(startX, startY, Property.WIDTH, Property.HEIGHT, arr, 0, Property.WIDTH);
            //viewer.showImage(arr);
            for (int i : arr) {
                sb.append(i + ";");
            }
            System.out.println("Send image:[" + bimg.getWidth() + ", " + bimg.getHeight() + "]");
//            Raster raster = bimg.getData();
            return sb.toString();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("抓取图像失败！");
            return null;
        }

    }

    public static void main(String[] args) {
//        System.out.println(saySomething());
        // TODO 增加 HTTP 服务端，实现分辨率、帧率的客户端读取
        // 能设置更好

        try {
            ServerSocket ss = new ServerSocket(Property.serverPort);
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
                        Frame frame = grabber.grab();
                        BufferedImage bimg = frameToImage(frame);
                        viewer.showImage(bimg);

                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        ImageIO.write(bimg, "jpg", out);
                        byte[] bs = out.toByteArray();

                        String temp = StringUtil.bytesToHexString(bs);
                        //bw.write(bs);
                        long timestamp = System.currentTimeMillis();
                        temp = temp + "," + timestamp;
                        bw.write(temp + "\n");
                        bw.flush();

                        Thread.sleep(1000 / Property.FPS);
                        /*
                        System.out.println("send a message.");
                        String img = getFrame();
                        viewer.showImage(img.split(";"));
                        bw.write(img + "\n");
                        bw.flush();
                        Thread.sleep(1000 / Property.FPS);
                        */
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
