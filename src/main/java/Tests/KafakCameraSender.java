package Tests;

import Util.ImageViewer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.Java2DFrameConverter;
import org.bytedeco.javacv.OpenCVFrameGrabber;
import org.opencv.core.Mat;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafakCameraSender {
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

    public static BufferedImage frameToImage(org.bytedeco.javacv.Frame frame) {
        Java2DFrameConverter converter = new Java2DFrameConverter();

        BufferedImage bufferedImage = converter.getBufferedImage(frame);
        return bufferedImage;
    }

    public static String getFrame() {
        try {
            Frame frame = grabber.grab();
            BufferedImage bimg = frameToImage(frame);
            int[] arr = new int[Property.WIDTH * Property.HEIGHT];
            int[][] data = new int[Property.WIDTH][Property.HEIGHT];
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

        Map<String,Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
            while (true){
                String img = getFrame();
                viewer.showImage(img.split(";"));
                producer.send(new ProducerRecord<String, String>("test2", img));
                Thread.sleep(1000 / Property.FPS);
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }
    }
}
