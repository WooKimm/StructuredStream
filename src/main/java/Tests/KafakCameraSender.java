package Tests;

import Util.ImageViewer;
import Util.StringUtil;
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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.imageio.ImageIO;

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
                //String s = getFrame();
                Frame frame = grabber.grab();
                BufferedImage bimg = frameToImage(frame);
                viewer.showImage(bimg);

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ImageIO.write(bimg, "jpg", out);
                byte[] bs = out.toByteArray();
                String img = StringUtil.bytesToHexString(bs);

                long timestamp = System.currentTimeMillis();
                img = img + "," + timestamp;
                producer.send(new ProducerRecord<String, String>("test1", img));

                Thread.sleep(1000/Property.FPS);
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }
    }
}
