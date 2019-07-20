package Util;

import Tests.Property;
import org.apache.calcite.util.Static;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.*;

public class VideoWriter extends ForeachWriter<Row>{
        public static long lastTime = 0;
        ImageViewer viewer = new ImageViewer("ImageViewer");

        @Override
        public boolean open(long partitionId, long epochId)
        {
            return true;
        }

        @Override
        public void process(Row value){
            String str = value.get(0).toString();

            byte[] bimg = StringUtil.hexStringToBytes(str);

            InputStream buffin = new ByteArrayInputStream(bimg);
            BufferedImage img = null;
            try {
                img = ImageIO.read(buffin);
            } catch (IOException e) {
                e.printStackTrace();
            }

            //图像处理函数
            /*
            List<String> strImg = new ArrayList<>();
            int[] arr = new int[Property.WIDTH * Property.HEIGHT];
            int startX = (img.getWidth() - Property.WIDTH) / 2;
            int startY = (img.getHeight() - Property.HEIGHT) / 2;
            arr = img.getRGB(startX, startY, Property.WIDTH, Property.HEIGHT, arr, 0, Property.WIDTH);
            //viewer.showImage(arr);
            for (int i : arr) {
                strImg.add(i+"");
            }
            String[] processedImg = new String[strImg.size()];
            for(int i = 0; i < strImg.size(); ++i)
            {
                processedImg[i] = strImg.get(i);
            }
            processedImg = ImageProcessUtil.toGray(processedImg);
            processedImg = ImageProcessUtil.watermark(processedImg);
            */
            int[] arr = new int[Property.WIDTH * Property.HEIGHT];
            //int[][] data = new int[Property.WIDTH][Property.HEIGHT];

            int startX = (img.getWidth() - Property.WIDTH) / 2;
            int startY = (img.getHeight() - Property.HEIGHT) / 2;
            arr = img.getRGB(startX, startY, Property.WIDTH, Property.HEIGHT, arr, 0, Property.WIDTH);
            BufferedImage image = ImageProcessUtil.toBnW(arr);

            viewer.showImage(image);


            if(value.length() == 2 && !((Long) lastTime).equals(0))
            {
                long currentTime = System.currentTimeMillis();
                viewer.updateInfo((int)(currentTime - Long.valueOf(value.get(1).toString())), currentTime - lastTime);
                lastTime = currentTime;
            }

        }

        @Override
        public void close(Throwable errorOrNull)
        {

        }
}