package Util;

import org.apache.calcite.util.Static;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;

public class VideoWriter extends ForeachWriter<Row>{
        public static Queue<Row> rows = new LinkedList<>();
        public static boolean isFirstStart = true;
        ImageViewer viewer = new ImageViewer("ImageViewer");

        @Override
        public boolean open(long partitionId, long epochId)
        {
            return true;
        }

        @Override
        public void process(Row value){
//            rows.offer(value);

                        String str = value.get(0).toString();
                        // pass the value
                        //      compare.frame = str

        /*             String[] data = str.split(";");*/
            if(value.length() == 2)
            {
                System.out.println(System.currentTimeMillis() - Long.valueOf(value.get(1).toString()));
            }
                        //      val newData = ImageProcess.watermark(ImageProcess.toGray(data))



        //if(str.length()!=0) str = str.substring(0,str.length()-1);
            byte[] tempb = StringUtil.hexStringToBytes(str);
            InputStream buffin = new ByteArrayInputStream(tempb);
            BufferedImage img = null;
            try {
                img = ImageIO.read(buffin);
            } catch (IOException e) {
                e.printStackTrace();
            }
            //if(str.length() == 0) System.err.println("空值！！！");
            viewer.showImage(img);


        }

        @Override
        public void close(Throwable errorOrNull)
        {

        }
}