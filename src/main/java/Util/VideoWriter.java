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
            viewer.showImage(img);

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