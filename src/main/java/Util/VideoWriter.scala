package Util;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

class VideoWriter extends ForeachWriter[Row]{
        val viewer = new ImageViewer("ImageViewer")

        override def open(partitionId: Long, epochId: Long): Boolean = true

        def process(value: Row): Unit = {
        val str = value.get(0).toString
        // pass the value
        //      compare.frame = str

        val data = str.split(";")
        //      val newData = ImageProcess.watermark(ImageProcess.toGray(data))
        viewer.showImage(data)
        ()
        };

        override def close(errorOrNull:Throwable): Unit = ()
}