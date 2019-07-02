package Util;

import org.apache.calcite.util.Static;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.util.LinkedList;
import java.util.Queue;

public class VideoWriter extends ForeachWriter<Row>{
        public static Queue<Row> rows = new LinkedList<>();
        public static boolean isFirstStart = true;

        @Override
        public boolean open(long partitionId, long epochId)
        {
            return true;
        }

        @Override
        public void process(Row value){
            rows.offer(value);
        }

        @Override
        public void close(Throwable errorOrNull)
        {

        }
}