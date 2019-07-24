package compress;

import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.spark.io.LZFCompressionCodec;
import org.xerial.snappy.Snappy;
import net.jpountz.lz4.*;

public class CompressTestForSpark {
    public static void main(String[] args) throws Exception {
        String input = "Hello snappy-java! Snappy-java is a JNI-based wrapper of Snappy, a fast compresser/decompresser.";
        byte[] bytes = input.getBytes();
        System.out.println(bytes.length);
        Lz4Codec codec = new Lz4Codec();
        SnappyCodec snappyCodec = new SnappyCodec();
        snappyCodec.createCompressor();
    }
}
