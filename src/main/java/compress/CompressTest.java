package compress;

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;
import org.xerial.snappy.Snappy;
import net.jpountz.lz4.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class CompressTest {
    public static void main(String[] args) throws Exception {
        StringBuilder sb = new StringBuilder();
        //要自行添加
        File csv = new File("src/main/java/compress/lineitem.csv");
        csv.setReadable(true);
        BufferedReader br = null;
        br = new BufferedReader(new FileReader(csv));
        String temp = "";
        while ((temp = br.readLine()) != null)
        {
            sb.append(temp);
        }

//        byte[] source = input.getBytes("UTF-8");

        //String input = getRandomString(1024*1024);
        byte[] source = sb.toString().getBytes();
        System.out.println(source.length);
        long currentTime = System.currentTimeMillis(), diffTime = 0;
        double compressRate = 0.0, decompressRate = 0.0, ratio = 0.0;

        System.out.println("snappy:");
        byte[] snappyCompressed = Snappy.compress(source);
        diffTime = System.currentTimeMillis() - currentTime;
        System.out.println(diffTime);
        System.out.println(snappyCompressed.length);
        compressRate = (double) source.length / 1024 / 1024 / diffTime * 1000;
        ratio = (double) snappyCompressed.length / source.length;
        System.out.println("压缩速率" + compressRate);
        System.out.println("压缩比" + ratio);
        currentTime = System.currentTimeMillis();
        byte[] snappyUncompressed = Snappy.uncompress(snappyCompressed);
        diffTime = System.currentTimeMillis() - currentTime;
        System.out.println(diffTime);
        System.out.println(snappyUncompressed.length);
        decompressRate = (double) snappyCompressed.length / 1024 / 1024 / diffTime * 1000;
        System.out.println("解压速率" + decompressRate);

        currentTime = System.currentTimeMillis();
        System.out.println("lzf:");
        byte[] lzfCompressed = LZFEncoder.encode(source);
        diffTime = System.currentTimeMillis() - currentTime;
        System.out.println(diffTime);
        System.out.println(lzfCompressed.length);
        compressRate = (double) source.length / 1024 / 1024 / diffTime * 1000;
        ratio = (double) lzfCompressed.length / source.length;
        System.out.println("压缩速率" + compressRate);
        System.out.println("压缩比" + ratio);
        currentTime = System.currentTimeMillis();
        byte[] lzfUncompressed = LZFDecoder.decode(lzfCompressed);
        diffTime = System.currentTimeMillis() - currentTime;
        System.out.println(diffTime);
        System.out.println(lzfUncompressed.length);
        decompressRate = (double) lzfCompressed.length / 1024 / 1024 / diffTime * 1000;
        System.out.println("解压速率" + decompressRate);

        LZ4Compressor lz4Compressor = LZ4Factory.fastestJavaInstance().fastCompressor();
        LZ4FastDecompressor lz4Decompressor = LZ4Factory.fastestJavaInstance().fastDecompressor();
        currentTime = System.currentTimeMillis();
        System.out.println("lz4:");
        byte[] lz4Compressed = lz4Compressor.compress(source);
        diffTime = System.currentTimeMillis() - currentTime;
        System.out.println(diffTime);
        System.out.println(lz4Compressed.length);
        compressRate = (double) source.length / 1024 / 1024 / diffTime * 1000;
        ratio = (double) lz4Compressed.length / source.length;
        System.out.println("压缩速率" + compressRate);
        System.out.println("压缩比" + ratio);
        currentTime = System.currentTimeMillis();
        byte[] lz4Uncompressed = lz4Decompressor.decompress(lz4Compressed, source.length);
        diffTime = System.currentTimeMillis() - currentTime;
        System.out.println(diffTime);
        System.out.println(lz4Uncompressed.length);
        decompressRate = (double) lz4Compressed.length / 1024 / 1024 / diffTime * 1000;
        System.out.println("解压速率" + decompressRate);
        
    }
}
