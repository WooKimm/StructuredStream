package compress;

import org.apache.spark.sql.sources.In;

import java.util.ArrayList;
import java.util.List;

public class Model {
    private List<Integer> datasize_maps = new ArrayList<>();
    private List<Integer> datasize_spills = new ArrayList<>();
    private double sRatio, lzfRatio, lz4Ratio;
    private double sCompressRate, lzfCompressRate, lz4CompressRate;
    private double sDecompressRate, lzfDecompressRate, lz4DecompressRate;
    private double networkRate;
    private double diskReadRate, diskWriteRate;
    //private boolean isMap, isSpill;

    private double sMapIncome, sSpillIncome;
    private double lzfMapIncome, lzfSpillIncome;
    private double lz4MapIncome, lz4SpillIncome;
    private double sMapCost, sSpillCost;
    private double lzfMapCost, lzfSpillCost;
    private double lz4MapCost, lz4SpillCost;

    private double totalIncome, mapIncome, spillIncome;
    private double totalCost, mapCost, spillCost;
    private double realIncome;

    public Model(List<Integer> datasize_maps, List<Integer> datasize_spills, double sRatio, double lzfRatio,
                 double lz4Ratio, double sCompressRate, double lzfCompressRate, double lz4CompressRate,
                 double sDecompressRate, double lzfDecompressRate, double lz4DecompressRate, double networkRate,
                 double diskReadRate, double diskWriteRate) {

        this.datasize_maps = datasize_maps;
        this.datasize_spills = datasize_spills;
        this.sRatio = sRatio;
        this.lzfRatio = lzfRatio;
        this.lz4Ratio = lz4Ratio;
        this.sCompressRate = sCompressRate;
        this.lzfCompressRate = lzfCompressRate;
        this.lz4CompressRate = lz4CompressRate;
        this.sDecompressRate = sDecompressRate;
        this.lzfDecompressRate = lzfDecompressRate;
        this.lz4DecompressRate = lz4DecompressRate;
        this.networkRate = networkRate;
        this.diskReadRate = diskReadRate;
        this.diskWriteRate = diskWriteRate;


        for(int i = 0; i < datasize_maps.size(); ++i)
        {
            sMapIncome += datasize_maps.get(i) * (1 - sRatio) / networkRate;
            lzfMapIncome += datasize_maps.get(i) * (1 - lzfRatio) / networkRate;
            lz4MapIncome += datasize_maps.get(i) * (1 - lz4Ratio) / networkRate;

            sMapCost += datasize_maps.get(i) / sCompressRate + datasize_maps.get(i) * sRatio / sDecompressRate;
            lzfMapCost += datasize_maps.get(i) / lzfCompressRate + datasize_maps.get(i) * lzfRatio / lzfDecompressRate;
            lz4MapCost += datasize_maps.get(i) / lz4CompressRate + datasize_maps.get(i) * lz4Ratio / lz4DecompressRate;
        }

        for(int i = 0; i < datasize_spills.size(); ++i)
        {
            sSpillIncome += datasize_spills.get(i) * (1 - sRatio) / (diskReadRate + diskWriteRate);
            lzfSpillIncome += datasize_spills.get(i) * (1 - lzfRatio) / (diskReadRate + diskWriteRate);
            lz4SpillIncome += datasize_spills.get(i) * (1 - lz4Ratio) / (diskReadRate + diskWriteRate);

            sSpillCost += datasize_spills.get(i) / sCompressRate + datasize_spills.get(i) * sRatio / sDecompressRate;
            lzfSpillCost += datasize_spills.get(i) / lzfCompressRate + datasize_spills.get(i) * lzfRatio / lzfDecompressRate;
            lz4SpillCost += datasize_spills.get(i) / lz4CompressRate + datasize_spills.get(i) * lz4Ratio / lz4DecompressRate;
        }

        System.out.println("0-1:");
        System.out.println(sSpillIncome - sSpillCost);
        System.out.println(lzfSpillIncome - lzfSpillCost);
        System.out.println(lz4SpillIncome - lz4SpillCost);

        System.out.println("1-0:");
        System.out.println(sMapIncome - sMapCost);
        System.out.println(lzfMapIncome - lzfMapCost);
        System.out.println(lz4MapIncome - lz4MapCost);

        System.out.println("1-1:");
        System.out.println(sMapIncome + sSpillIncome - sMapCost - sSpillCost);
        System.out.println(lzfMapIncome + lzfSpillIncome - lzfMapCost - lzfSpillCost);
        System.out.println(lz4MapIncome + lz4SpillIncome - lz4MapCost - lz4SpillCost);
    }

    public static void main(String[] args)
    {
        List<Integer> datasize_maps = new ArrayList<>();
        List<Integer> datasize_spills = new ArrayList<>();
        datasize_maps.add(100);
        datasize_spills.add(20);

        Model model = new Model(datasize_maps, datasize_spills, 0.4856, 0.4794, 0.5095,
                72.68, 119.41, 113.52, 264.32,
                242.14, 172.40, 10, 350, 350);
    }
}
