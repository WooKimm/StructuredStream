package Util;




import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileForeachWriter extends ForeachWriter<Row> {

    CsvWriter fileWriter;

    public FileForeachWriter(){

    }

    @Override
    public boolean open(long partitionId, long epochId) {
        try {
            FileUtils.forceMkdir(new File("src/test/resources/"+partitionId));
            fileWriter = new CsvWriter(new File("src/test/resources/"+partitionId+"/temp.csv"),new CsvWriterSettings());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void process(Row value) {
        fileWriter.addValue(value.get(0).toString()+";"+value.get(1).toString());
        fileWriter.flush();
    }

    @Override
    public void close(Throwable errorOrNull) {
        fileWriter.close();

    }
}
