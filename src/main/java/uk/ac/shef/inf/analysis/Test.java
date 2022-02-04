package uk.ac.shef.inf.analysis;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        try {
            Path source = Paths.get("/home/zz/Work/data/amazon/labelled/small/Books_5.json.1.tar.gz");
            InputStream fi = Files.newInputStream(source);
            BufferedInputStream bi = new BufferedInputStream(fi);
            GzipCompressorInputStream gzi = new GzipCompressorInputStream(bi);
            TarArchiveInputStream ti = new TarArchiveInputStream(gzi);
            CSVParser parser = new CSVParserBuilder().withStrictQuotes(true).withEscapeChar('\0')
                    .withQuoteChar('"').withSeparator(',').build();
            BufferedReader br = null;
            ArchiveEntry entry;
            entry = ti.getNextEntry();
            while (entry != null) {
                br = new BufferedReader(new InputStreamReader(ti)); // Read directly from tarInput
                System.out.format("\n%s\t\t  > %s", new Date(), entry.getName());
                try{
                    CSVReader reader = new CSVReaderBuilder(br).withCSVParser(parser)
                            .build();
                    List<String[]> r = readAllLines(reader);
                } catch (Exception ioe){
                    ioe.printStackTrace();
                }
                System.out.println(entry.getName());
                entry=ti.getNextEntry();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static List<String[]> readAllLines(CSVReader reader) {
        List<String[]> out = new ArrayList<>();
        int line=0;
        try{
            String[] lineInArray = reader.readNext();

            while(lineInArray!=null) {
                //System.out.println(Arrays.asList(lineInArray));
                out.add(lineInArray);
                line++;
                lineInArray=reader.readNext();
            }
        }catch (Exception e){
            System.out.println(line);
            e.printStackTrace();
        }
        System.out.println(out.size());
        return out;
    }
}
