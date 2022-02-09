package uk.ac.shef.inf.analysis;

import com.opencsv.*;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import uk.ac.shef.inf.Util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class LabeledDataExtractor {

    public void extract(String inFolder, String outFolder, int minWords, String mlLabel) throws IOException {
        Map<String, List<Path>> filesByName = new HashMap<>();

        List<String> files = Util.listFiles(inFolder);
        System.out.format("%s\tProcessing a total of %d files\n", new Date(), files.size());

        for (String f : files) {
            if (!f.endsWith(".tar.gz"))
                continue;

            String datasource = f.substring(0, f.indexOf("."));
            Path source = Paths.get(inFolder + "/" + f);
            if (filesByName.containsKey(datasource)) {
                List<Path> matchingFiles = filesByName.get(datasource);
                matchingFiles.add(source);
            } else {
                List<Path> matchingFiles = new ArrayList<>();
                matchingFiles.add(source);
                filesByName.put(datasource, matchingFiles);
            }
        }

        for (Map.Entry<String, List<Path>> en : filesByName.entrySet()) {
            int count=0, countSelected=0;
            System.out.format("%s\tStarted %s, total of %d files \n", new Date(), en.getKey(), en.getValue().size());
            File file = new File(outFolder+"/"+en.getKey()+".txt");

            FileOutputStream fos = new FileOutputStream(file);
            OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            BufferedWriter writer = new BufferedWriter(osw);

            for (Path source : en.getValue()) {
                System.out.format("%s\t\tprocessing %s\n", new Date(), source);

                //InputStream Input stream , The following four streams will tar.gz Read into memory and operate
                //BufferedInputStream Buffered input stream
                //GzipCompressorInputStream Decompress the input stream
                //TarArchiveInputStream Explain tar Packet input stream
                try {
                    InputStream fi = Files.newInputStream(source);
                    BufferedInputStream bi = new BufferedInputStream(fi);
                    GzipCompressorInputStream gzi = new GzipCompressorInputStream(bi);
                    TarArchiveInputStream ti = new TarArchiveInputStream(gzi);
                    BufferedReader br = null;
                    ArchiveEntry entry;
                    entry = ti.getNextEntry();

                    CSVParser parser = new CSVParserBuilder().withStrictQuotes(true).withEscapeChar('\0')
                            .withQuoteChar('"').build();

                    while (entry != null) {
                        // Get the unzip file directory , And determine whether the file is damaged
                        br = new BufferedReader(new InputStreamReader(ti)); // Read directly from tarInput
                        System.out.format("%s\t\t  > %s\n", new Date(), entry.getName());
                        try {

                            CSVReader reader = new CSVReaderBuilder(br).withCSVParser(parser)
                                    .build();
                            //List<String[]> r = readAllLines(reader);
                            List<String[]> r = reader.readAll();
                            int listIndex = 0;
                            for (String[] arrays : r) {
                                if (listIndex == 0) {
                                    listIndex++;
                                    continue;
                                }
                            /*
                            columns
                            0 - file
                            1 - rating
                            2 - review
                            4 - ml label
                            5 - verified
                            6 - author
                            7 - product id
                             */
                                String review = String.valueOf(arrays[2]);
                                String label = arrays[4];

                                if (!label.equalsIgnoreCase(mlLabel))
                                    continue;
                                count++;
                                review=review.replaceAll("\n"," ").trim();
                                if (review.split("\\s+").length<minWords)
                                    continue;
                                countSelected++;

                                writer.append(review);
                                writer.newLine();
                            }
                        } catch (Exception ioe) {
                            System.err.format("%s\t\tFailed %s\n", new Date(), entry.getName());
                            ioe.printStackTrace();

                        }
                        entry = ti.getNextEntry();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            writer.close();
            System.out.format("%s\tFinished %s, total %d, selected %d \n", new Date(), en.getKey(), count, countSelected);

        }
    }

    public static void main(String[] args) throws IOException {
        LabeledDataExtractor extractor = new LabeledDataExtractor();
        extractor.extract(args[0], args[1], 1, "CG");
    }
}
