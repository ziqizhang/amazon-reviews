package uk.ac.shef.inf.analysis;

import com.opencsv.*;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.math3.util.Precision;
import uk.ac.shef.inf.Util;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


/**
 * input should be the labeled .tar.gz files by the ML model
 *
 * output will produce csv files containing basic stats
 */
public class LabeledDataAnalyser {

    public void analyse(String inFolder, String outFolder) throws IOException {
        Map<String, Integer> total=new HashMap<>();
        Map<String, Integer> verified=new HashMap<>();
        Map<String, Integer> non_verified=new HashMap<>();
        Map<String, Integer> rating_0=new HashMap<>();
        Map<String, Integer> rating_1=new HashMap<>();
        Map<String, Integer> rating_2=new HashMap<>();
        Map<String, Integer> rating_3=new HashMap<>();
        Map<String, Integer> rating_4=new HashMap<>();
        Map<String, Integer> rating_5=new HashMap<>();
        Map<String, Integer> fake=new HashMap<>();
        Map<String, Integer> real=new HashMap<>();

        Map<String, Integer> author_fake=new HashMap<>();
        Map<String, Integer> author_total=new HashMap<>();

        Map<String, Integer> fake_rating0=new HashMap<>();
        Map<String, Integer> real_rating0=new HashMap<>();
        Map<String, Integer> fake_rating1=new HashMap<>();
        Map<String, Integer> real_rating1=new HashMap<>();
        Map<String, Integer> fake_rating2=new HashMap<>();
        Map<String, Integer> real_rating2=new HashMap<>();
        Map<String, Integer> fake_rating3=new HashMap<>();
        Map<String, Integer> real_rating3=new HashMap<>();
        Map<String, Integer> fake_rating4=new HashMap<>();
        Map<String, Integer> real_rating4=new HashMap<>();
        Map<String, Integer> fake_rating5=new HashMap<>();
        Map<String, Integer> real_rating5=new HashMap<>();

        Map<String, Integer> fake_verified=new HashMap<>();
        Map<String, Integer> real_verified=new HashMap<>();
        Map<String, Integer> fake_non_verified=new HashMap<>();
        Map<String, Integer> real_non_verified=new HashMap<>();


        List<String> files = Util.listFiles(inFolder);
        System.out.format("%s\tProcessing a total of %d files", new Date(), files.size());

        for (String f: files){
            if (!f.endsWith(".tar.gz"))
                continue;

            String datasource = f.substring(0, f.indexOf("."));

            Path source = Paths.get(inFolder+"/"+f);
            System.out.format("\n%s\t\tStarted %s", new Date(), source);

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
                /*
                Path source = Paths.get("/home/zz/Work/data/amazon/labelled/subset/Books_5.json.27.tar.gz");
            InputStream fi = Files.newInputStream(source);
            BufferedInputStream bi = new BufferedInputStream(fi);
            GzipCompressorInputStream gzi = new GzipCompressorInputStream(bi);
            TarArchiveInputStream ti = new TarArchiveInputStream(gzi);
            ArchiveEntry entry;
            entry = ti.getNextEntry();
            while (entry != null) {
                System.out.println(entry.getName());
                entry=ti.getNextEntry();
            }
                 */
                CSVParser parser = new CSVParserBuilder().withStrictQuotes(true).withEscapeChar('\0')
                        .withQuoteChar('"').build();

                while (entry!= null) {
                    // Get the unzip file directory , And determine whether the file is damaged
                    br = new BufferedReader(new InputStreamReader(ti)); // Read directly from tarInput
                    System.out.format("\n%s\t\t  > %s", new Date(), entry.getName());
                    try{

                        CSVReader reader = new CSVReaderBuilder(br).withCSVParser(parser)
                                .build();
                        //List<String[]> r = readAllLines(reader);
                        List<String[]> r = reader.readAll();
                        int listIndex = 0;
                        for (String[] arrays : r) {
                            if (listIndex==0){
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
                            int index = 0;
                            String rt = String.valueOf((int)Double.parseDouble(arrays[1]));
                            String label = arrays[4];
                            String vrf=arrays[5];
                            String ath =arrays[6];

                            int freq = total.getOrDefault(datasource, 0);
                            total.put(datasource, freq + 1);
                            //rating
                            if(rt.equalsIgnoreCase("0")){
                                freq = rating_0.getOrDefault(datasource, 0);
                                rating_0.put(datasource, freq + 1);
                            }else if(rt.equalsIgnoreCase("1")){
                                freq = rating_1.getOrDefault(datasource, 0);
                                rating_1.put(datasource, freq + 1);
                            }else if(rt.equalsIgnoreCase("2")){
                                freq = rating_2.getOrDefault(datasource, 0);
                                rating_2.put(datasource, freq + 1);
                            }else if(rt.equalsIgnoreCase("3")){
                                freq = rating_3.getOrDefault(datasource, 0);
                                rating_3.put(datasource, freq + 1);
                            }else if(rt.equalsIgnoreCase("4")){
                                freq = rating_4.getOrDefault(datasource, 0);
                                rating_4.put(datasource, freq + 1);
                            }else if(rt.equalsIgnoreCase("5")){
                                freq = rating_5.getOrDefault(datasource, 0);
                                rating_5.put(datasource, freq + 1);
                            }
                            //verified
                            if (vrf.equalsIgnoreCase("true")) {
                                freq = verified.getOrDefault(datasource, 0);
                                verified.put(datasource, freq + 1);
                            }else if (vrf.equalsIgnoreCase("false")) {
                                freq = non_verified.getOrDefault(datasource, 0);
                                non_verified.put(datasource, freq + 1);
                            }
                            //fake
                            int freq_author = author_total.getOrDefault(ath, 0);
                            author_total.put(ath, freq_author+1);
                            if (label.equalsIgnoreCase("cg")) {
                                freq = fake.getOrDefault(datasource, 0);
                                fake.put(datasource, freq + 1);

                                int freq_author_fake = author_fake.getOrDefault(ath, 0);
                                author_fake.put(ath, freq_author_fake+1);
                            }else {
                                freq = real.getOrDefault(datasource, 0);
                                real.put(datasource, freq + 1);
                            }

                            /*
                            Map<String, Integer> fake_verified=new HashMap<>();
                            Map<String, Integer> real_verified=new HashMap<>();
                            Map<String, Integer> fake_non_verified=new HashMap<>();
                            Map<String, Integer> real_non_verified=new HashMap<>();
                             */
                            if (vrf.equalsIgnoreCase("true")) {
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_verified.getOrDefault(datasource, 0);
                                    fake_verified.put(datasource, fr+1);
                                }else{
                                    int fr = real_verified.getOrDefault(datasource, 0);
                                    real_verified.put(datasource, fr+1);
                                }
                            }else if (vrf.equalsIgnoreCase("false")) {
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_non_verified.getOrDefault(datasource, 0);
                                    fake_non_verified.put(datasource, fr+1);
                                }else{
                                    int fr = real_non_verified.getOrDefault(datasource, 0);
                                    real_non_verified.put(datasource, fr+1);
                                }
                            }

                            /*
                            Map<String, Integer> fake_rating0=new HashMap<>();
                            Map<String, Integer> real_rating0=new HashMap<>();
                            Map<String, Integer> fake_rating1=new HashMap<>();
                            Map<String, Integer> real_rating1=new HashMap<>();
                            Map<String, Integer> fake_rating2=new HashMap<>();
                            Map<String, Integer> real_rating2=new HashMap<>();
                            Map<String, Integer> fake_rating3=new HashMap<>();
                            Map<String, Integer> real_rating3=new HashMap<>();
                            Map<String, Integer> fake_rating4=new HashMap<>();
                            Map<String, Integer> real_rating4=new HashMap<>();
                            Map<String, Integer> fake_rating5=new HashMap<>();
                            Map<String, Integer> real_rating5=new HashMap<>();
                             */
                            if(rt.equalsIgnoreCase("0")){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_rating0.getOrDefault(datasource, 0);
                                    fake_rating0.put(datasource, fr+1);
                                }else{
                                    int fr = real_rating0.getOrDefault(datasource, 0);
                                    real_rating0.put(datasource, fr+1);
                                }
                            }else if(rt.equalsIgnoreCase("1")){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_rating1.getOrDefault(datasource, 0);
                                    fake_rating1.put(datasource, fr+1);
                                }else{
                                    int fr = real_rating1.getOrDefault(datasource, 0);
                                    real_rating1.put(datasource, fr+1);
                                }
                            }else if(rt.equalsIgnoreCase("2")){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_rating2.getOrDefault(datasource, 0);
                                    fake_rating2.put(datasource, fr+1);
                                }else{
                                    int fr = real_rating2.getOrDefault(datasource, 0);
                                    real_rating2.put(datasource, fr+1);
                                }
                            }else if(rt.equalsIgnoreCase("3")){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_rating3.getOrDefault(datasource, 0);
                                    fake_rating3.put(datasource, fr+1);
                                }else{
                                    int fr = real_rating3.getOrDefault(datasource, 0);
                                    real_rating3.put(datasource, fr+1);
                                }
                            }else if(rt.equalsIgnoreCase("4")){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_rating4.getOrDefault(datasource, 0);
                                    fake_rating4.put(datasource, fr+1);
                                }else{
                                    int fr = real_rating4.getOrDefault(datasource, 0);
                                    real_rating4.put(datasource, fr+1);
                                }
                            }else if(rt.equalsIgnoreCase("5")){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_rating5.getOrDefault(datasource, 0);
                                    fake_rating5.put(datasource, fr+1);
                                }else{
                                    int fr = real_rating5.getOrDefault(datasource, 0);
                                    real_rating5.put(datasource, fr+1);
                                }
                            }

                            listIndex++;
                        }
                    }catch (Exception ioe){
                        System.err.format("\n%s\t\tFailed %s", new Date(), entry.getName());
                        ioe.printStackTrace();

                    }
                    entry = ti.getNextEntry();
                }

            }catch (Exception e){
                e.printStackTrace();
            }

        }

        System.out.format("\n%s\tAll completed, saving data", new Date());
        File file = new File(outFolder+"/stats.csv");
        try {
            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(file);
            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputfile, ',',
                    CSVWriter.DEFAULT_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);

            // adding header to csv
            String[] header = { "DataSource", "Total", "Total_fake%",
                    "Total_real%","Total_verified%","Total_nonverified%",
                    "Total_rating_0%","Total_rating_1%","Total_rating_2%",
                    "Total_rating_3%","Total_rating_4%","Total_rating_5%",
                    "%fake_of_verified","%real_of_verified",
                    "%fake_of_nonverified","%real_of_nonverified",
                    "%fake_of_0rating","%real_of_0rating",
                    "%fake_of_1rating","%real_of_1rating",
                    "%fake_of_2rating","%real_of_2rating",
                    "%fake_of_3rating","%real_of_3rating",
                    "%fake_of_4rating","%real_of_4rating",
                    "%fake_of_5rating","%real_of_5rating",
            };
            writer.writeNext(header);

            List<String> keys = new ArrayList<>(total.keySet());
            Collections.sort(keys);

            for (String k : keys){
                int total_freq =total.get(k);
                int total_fake = fake.getOrDefault(k, 0);
                double percent_fake = (double) total_fake /total_freq;
                int total_real = real.getOrDefault(k, 0);
                double percent_real = (double) total_real /total_freq;

                int total_verified=verified.getOrDefault(k, 0);
                double percent_verified=(double)total_verified/total_freq;
                int total_nonverified=non_verified.getOrDefault(k, 0);
                double percent_nonverified=(double)total_nonverified/total_freq;

                int total_rating0 = rating_0.getOrDefault(k, 0);
                double percent_rating0=(double)total_rating0/total_freq;
                int total_rating1 = rating_1.getOrDefault(k, 0);
                double percent_rating1=(double)total_rating1/total_freq;
                int total_rating2 = rating_2.getOrDefault(k, 0);
                double percent_rating2=(double)total_rating2/total_freq;
                int total_rating3 = rating_3.getOrDefault(k, 0);
                double percent_rating3=(double)total_rating3/total_freq;
                int total_rating4 = rating_4.getOrDefault(k, 0);
                double percent_rating4=(double)total_rating4/total_freq;
                int total_rating5 = rating_5.getOrDefault(k, 0);
                double percent_rating5=(double)total_rating5/total_freq;

                int fake_of_verified=fake_verified.getOrDefault(k, 0);
                double percent_fov = (double)fake_of_verified/total_verified;
                int real_of_verified=real_verified.getOrDefault(k, 0);
                double percent_rov = (double)real_of_verified/total_verified;
                int fake_of_nonverified=fake_non_verified.getOrDefault(k, 0);
                double percent_fonv = (double)fake_of_nonverified/total_nonverified;
                int real_of_nonverified=real_non_verified.getOrDefault(k, 0);
                double percent_ronv = (double)real_of_nonverified/total_nonverified;

                int fake_of_0rating=fake_rating0.getOrDefault(k, 0);
                double percent_fo0 =total_rating0==0?0: (double)fake_of_0rating/total_rating0;
                int real_of_0rating=real_rating0.getOrDefault(k, 0);
                double percent_ro0 =total_rating0==0?0: (double)real_of_0rating/total_rating0;
                int fake_of_1rating=fake_rating1.getOrDefault(k, 0);
                double percent_fo1 =total_rating1==0?0: (double)fake_of_1rating/total_rating1;
                int real_of_1rating=real_rating1.getOrDefault(k, 0);
                double percent_ro1 =total_rating1==0?0: (double)real_of_1rating/total_rating1;
                int fake_of_2rating=fake_rating2.getOrDefault(k, 0);
                double percent_fo2 =total_rating2==0?0: (double)fake_of_2rating/total_rating2;
                int real_of_2rating=real_rating2.getOrDefault(k, 0);
                double percent_ro2 =total_rating2==0?0: (double)real_of_2rating/total_rating2;
                int fake_of_3rating=fake_rating3.getOrDefault(k, 0);
                double percent_fo3 =total_rating3==0?0: (double)fake_of_3rating/total_rating3;
                int real_of_3rating=real_rating3.getOrDefault(k, 0);
                double percent_ro3 =total_rating3==0?0: (double)real_of_3rating/total_rating3;
                int fake_of_4rating=fake_rating4.getOrDefault(k, 0);
                double percent_fo4 =total_rating4==0?0: (double)fake_of_4rating/total_rating4;
                int real_of_4rating=real_rating4.getOrDefault(k, 0);
                double percent_ro4 =total_rating4==0?0: (double)real_of_4rating/total_rating4;
                int fake_of_5rating=fake_rating5.getOrDefault(k, 0);
                double percent_fo5 =total_rating5==0?0: (double)fake_of_5rating/total_rating5;
                int real_of_5rating=real_rating5.getOrDefault(k, 0);
                double percent_ro5 =total_rating5==0?0: (double)real_of_5rating/total_rating5;

                String[] row = { k,
                        String.valueOf(total_freq),
                        String.valueOf(Precision.round(percent_fake,2)), String.valueOf(Precision.round(percent_real,2)),
                        String.valueOf(Precision.round(percent_verified,2)), String.valueOf(Precision.round(percent_nonverified,2)),
                        String.valueOf(Precision.round(percent_rating0,2)),
                        String.valueOf(Precision.round(percent_rating2,2)),
                        String.valueOf(Precision.round(percent_rating2,2)),
                        String.valueOf(Precision.round(percent_rating3,2)),
                        String.valueOf(Precision.round(percent_rating4,2)),
                        String.valueOf(Precision.round(percent_rating5,2)),
                        String.valueOf(Precision.round(percent_fov,2)),String.valueOf(Precision.round(percent_rov,2)),
                        String.valueOf(Precision.round(percent_fonv,2)),String.valueOf(Precision.round(percent_ronv,2)),
                        String.valueOf(Precision.round(percent_fo0,2)),String.valueOf(Precision.round(percent_ro0,2)),
                        String.valueOf(Precision.round(percent_fo2,2)),String.valueOf(Precision.round(percent_ro2,2)),
                        String.valueOf(Precision.round(percent_fo2,2)),String.valueOf(Precision.round(percent_ro2,2)),
                        String.valueOf(Precision.round(percent_fo3,2)),String.valueOf(Precision.round(percent_ro3,2)),
                        String.valueOf(Precision.round(percent_fo4,2)),String.valueOf(Precision.round(percent_ro4,2)),
                        String.valueOf(Precision.round(percent_fo5,2)),String.valueOf(Precision.round(percent_ro5,2))
                    };
                writer.writeNext(row);
            }
            // closing writer connection
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        file = new File(outFolder+"/user.csv");
        try {
            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(file);
            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputfile, ',',
                    CSVWriter.DEFAULT_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);
            String[] header = {"user", "total", "fake%"};
            writer.writeNext(header);
            List<String> users = new ArrayList<>(author_total.keySet());
            users.sort((s, t1) -> author_total.get(t1).compareTo(author_total.get(s)));
            int topn=1000, count=0;

            for (String u : users){
                int total_of_user = author_total.get(u);
                int totalfake_of_user=author_fake.getOrDefault(u,0);
                double perc = (double) totalfake_of_user /total_of_user;
                String[] row = {u, String.valueOf(total_of_user), String.valueOf(Precision.round(perc, 1))};
                writer.writeNext(row);
                count++;
                if (count>=topn)
                    break;
            }
            writer.close();
        }
        catch (IOException ioe){
            ioe.printStackTrace();
        }
    }

    private List<String[]> readAllLines(CSVReader reader) {
        List<String[]> out = new ArrayList<>();
        int line=0;
        try{
            String[] lineInArray = reader.readNext();

            while(lineInArray!=null) {
                line++;
                out.add(lineInArray);
                lineInArray=reader.readNext();

            }
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(out.size());
        return out;
    }

    public static void main(String[] args) throws IOException {
        LabeledDataAnalyser analyser = new LabeledDataAnalyser();
        analyser.analyse(args[0], args[1]);
    }
}