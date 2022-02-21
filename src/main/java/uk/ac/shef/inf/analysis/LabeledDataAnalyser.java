package uk.ac.shef.inf.analysis;

import com.opencsv.*;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.util.Precision;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import uk.ac.shef.inf.Util;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


/**
 * input should be the labeled .tar.gz files by the ML model
 *
 * output will produce
 * - a csv file containing basic stats
 * - a csv file of top 1000 most posting users and their fakek %
 * - a csv file containing distribution of fake/real over ratings
 * - a csv file containing distribution of fake/real over review length
 */
public class LabeledDataAnalyser {

    /**
     * "label": 'Fake',  # not required
     *         "mean":  5,  # not required
     *         "med": 5.5,
     *         "q1": 3.5,
     *         "q3": 7.5,
     *         # "cilo": 5.3 # not required
     *         # "cihi": 5.7 # not required
     *         "whislo": 2.0,  # required
     *         "whishi": 8.0,  # required
     * @param stats
     * @param label
     */
    public JSONObject prepareJSONStringBoxplot(DescriptiveStatistics stats, String label){
        JSONObject boxplot_stats = new JSONObject();
        boxplot_stats.put("mean", Precision.round(stats.getMean(),1));
        boxplot_stats.put("med", stats.getPercentile(50));
        boxplot_stats.put("q1", stats.getPercentile(25));
        boxplot_stats.put("q3", stats.getPercentile(75));
        boxplot_stats.put("whishi", stats.getMax());
        boxplot_stats.put("whislo",stats.getMin());
        boxplot_stats.put("label",label);
        boxplot_stats.put("stdev",stats.getStandardDeviation());
        boxplot_stats.put("variance",stats.getVariance());

        return boxplot_stats;
    }

    public JSONObject prepareJSONStringCategory(DescriptiveStatistics fake,
                                                DescriptiveStatistics real,
                                                String category){
        JSONObject pair = new JSONObject();
        pair.put("fake", prepareJSONStringBoxplot(fake, "Fake"));
        pair.put("real", prepareJSONStringBoxplot(real, "Real"));
        JSONObject output = new JSONObject();
        output.put(category, pair);
        return output;
    }

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

        Map<String, Integer> fake_1word=new HashMap<>();
        Map<String, Integer> real_1word=new HashMap<>();
        Map<String, Integer> fake_2words=new HashMap<>();
        Map<String, Integer> real_2words=new HashMap<>();
        Map<String, Integer> fake_3words=new HashMap<>();
        Map<String, Integer> real_3words=new HashMap<>();
        Map<String, Integer> fake_4words=new HashMap<>();
        Map<String, Integer> real_4words=new HashMap<>();
        Map<String, Integer> fake_5words=new HashMap<>();
        Map<String, Integer> real_5words=new HashMap<>();
        Map<String, Integer> fake_10words=new HashMap<>();
        Map<String, Integer> real_10words=new HashMap<>();
        Map<String, Integer> fake_50words=new HashMap<>();
        Map<String, Integer> real_50words=new HashMap<>();
        Map<String, Integer> fake_100words=new HashMap<>();
        Map<String, Integer> real_100words=new HashMap<>();
        Map<String, Integer> fake_150words=new HashMap<>();
        Map<String, Integer> real_150words=new HashMap<>();
        Map<String, Integer> fake_200words=new HashMap<>();
        Map<String, Integer> real_200words=new HashMap<>();
        Map<String, Integer> fake_250words=new HashMap<>();
        Map<String, Integer> real_250words=new HashMap<>();
        Map<String, Integer> fake_manywords=new HashMap<>();
        Map<String, Integer> real_manywords=new HashMap<>();


        List<String> files = Util.listFiles(inFolder);
        System.out.format("%s\tProcessing a total of %d files", new Date(), files.size());

        String prev_datasource=null;
        DescriptiveStatistics stats_rating_fake=new DescriptiveStatistics();
        DescriptiveStatistics stats_rating_real=new DescriptiveStatistics();
        DescriptiveStatistics stats_words_fake=new DescriptiveStatistics();
        DescriptiveStatistics stats_words_real=new DescriptiveStatistics();

        JSONArray rating_json=new JSONArray();
        JSONArray words_json=new JSONArray();

        for (String f: files){
            if (!f.endsWith(".tar.gz"))
                continue;

            String datasource = f.substring(0, f.indexOf("."));

            //write descriptive stats
            if (prev_datasource!=null && !prev_datasource.equalsIgnoreCase(datasource)){
                System.out.format("\n%s\t\t  > reset descriptive stats", new Date());
                JSONObject rating_data=prepareJSONStringCategory(stats_rating_fake, stats_rating_real, prev_datasource);
                rating_json.add(rating_data);
                JSONObject words_data=prepareJSONStringCategory(stats_words_fake, stats_words_real, prev_datasource);
                words_json.add(words_data);

                stats_rating_fake=new DescriptiveStatistics();
                stats_rating_real=new DescriptiveStatistics();
                stats_words_fake=new DescriptiveStatistics();
                stats_words_real=new DescriptiveStatistics();
                prev_datasource=datasource;
            }
            if (prev_datasource==null)
                prev_datasource=datasource;

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
                            String review=arrays[2];
                            int words = review.split("\\s+").length;

                            if (label.equalsIgnoreCase("cg")){
                                stats_rating_fake.addValue(Double.parseDouble(arrays[1]));
                                stats_words_fake.addValue(words);
                            }else{
                                stats_rating_real.addValue(Double.parseDouble(arrays[1]));
                                stats_words_real.addValue(words);
                            }

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

                            //review length
                            if(words==1){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_1word.getOrDefault(datasource, 0);
                                    fake_1word.put(datasource, fr+1);
                                }else{
                                    int fr = real_1word.getOrDefault(datasource, 0);
                                    real_1word.put(datasource, fr+1);
                                }
                            }else if(words==2){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_2words.getOrDefault(datasource, 0);
                                    fake_2words.put(datasource, fr+1);
                                }else{
                                    int fr = real_2words.getOrDefault(datasource, 0);
                                    real_2words.put(datasource, fr+1);
                                }
                            }else if(words==3){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_3words.getOrDefault(datasource, 0);
                                    fake_3words.put(datasource, fr+1);
                                }else{
                                    int fr = real_3words.getOrDefault(datasource, 0);
                                    real_3words.put(datasource, fr+1);
                                }
                            }else if(words==4){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_4words.getOrDefault(datasource, 0);
                                    fake_4words.put(datasource, fr+1);
                                }else{
                                    int fr = real_4words.getOrDefault(datasource, 0);
                                    real_4words.put(datasource, fr+1);
                                }
                            }else if(words==5){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_5words.getOrDefault(datasource, 0);
                                    fake_5words.put(datasource, fr+1);
                                }else{
                                    int fr = real_5words.getOrDefault(datasource, 0);
                                    real_5words.put(datasource, fr+1);
                                }
                            }else if(words>5 && words<=10){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_10words.getOrDefault(datasource, 0);
                                    fake_10words.put(datasource, fr+1);
                                }else{
                                    int fr = real_10words.getOrDefault(datasource, 0);
                                    real_10words.put(datasource, fr+1);
                                }
                            }else if(words>10 && words<=50){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_50words.getOrDefault(datasource, 0);
                                    fake_50words.put(datasource, fr+1);
                                }else{
                                    int fr = real_50words.getOrDefault(datasource, 0);
                                    real_50words.put(datasource, fr+1);
                                }
                            }else if(words>50 && words<=100){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_100words.getOrDefault(datasource, 0);
                                    fake_100words.put(datasource, fr+1);
                                }else{
                                    int fr = real_100words.getOrDefault(datasource, 0);
                                    real_100words.put(datasource, fr+1);
                                }
                            }else if(words>100 && words<=150){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_150words.getOrDefault(datasource, 0);
                                    fake_150words.put(datasource, fr+1);
                                }else{
                                    int fr = real_150words.getOrDefault(datasource, 0);
                                    real_150words.put(datasource, fr+1);
                                }
                            }else if(words>150 && words<=200){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_200words.getOrDefault(datasource, 0);
                                    fake_200words.put(datasource, fr+1);
                                }else{
                                    int fr = real_200words.getOrDefault(datasource, 0);
                                    real_200words.put(datasource, fr+1);
                                }
                            }else if(words>200 && words<=250){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_250words.getOrDefault(datasource, 0);
                                    fake_250words.put(datasource, fr+1);
                                }else{
                                    int fr = real_250words.getOrDefault(datasource, 0);
                                    real_250words.put(datasource, fr+1);
                                }
                            }else if(words>250){
                                if (label.equalsIgnoreCase("cg")) {
                                    int fr = fake_manywords.getOrDefault(datasource, 0);
                                    fake_manywords.put(datasource, fr+1);
                                }else{
                                    int fr = real_manywords.getOrDefault(datasource, 0);
                                    real_manywords.put(datasource, fr+1);
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

        //descriptive stats
        JSONObject rating_data=prepareJSONStringCategory(stats_rating_fake, stats_rating_real, prev_datasource);
        rating_json.add(rating_data);
        JSONObject words_data=prepareJSONStringCategory(stats_words_fake, stats_words_real, prev_datasource);
        words_json.add(words_data);
        FileWriter jsonwriter_rating = new FileWriter(outFolder+"/stats_descriptive_rating.json");
        jsonwriter_rating.write(rating_json.toJSONString());
        jsonwriter_rating.close();
        FileWriter jsonwriter_words = new FileWriter(outFolder+"/stats_descriptive_words.json");
        jsonwriter_words.write(words_json.toJSONString());
        jsonwriter_words.close();

        File file = new File(outFolder+"/stats.csv");
        File file_dist_rating = new File(outFolder+"/stats_dist_rating.csv");
        File file_dist_words = new File(outFolder+"/stats_dist_words.csv");
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

            // create FileWriter object with file as parameter
            FileWriter outputfile_distrating = new FileWriter(file_dist_rating);
            // create CSVWriter object filewriter object as parameter
            CSVWriter writer_distrating = new CSVWriter(outputfile_distrating, ',',
                    CSVWriter.DEFAULT_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);

            // adding header to csv
            String[] header_distrating = { "", "1", "2",
                    "3","4","5"
            };
            writer_distrating.writeNext(header_distrating);

            // create FileWriter object with file as parameter
            FileWriter outputfile_distwords = new FileWriter(file_dist_words);
            // create CSVWriter object filewriter object as parameter
            CSVWriter writer_distwords = new CSVWriter(outputfile_distwords, ',',
                    CSVWriter.DEFAULT_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);

            // adding header to csv
            String[] header_distwords = { "", "1", "2",
                    "3","4","5","5-10","10-50","50-100","100-150","150-200","200-250","Over 250"
            };
            writer_distwords.writeNext(header_distwords);

            List<String> keys = new ArrayList<>(total.keySet());
            keys.sort((s, t1) -> total.get(t1).compareTo(total.get(s)));

            //key is the category
            for (String k : keys){
                //general stats
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
                        String.valueOf(Precision.round(percent_fake,3)), String.valueOf(Precision.round(percent_real,3)),
                        String.valueOf(Precision.round(percent_verified,3)), String.valueOf(Precision.round(percent_nonverified,3)),
                        String.valueOf(Precision.round(percent_rating0,3)),
                        String.valueOf(Precision.round(percent_rating1,3)),
                        String.valueOf(Precision.round(percent_rating2,3)),
                        String.valueOf(Precision.round(percent_rating3,3)),
                        String.valueOf(Precision.round(percent_rating4,3)),
                        String.valueOf(Precision.round(percent_rating5,3)),
                        String.valueOf(Precision.round(percent_fov,3)),String.valueOf(Precision.round(percent_rov,3)),
                        String.valueOf(Precision.round(percent_fonv,3)),String.valueOf(Precision.round(percent_ronv,3)),
                        String.valueOf(Precision.round(percent_fo0,3)),String.valueOf(Precision.round(percent_ro0,3)),
                        String.valueOf(Precision.round(percent_fo1,3)),String.valueOf(Precision.round(percent_ro1,3)),
                        String.valueOf(Precision.round(percent_fo2,3)),String.valueOf(Precision.round(percent_ro2,3)),
                        String.valueOf(Precision.round(percent_fo3,3)),String.valueOf(Precision.round(percent_ro3,3)),
                        String.valueOf(Precision.round(percent_fo4,3)),String.valueOf(Precision.round(percent_ro4,3)),
                        String.valueOf(Precision.round(percent_fo5,3)),String.valueOf(Precision.round(percent_ro5,3))
                    };
                writer.writeNext(row);

                //dist rating stats
                double percent_fo1_of_total =total_fake==0?0: (double)fake_of_1rating/total_fake;
                double percent_ro1_of_total =total_real==0?0: (double)real_of_1rating/total_real;
                double percent_fo2_of_total =total_fake==0?0: (double)fake_of_2rating/total_fake;
                double percent_ro2_of_total =total_real==0?0: (double)real_of_2rating/total_real;
                double percent_fo3_of_total =total_fake==0?0: (double)fake_of_3rating/total_fake;
                double percent_ro3_of_total =total_real==0?0: (double)real_of_3rating/total_real;
                double percent_fo4_of_total =total_fake==0?0: (double)fake_of_4rating/total_fake;
                double percent_ro4_of_total =total_real==0?0: (double)real_of_4rating/total_real;
                double percent_fo5_of_total =total_fake==0?0: (double)fake_of_5rating/total_fake;
                double percent_ro5_of_total =total_real==0?0: (double)real_of_5rating/total_real;

                String[] row_dist_rating_real={k+" Real",String.valueOf(Precision.round(percent_ro1_of_total,3))
                        ,String.valueOf(Precision.round(percent_ro2_of_total,3)),
                        String.valueOf(Precision.round(percent_ro3_of_total,3)),
                        String.valueOf(Precision.round(percent_ro4_of_total,3)),
                        String.valueOf(Precision.round(percent_ro5_of_total,3))};
                String[] row_dist_rating_fake={k+ " Fake",String.valueOf(Precision.round(percent_fo1_of_total,3)),
                        String.valueOf(Precision.round(percent_fo2_of_total,3)),
                        String.valueOf(Precision.round(percent_fo3_of_total,3)),
                        String.valueOf(Precision.round(percent_fo4_of_total,3)),
                        String.valueOf(Precision.round(percent_fo5_of_total,3))};
                writer_distrating.writeNext(row_dist_rating_real);
                writer_distrating.writeNext(row_dist_rating_fake);

                //dist words stats
                int real_of_1word=real_1word.getOrDefault(k, 0);
                double percent_real1word =total_real==0?0: (double)real_of_1word/total_real;
                int real_of_2word=real_2words.getOrDefault(k, 0);
                double percent_real2word =total_real==0?0: (double)real_of_2word/total_real;
                int real_of_3word=real_3words.getOrDefault(k, 0);
                double percent_real3word =total_real==0?0: (double)real_of_3word/total_real;
                int real_of_4word=real_4words.getOrDefault(k, 0);
                double percent_real4word =total_real==0?0: (double)real_of_4word/total_real;
                int real_of_5word=real_5words.getOrDefault(k, 0);
                double percent_real5word =total_real==0?0: (double)real_of_5word/total_real;
                int real_of_10word=real_10words.getOrDefault(k, 0);
                double percent_real10word =total_real==0?0: (double)real_of_10word/total_real;
                int real_of_50word=real_50words.getOrDefault(k, 0);
                double percent_real50word =total_real==0?0: (double)real_of_50word/total_real;
                int real_of_100word=real_100words.getOrDefault(k, 0);
                double percent_real100word =total_real==0?0: (double)real_of_100word/total_real;
                int real_of_150word=real_150words.getOrDefault(k, 0);
                double percent_real150word =total_real==0?0: (double)real_of_150word/total_real;
                int real_of_200word=real_200words.getOrDefault(k, 0);
                double percent_real200word =total_real==0?0: (double)real_of_200word/total_real;
                int real_of_250word=real_250words.getOrDefault(k, 0);
                double percent_real250word =total_real==0?0: (double)real_of_250word/total_real;
                int real_of_manyword=real_manywords.getOrDefault(k, 0);
                double percent_realmanyword =total_real==0?0: (double)real_of_manyword/total_real;


                String[] row_dist_words_real={k+" Real",String.valueOf(Precision.round(percent_real1word,3))
                        ,String.valueOf(Precision.round(percent_real2word,3))
                        ,String.valueOf(Precision.round(percent_real3word,3))
                        ,String.valueOf(Precision.round(percent_real4word,3))
                        ,String.valueOf(Precision.round(percent_real5word,3))
                        ,String.valueOf(Precision.round(percent_real10word,3))
                        ,String.valueOf(Precision.round(percent_real50word,3))
                        ,String.valueOf(Precision.round(percent_real100word,3))
                        ,String.valueOf(Precision.round(percent_real150word,3))
                        ,String.valueOf(Precision.round(percent_real200word,3))
                        ,String.valueOf(Precision.round(percent_real250word,3))
                        ,String.valueOf(Precision.round(percent_realmanyword,3))};
                writer_distwords.writeNext(row_dist_words_real);

                int fake_of_1word=fake_1word.getOrDefault(k, 0);
                double percent_fake1word =total_fake==0?0: (double)fake_of_1word/total_fake;
                int fake_of_2word=fake_2words.getOrDefault(k, 0);
                double percent_fake2word =total_fake==0?0: (double)fake_of_2word/total_fake;
                int fake_of_3word=fake_3words.getOrDefault(k, 0);
                double percent_fake3word =total_fake==0?0: (double)fake_of_3word/total_fake;
                int fake_of_4word=fake_4words.getOrDefault(k, 0);
                double percent_fake4word =total_fake==0?0: (double)fake_of_4word/total_fake;
                int fake_of_5word=fake_5words.getOrDefault(k, 0);
                double percent_fake5word =total_fake==0?0: (double)fake_of_5word/total_fake;
                int fake_of_10word=fake_10words.getOrDefault(k, 0);
                double percent_fake10word =total_fake==0?0: (double)fake_of_10word/total_fake;
                int fake_of_50word=fake_50words.getOrDefault(k, 0);
                double percent_fake50word =total_fake==0?0: (double)fake_of_50word/total_fake;
                int fake_of_100word=fake_100words.getOrDefault(k, 0);
                double percent_fake100word =total_fake==0?0: (double)fake_of_100word/total_fake;
                int fake_of_150word=fake_150words.getOrDefault(k, 0);
                double percent_fake150word =total_fake==0?0: (double)fake_of_150word/total_fake;
                int fake_of_200word=fake_200words.getOrDefault(k, 0);
                double percent_fake200word =total_fake==0?0: (double)fake_of_200word/total_fake;
                int fake_of_250word=fake_250words.getOrDefault(k, 0);
                double percent_fake250word =total_fake==0?0: (double)fake_of_250word/total_fake;
                int fake_of_manyword=fake_manywords.getOrDefault(k, 0);
                double percent_fakemanyword =total_fake==0?0: (double)fake_of_manyword/total_fake;

                String[] row_dist_words_fake={k+" Fake",String.valueOf(Precision.round(percent_fake1word,3))
                        ,String.valueOf(Precision.round(percent_fake2word,3))
                        ,String.valueOf(Precision.round(percent_fake3word,3))
                        ,String.valueOf(Precision.round(percent_fake4word,3))
                        ,String.valueOf(Precision.round(percent_fake5word,3))
                        ,String.valueOf(Precision.round(percent_fake10word,3))
                        ,String.valueOf(Precision.round(percent_fake50word,3))
                        ,String.valueOf(Precision.round(percent_fake100word,3))
                        ,String.valueOf(Precision.round(percent_fake150word,3))
                        ,String.valueOf(Precision.round(percent_fake200word,3))
                        ,String.valueOf(Precision.round(percent_fake250word,3))
                        ,String.valueOf(Precision.round(percent_fakemanyword,3))};

                writer_distwords.writeNext(row_dist_words_fake);

            }
            // closing writer connection
            writer.close();
            writer_distrating.close();
            writer_distwords.close();
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
                String[] row = {u, String.valueOf(total_of_user), String.valueOf(Precision.round(perc, 3))};
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
