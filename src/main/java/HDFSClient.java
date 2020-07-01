import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URL;
import java.util.Date;
import java.util.zip.GZIPInputStream;

public class HDFSClient {
    final String APP_USER = "/user";
    final String APP_HOME = APP_USER + "/weather";
    final String APP_DATA = APP_HOME + "/data/";
    final String APP_OUT = APP_HOME + "/out";
    final String URL = "https://www.ncei.noaa.gov/data/global-hourly/archive/csv/";
    final int start = 1901;
    final int end = 1901;//2020
    Configuration configuration;
    FileSystem fileSystem;

    public HDFSClient(boolean deleteOldFiles) throws IOException {
        this.configuration = new Configuration();
        configuration.set("dfs.block.size", String.valueOf(4 * 1024 * 1024));
        System.setProperty("hadoop.home.dir", "c:\\hadoop\\");
        this.fileSystem = FileSystem.get(configuration);
        if (deleteOldFiles)
            fileSystem.delete(new Path(APP_DATA), true);
        System.out.println("[INFO] creating folders");
        fileSystem.mkdirs(new Path(APP_USER));
        fileSystem.mkdirs(new Path(APP_HOME));
        fileSystem.mkdirs(new Path(APP_DATA));
    }


    public static void main(String[] args) throws IOException {
        HDFSClient hdfsClient = new HDFSClient(false);
        hdfsClient.downloadZips();
        hdfsClient.showCopyFromLocalFileResults();
        hdfsClient.mapReduce();
        hdfsClient.showResults();

    }

    public void downloadZips() throws IOException {
        for (int i = start; i <= end; i++) {
            System.out.println("[INFO] Downloading " + i);
            String filename = i + ".tar.gz";
            URL fileURL = new URL(URL + "/" + filename);
            String filepath = System.getProperty("user.home") + "output/files/" + filename;
            File fileoutput = new File(filepath);
            FileUtils.copyURLToFile(
                    fileURL,
                    fileoutput,
                    100000, 100000
            );
            System.out.println("[INFO] Unzipping " + i);
            System.out.println("path: " + System.getProperty("user.home")
                    + "/output/unzippedFiles/" + i + "/");
            this.unZipFiles(fileoutput.getAbsolutePath(), new File(System.getProperty("user.home")
                    + "/output/unzippedFiles/" + i + "/"));
            fileSystem.copyFromLocalFile(new Path(System.getProperty("user.home")
                    + "/output/unzippedFiles/" + i), new Path(APP_DATA));
        }
    }

    private void unZipFiles(String tarFile, File destFile) {
        TarArchiveInputStream tarArchiveInputStream = null;
        try {
            FileInputStream fis = new FileInputStream(tarFile);
            GZIPInputStream gzipInputStream = new GZIPInputStream(new BufferedInputStream(fis));
            tarArchiveInputStream = new TarArchiveInputStream(gzipInputStream);
            TarArchiveEntry tarEntry;
            while ((tarEntry = tarArchiveInputStream.getNextTarEntry()) != null) {
                System.out.println(" tar entry- " + tarEntry.getName());
                if (!tarEntry.isDirectory()) {
                    File outputFile = new File(destFile + File.separator + tarEntry.getName());
                    outputFile.getParentFile().mkdirs();
                    IOUtils.copy(tarArchiveInputStream, new FileOutputStream(outputFile));
                }
            }
        } catch (IOException ex) {
            System.out.println("Error while unTarring a file- " + ex.getMessage());
        } finally {
            if (tarArchiveInputStream != null) {
                try {
                    tarArchiveInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void getAllFilePath(Path filePath) throws IOException {
        FileStatus[] fileStatus = fileSystem.listStatus(filePath);
        for (FileStatus status : fileStatus) {
            if (status.isDirectory()) {
                getAllFilePath(status.getPath());
            } else {
                System.out.printf("%-12s%-15s%-10d%-30s%-25s\n",
                        status.getPermission().toString(),
                        status.getOwner(),
                        status.getLen(),
                        new Date(status.getModificationTime()).toString(),
                        status.getPath().toString()
                );
            }
        }
    }

    private void showCopyFromLocalFileResults() {
        System.out.printf("%-12s%-15s%-10s%-30s%-25s\n",
                "Permission",
                "Owner",
                "Len",
                "Modify Date",
                "Path"
        );
        try {
            this.getAllFilePath(new Path(this.APP_USER));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void mapReduce() {
        for (int i = start; i <= end; i++) {
            try {
                System.out.println("[INFO] map reduce of " + i);
                Job job = Job.getInstance(configuration, "Temperature Data");
                job.setJarByClass(HDFSClient.class);

                job.setMapperClass(TemperatureMapper.class);
                job.setCombinerClass(TemperatureReducer.class);
                job.setReducerClass(TemperatureReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);

                FileInputFormat.addInputPath(job, new Path(APP_DATA + i));
                String out_string = APP_OUT + "/" + i;
                FileOutputFormat.setOutputPath(job, new Path(out_string));
                Path outDir = new Path(out_string);

                if (fileSystem.isDirectory(outDir)) fileSystem.delete(outDir, true);
                job.waitForCompletion(true);
            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private void showResults() {
        System.out.println("[INFO] final results");
        for (int year = start; year <= end; year++) {
            try {
                Path path = new Path(APP_OUT + "/" + year + "/part-r-00000");
                FSDataInputStream in = fileSystem.open(path);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String line = null;
                int station = 1, i = 0;
                while ((line = br.readLine()) != null) {
                    if (i % 2 == 0) {
                        System.out.println("\t Station " + (station++) + ":");
                        String max = line.split("\t")[1];
                        System.out.println("\t\t Max " + max);
                    } else {
                        String min = line.split("\t")[1];
                        System.out.println("\t\t Min " + min);
                    }
                    i++;
                }
                in.close();
                br.close();
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
