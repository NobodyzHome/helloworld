package com.mzq.usage.haddop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;

public class HelloWorld {

    public static void main(String[] args) throws IOException {
        testLoadFile();
//        testPutFile();
    }

    public static void testListFiles() throws IOException {
        // 入参true代表从本地classpath中寻找core-site.xml和hdfs-site.xml这两个文件，从里面寻找连接hdfs系统的属性
        Configuration configuration = new Configuration(true);
        FileSystem fileSystem = FileSystem.get(configuration);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/test/external"), false);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            Path path = next.getPath();
            System.out.println(path);
            BlockLocation[] blockLocations = next.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                System.out.printf("start = %d,length= %d,hosts=%s%n", blockLocation.getOffset(), blockLocation.getLength(), String.join(",", blockLocation.getHosts()));
            }
        }
    }

    public static void testPutFile() throws IOException {
        Configuration configuration = new Configuration();
        Path path = new Path("/heihei/kafka.pdf");
        try (FileSystem fileSystem = FileSystem.get(configuration);
             FileInputStream fileInputStream = new FileInputStream("/Users/maziqiang/Downloads/kafka.pdf")) {
            boolean exists = fileSystem.exists(path);
            if (exists) {
                fileSystem.delete(path, false);
            }

            FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
            byte[] buffer = new byte[1024];
            while (fileInputStream.read(buffer) != -1) {
                fsDataOutputStream.write(buffer);
            }
            fsDataOutputStream.flush();
            fsDataOutputStream.close();
        }
    }

    public static void testLoadFile() throws IOException {
        Configuration configuration = new Configuration();

        try (FileSystem fileSystem = FileSystem.get(configuration);
             FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/heihei/kafka.pdf"))) {
            java.nio.file.Path localPath = Paths.get("/Users/maziqiang/Documents/kafka.pdf");
            File file = localPath.toFile();
            if (file.exists()) {
                file.delete();
            }
            file.createNewFile();

            FileOutputStream fileOutputStream = new FileOutputStream(file);
            byte[] buffer = new byte[2048];
            while (fsDataInputStream.read(buffer) != -1) {
                fileOutputStream.write(buffer);
            }
            fileOutputStream.flush();
            fileOutputStream.close();
        }
    }
}
