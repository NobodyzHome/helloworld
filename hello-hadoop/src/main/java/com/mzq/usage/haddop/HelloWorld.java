package com.mzq.usage.haddop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileInputStream;
import java.io.IOException;

public class HelloWorld {

    public static void main(String[] args) throws IOException {
        testPutFile();
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
        Path path = new Path("/heihei/test.txt");
        try (FileSystem fileSystem = FileSystem.get(configuration);
             FileInputStream fileInputStream = new FileInputStream("/Users/maziqiang/Downloads/公租房验房流程及标准/福利房EBS操作指引操作同福利房.pdf")) {
            boolean exists = fileSystem.exists(path);
            if(exists){
                fileSystem.delete(path,false);
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

    }
}
