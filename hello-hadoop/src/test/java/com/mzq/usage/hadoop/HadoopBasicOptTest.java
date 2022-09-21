package com.mzq.usage.hadoop;

import com.mzq.usage.Employee;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;

public class HadoopBasicOptTest {

    @Test
    public void testListFiles() throws IOException {
        // 入参true代表从本地classpath中寻找core-site.xml和hdfs-site.xml这两个文件，从里面寻找连接hdfs系统的属性
        Configuration configuration = new Configuration(true);
        FileSystem fileSystem = FileSystem.get(configuration);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/upload/kafka.pdf"), false);
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

    @Test
    public void testPutFile() throws IOException {
        Configuration configuration = new Configuration();
        Path path = new Path("/upload/kafka.pdf");
        try (FileSystem fileSystem = FileSystem.get(configuration);
             FileInputStream fileInputStream = new FileInputStream("/Users/maziqiang/Downloads/677585 Apache Kafka实战.pdf")) {
            boolean exists = fileSystem.exists(path);
            if (exists) {
                fileSystem.delete(path, false);
            }

            // 实际hdfs在上传文件时需要把文件切块，并把切块上传到不同的datanode中。但客户端在使用时，完全没有要发送给不同客户端的感觉，只认为是往OutputStream里发送数据。hdfs对客户端屏蔽了分布式文件系统的感觉。
            FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
            byte[] buffer = new byte[1024];
            while (fileInputStream.read(buffer) != -1) {
                fsDataOutputStream.write(buffer);
            }
            fsDataOutputStream.flush();
            fsDataOutputStream.close();
        }
    }

    @Test
    public void testLoadFile() throws IOException {
        Configuration configuration = new Configuration();

        try (FileSystem fileSystem = FileSystem.get(configuration);
             FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/upload/kafka.pdf"))) {
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

    @Test
    public void testCheckFileStatus() throws IOException {
        Configuration configuration = new Configuration();
        try (FileSystem fileSystem = FileSystem.get(configuration)) {

            Path path = new Path("/upload/kafka.pdf");
            // 通过getFileStatus方法，我们可以看到指定路径的文件的信息，和我们在hdfs页面能看到的信息一样
            // 包括文件按多大的切块来切分的（blockSize），切块的副本数量，文件的大小、最后更新时间、owner、group等信息。
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            short replication = fileStatus.getReplication();
            long blockSize = fileStatus.getBlockSize();
            long fileLength = fileStatus.getLen();
            long modificationTime = fileStatus.getModificationTime();
            String owner = fileStatus.getOwner();
            String group = fileStatus.getGroup();

            System.out.println("文件信息：" + path.getName());
            System.out.printf("file=%s,blockSize=%d,replication=%d,fileLength=%d,modify_time=%d,owner=%s,group=%s%n"
                    , path, blockSize, replication, fileLength, modificationTime, owner, group);

            System.out.println("切块信息：" + path.getName());
            // 对于一个切块来说，它主要的信息有切块的起始位置(offset)、长度(length)、副本存储在datanode的地址(names属性，例如172.20.0.4:50010)、副本存储在datanode的host(hosts属性，例如13c248966ca2)
            BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(path, 0, fileLength);
            for (BlockLocation blockLocation : fileBlockLocations) {
                System.out.printf("start offset=%d,length=%d,hosts=%s,names=%s%n", blockLocation.getOffset(), blockLocation.getLength(), String.join(",", blockLocation.getHosts()), String.join(",", blockLocation.getNames()));
            }
        }
    }

    @Test
    public void testLoadFilePartly() {
        Configuration configuration = new Configuration();
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            Path path = new Path("/upload/data-center-web-info.log");
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, false);
            }
            FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
            java.nio.file.Path localPath = Paths.get("/Users/maziqiang/Downloads/data-center-web-info-2020-07-14-1.log");
            File localFile = localPath.toFile();
            if (!localFile.exists()) {
                throw new IllegalStateException("需要上传的文件不存在！");
            }

            FileInputStream fileInputStream = new FileInputStream(localFile);
            byte[] buffer = new byte[2048];
            while (fileInputStream.read(buffer) != -1) {
                fsDataOutputStream.write(buffer);
            }
            fsDataOutputStream.flush();
            fileInputStream.close();
            fsDataOutputStream.close();

            // 获取文件的状态，主要需要知道文件有多大。在获取切块信息时，需要给出要多少字节范围内的块。由于我们现在不知道文件多大，所以需要先获取。
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            // 根据文件的大小，查询从文件最开始（offset=0），文件大小是length都有哪些块，也就是整个文件被拆分成了哪些块。我们也可以指定offset和length,查询指定offset开始，长度在length内，都有哪些切块。
            // 这个是hdfs需要实现的核心内容，因为它是实现分布式计算的前提。之后每个MR程序，都使用该方法获取自己需要读取的块的信息，而不是读取文件的全部块。
            // 这样就可以实现多个MR程序并行对同一个文件的不同块进行计算，这就是分布式计算的核心！！！
            BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            BlockLocation blockWeNeed = fileBlockLocations[fileBlockLocations.length - 2];

            // 使用指定目录开启一个从hdfs到客户端的输入流
            FSDataInputStream fsDataInputStream = fileSystem.open(path);
            // 使用seek方法，让输入流跳过不需要的字节，直接从指定offset开始读起
            fsDataInputStream.seek(blockWeNeed.getOffset());

            File file = new File("/Users/maziqiang/Documents/data-center-web-info-part.log");
            if (file.exists()) {
                file.delete();
            }

            // 根据我们要读的block的长度，在读取数据时，只读取该切块的大小的数据
            long blockLength = blockWeNeed.getLength();
            // 3000个字节读一次，减少I/O频率
            long initialBufferSize = 3000;
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            // 当循环执行完后，我们就获取了从offset开始读，并且读了长度为blockLength大小的数据，也就是只读取了我们需要的切块的数据。
            // 这个功能的实现就是分布式计算的核心功能！！！
            // 在这里我们要注意一个细节，在读取文件时，是需要向多个切块所在的datanode进行数据拉取的。但我们在编程时，根本没有要连接哪个datanode的概念，
            // 我们完全是只使用FsDataInputStream进行数据的读取。因此是FsDataInputStream帮我们实现了分布式文件读取的细节，让我们感觉就像读本地文件一样。
            do {
                long bufferSize = Math.min(blockLength, initialBufferSize);
                buffer = new byte[(int) bufferSize];
                fsDataInputStream.read(buffer);
                fileOutputStream.write(buffer);
                blockLength -= bufferSize;
            } while (blockLength > 0);

            fileOutputStream.flush();
            fileOutputStream.close();
            fsDataInputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUploadFile() {
        Configuration configuration = new Configuration();
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            fileSystem.delete(new Path("/hello_external/000000_0"));
//
//            Path path = new Path("/upload/emp_data.txt");
//            if (fileSystem.exists(path)) {
//                fileSystem.delete(path, false);
//            }
//            FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
//            for (int i = 1; i <= 20; i++) {
//                fsDataOutputStream.write(Employee.generate().toString().getBytes());
//            }
//            fsDataOutputStream.flush();
//            fsDataOutputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
