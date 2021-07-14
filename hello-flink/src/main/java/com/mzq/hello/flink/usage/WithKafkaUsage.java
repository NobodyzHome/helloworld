package com.mzq.hello.flink.usage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mzq.hello.domain.WaybillC;
import com.mzq.hello.flink.func.source.WaybillCSource;
import com.mzq.hello.flink.kafka.WaybillcDeserializationSchema;
import com.mzq.hello.flink.kafka.WaybillcSerializationSchema;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.SystemDefaultCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

public class WithKafkaUsage extends BaseFlinkUsage {

    private static final Logger logger = LoggerFactory.getLogger(WithKafkaUsage.class);
    private static final String WAYBILL_C_SETTINGS = "{\"settings\":{\"number_of_replicas\":1,\"number_of_shards\":1},\"mappings\":{\"properties\":{\"waybillCode\":{\"type\":\"keyword\"},\"waybillSign\":{\"type\":\"keyword\"},\"siteCode\":{\"type\":\"keyword\"},\"siteName\":{\"type\":\"keyword\"},\"timeStamp\":{\"type\":\"long\"},\"watermark\":{\"type\":\"long\"},\"siteWaybills\":{\"type\":\"keyword\"}}}}";

    public WithKafkaUsage(StreamExecutionEnvironment streamExecutionEnvironment) {
        super(streamExecutionEnvironment);
    }

    @Override
    protected String jobName() {
        return WithKafkaUsage.class.getName();
    }

    @Override
    protected void setupStream() throws Exception {
        testUseWithKafka();
    }

    private void testUseWithKafka() throws Exception {
        RestClientBuilder restClientBuilder = RestClient.builder(HttpHost.create("my-elasticsearch:9200"));
        restClientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
        restClientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                logger.error("访问节点失败！node={}", node);
            }
        });
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        IndicesClient indicesClient = restHighLevelClient.indices();
        if (indicesClient.exists(new GetIndexRequest("waybill-c"), RequestOptions.DEFAULT)) {
            indicesClient.delete(new DeleteIndexRequest("waybill-c"), RequestOptions.DEFAULT);
        }
        indicesClient.create(new CreateIndexRequest("waybill-c").source(WAYBILL_C_SETTINGS, XContentType.JSON), RequestOptions.DEFAULT);

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaybillC> waybillcStreamSource = streamExecutionEnvironment.addSource(new WaybillCSource()).setParallelism(1);

        String topic = "waybill-c";
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, "3");
        WaybillcSerializationSchema waybillcSerializationSchema = new WaybillcSerializationSchema(topic);
        FlinkKafkaProducer<WaybillC> flinkKafkaProducer = new FlinkKafkaProducer<>(topic, waybillcSerializationSchema, producerConfig, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        waybillcStreamSource.addSink(flinkKafkaProducer).setParallelism(5);

        WaybillcDeserializationSchema waybillcDeSerializationSchema = new WaybillcDeserializationSchema();
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "hello-group");
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "hello-client");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        FlinkKafkaConsumer<WaybillC> flinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, waybillcDeSerializationSchema, consumerConfig);
        DataStreamSource<WaybillC> waybillcKafkaSource = streamExecutionEnvironment.addSource(flinkKafkaConsumer);

        waybillcKafkaSource.addSink(new RichSinkFunction<WaybillC>() {
            private RedisClient redisClient;
            private StatefulRedisConnection<String, String> connect;
            private ObjectMapper objectMapper;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                redisClient = RedisClient.create("redis://my-redis:6379/3");
                connect = redisClient.connect();
                objectMapper = new ObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            }

            @Override
            public void invoke(WaybillC waybillC, Context context) throws Exception {
                RedisFuture<String> redisFuture = connect.async().set(waybillC.getWaybillCode(), objectMapper.writeValueAsString(waybillC));
                redisFuture.whenComplete((result, throwable) -> {
                    if (Objects.nonNull(throwable)) {
                        logger.error("写入redis失败。waybillC={}", waybillC.getWaybillCode(), throwable);
                    } else {
                        logger.info("写入redis成功。waybillC={}，当前线程：{}", waybillC.getWaybillCode(), Thread.currentThread().getName());
                    }
                });
            }

            @Override
            public void close() throws Exception {
                super.close();

                connect.close();
                redisClient.shutdown();
            }
        }).setParallelism(6);

        ElasticsearchSink.Builder<WaybillC> waybillcEsSinkBuilder = new ElasticsearchSink.Builder<>(Collections.singletonList(HttpHost.create("my-elasticsearch:9200")),
                new ElasticsearchSinkFunction<WaybillC>() {
                    private ObjectMapper objectMapper;

                    @Override
                    public void open() throws Exception {
                        objectMapper = new ObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
                    }

                    @Override
                    public void process(WaybillC waybillC, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        try {
                            byte[] source = objectMapper.writeValueAsBytes(waybillC);
                            UpdateRequest updateRequest = new UpdateRequest("waybill-c", waybillC.getWaybillCode());
                            updateRequest.doc(source, XContentType.JSON).docAsUpsert(true).retryOnConflict(5);
                            requestIndexer.add(updateRequest);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }

                    }
                });

        waybillcEsSinkBuilder.setRestClientFactory(restClientBuilder1 -> {
            restClientBuilder1.setFailureListener(new RestClient.FailureListener() {
                @Override
                public void onFailure(Node node) {
                    logger.error("访问节点失败！节点={}", node);
                }
            });
            restClientBuilder1.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
            restClientBuilder1.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(1000).setSocketTimeout(1500).setConnectTimeout(800));
            restClientBuilder1.setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.setMaxConnTotal(200);

                SystemDefaultCredentialsProvider credentialsProvider = new SystemDefaultCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("user", "pass"));
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                return httpClientBuilder;
            });
        });
        waybillcEsSinkBuilder.setBulkFlushMaxActions(30);
        waybillcEsSinkBuilder.setBulkFlushMaxSizeMb(5);
        waybillcEsSinkBuilder.setBulkFlushInterval(10000);
        waybillcEsSinkBuilder.setBulkFlushBackoff(true);
        waybillcEsSinkBuilder.setBulkFlushBackoffDelay(2000);
        waybillcEsSinkBuilder.setBulkFlushBackoffRetries(5);
        waybillcEsSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.CONSTANT);
        waybillcEsSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        ElasticsearchSink<WaybillC> waybillCElasticsearchSink = waybillcEsSinkBuilder.build();
        waybillcKafkaSource.addSink(waybillCElasticsearchSink).setParallelism(5);

        streamExecutionEnvironment.execute();
    }
}
