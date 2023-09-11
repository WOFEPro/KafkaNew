package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CustomProducerTest3GPTAdvancedVersion {

    private static final Logger logger = LoggerFactory.getLogger(CustomProducerTest.class);
    private static final int MAX_RETRIES = 3;
    private static final ConcurrentHashMap<String, Integer> retryCounts = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();

        String filePath = "C:\\dashboard\\log_java\\dashboard.txt";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.232:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        File latestFile = new File(filePath);

        if (latestFile != null) {
            JSONObject messageJson = dataCode(latestFile);
            String message = messageJson.toString();

            ProducerRecord<String, String> record = new ProducerRecord<>("learMessage", message);

            try {
                kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            logger.info("消息发送成功 - 主题: {}, 分区: {}", metadata.topic(), metadata.partition());
                        } else {
                            String key = record.key();
                            int retryCount = retryCounts.getOrDefault(key, 0);
                            if (retryCount < MAX_RETRIES) {
                                retryCounts.put(key, retryCount + 1);
                                // 添加重试之间的延迟，避免频繁重试
                                try {
                                    TimeUnit.SECONDS.sleep(1);
                                } catch (InterruptedException ex) {
                                    Thread.currentThread().interrupt();
                                }
                            } else {
                                handleExceptionWithApiCall(messageJson, e);
                            }
                        }
                    }
                });
            } catch (Exception e) {
                logger.error("Kafka 异常: {}", e.getMessage());
                handleExceptionWithApiCall(messageJson, e);
            }
        }

        kafkaProducer.close();

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        logger.info("程序运行总时间（毫秒）: {}", totalTime);
    }

    public static JSONObject dataCode(File file) throws IOException {
        String json = new String(Files.readAllBytes(file.toPath()));
        JSONObject jsonObject = new JSONObject(json);

        String assemblyName = jsonObject.getString("AssemblyName");
        String serialNo = jsonObject.getString("SerialNo");
        String qty = jsonObject.getString("Qty");
        String finishedTime = jsonObject.getString("FinishedTime");
        String station = jsonObject.getString("Station");
        String line = jsonObject.getString("Line");

        JSONObject requestData = new JSONObject();
        requestData.put("AssemblyName", assemblyName);
        requestData.put("SerialNo", serialNo);
        requestData.put("Qty", qty);
        requestData.put("FinishedTime", finishedTime + "+08:00");
        requestData.put("Station", station);
        requestData.put("Line", line);

        return requestData;
    }

    private static void handleExceptionWithApiCall(JSONObject data, Exception e) {
        logger.error("处理异常并执行 API 调用: {}", e.getMessage());
        try {
            callApiWithData(data);
        } catch (IOException ex) {
            logger.error("API 调用异常: {}", ex.getMessage());
        }
    }

    private static void callApiWithData(JSONObject data) throws IOException {
        String hostUrl = "http://192.168.0.232:8090/AssemblyCount/assemblycount";
        URL url = new URL(hostUrl);

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        try (OutputStream outputStream = conn.getOutputStream()) {
            byte[] input = data.toString().getBytes("utf-8");
            outputStream.write(input, 0, input.length);
        }

        int responseCode = conn.getResponseCode();
        logger.info("API 响应码: {}", responseCode);

        conn.disconnect();
    }
}
