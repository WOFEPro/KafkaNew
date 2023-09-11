package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class CustomProducerWithSendError {
    private static final String KAFKA_TOPIC = "learMessage";
    private static final String FILE_PATH = "C:\\dashboard\\log_java\\dashboard.txt";
    private static final int MAX_RETRIES = 3;

    // 使用 ConcurrentHashMap 来存储消息的重试次数
    private static final ConcurrentHashMap<String, Integer> retryCounts = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        // 配置属性
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.232:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0); // 设置初始重试次数为0

        // 创建 Kafka 生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 发送数据
        File latestFile = new File(FILE_PATH);

        if (latestFile != null) {
            try {
                //解析Json数据
                JSONObject messageJson = dataCode(latestFile);
                //Json数据到String的转换
                String message = messageJson.toString();

                ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, message);

                kafkaProducer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        // 消息发送失败，增加重试次数
                        String key = record.key(); // 你可以根据需求设置消息的 key
                        int retryCount = retryCounts.getOrDefault(key, 0);
                        if (retryCount < MAX_RETRIES) {
                            retryCounts.put(key, retryCount + 1);
                        } else {
                            // 达到最大重试次数，执行 API 调用
                            try {
                                callApiWithData(messageJson);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void callApiWithData(JSONObject data) throws IOException {
        // 在这里执行 API 调用，并传递数据
        // 定义请求的URL和执行 API 调用的逻辑
    }


    public static JSONObject dataCode(File file) throws IOException{

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
}

