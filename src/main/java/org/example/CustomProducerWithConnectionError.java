package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

public class CustomProducerWithConnectionError {
    public static void main(String[] args) throws IOException {
        String filePath = "C:\\dashboard\\log_java\\dashboard.txt";

        // 配置属性
        Properties properties = new Properties();

        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.232:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 创建 Kafka 生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 2.发送数据
        File latestFile = new File(filePath);

        if (latestFile != null) {
            String json = new String(Files.readAllBytes(latestFile.toPath()));
            JSONObject jsonObject = new JSONObject(json);

            // ... 解析 JSON 数据和构造消息的代码 ...
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

            String message = requestData.toString();

            try {
                kafkaProducer.send(new ProducerRecord<>("learMessage", message), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            System.out.println("主题:" + metadata.topic() + "分区:" + metadata.partition());
                        } else {
                            // 在发送失败时处理异常
                            System.err.println("消息发送失败: " + e.getMessage());

                            if (e instanceof TimeoutException || e instanceof DisconnectException) {
                                // 在连接错误时执行 API 调用
                                callApiWithData(requestData);
                            }
                        }
                    }
                });
            } catch (Exception e) {
                // 处理 Kafka 异常，可以选择执行 API 调用等操作
                System.err.println("Kafka 异常: " + e.getMessage());
            }
        }

        // 3.关闭资源
        kafkaProducer.close();
    }

    private static void callApiWithData(JSONObject data) {
        // 执行 API 调用的代码
        System.out.println("执行 API 调用，传递数据：" + data);
        // 此处应添加执行 API 调用的代码
    }
}
