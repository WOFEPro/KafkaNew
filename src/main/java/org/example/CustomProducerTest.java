package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;


import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class CustomProducerTest {

    private static final int MAX_RETRIES = 3;
    // 使用 ConcurrentHashMap 来存储消息的重试次数
    private static final ConcurrentHashMap<String, Integer> retryCounts = new ConcurrentHashMap<>();
    //1.ConcurrentHashMap<String, Integer>：这部分定义了变量的数据类型。ConcurrentHashMap 是 Java 中的一个线程安全的哈希表（散列表）实现，它用于存储键值对。在这里，键的类型是 String，值的类型是 Integer，即用于存储消息的重试次数。
    //2.retryCounts = new ConcurrentHashMap<>();：这段代码初始化 retryCounts 变量为一个新创建的空的 ConcurrentHashMap 实例，准备用于存储消息的重试次数信息。
    public static void main(String[] args) throws IOException{

        long startTime = System.currentTimeMillis(); // 记录程序开始时间

        String filePath = "C:\\dashboard\\log_java\\dashboard.txt";

        //0.配置属性
        Properties properties = new Properties();

        //连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.0.106:9092");
        //指定对应的key和value的序列化类型
        //properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //与下面等价
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //数据的单分区有序(设置 max.in.flight.requests.per.connection 参数)
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,1);
        //acks
        properties.put(ProducerConfig.ACKS_CONFIG,"1");
        // 设置为0，不进行重试，重试由代码控制
        properties.put(ProducerConfig.RETRIES_CONFIG,0);
        // 设置为等待连接的最大时间，单位是毫秒
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);

        //1.创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //2.发送数据
        File latestFile = new File(filePath);

        if (latestFile != null){

            //解析Json数据
            JSONObject messageJson = dataCode(latestFile);
            //Json数据到String的转换
            String message = messageJson.toString();

            ProducerRecord<String, String> record = new ProducerRecord<>("learMessage", message);

            try{
                kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if( e == null){
                            System.out.println("主题:"+ metadata.topic() + "分区:" + metadata.partition());
                        }else {
                            // 消息发送失败,增加重试次数
                            //1.retryCounts 是一个 ConcurrentHashMap，它用于存储每条消息的重试次数。这个 map 的键是消息的 key（或者你可以根据消息内容生成一个唯一的键），值是消息的重试次数。
                            //2.retryCounts.getOrDefault(key, 0) 这个表达式的作用是从 retryCounts 中获取与给定 key 关联的值，如果 key 不存在，就返回默认值 0。这就意味着，如果消息的重试次数在 retryCounts 中已经存在，那么就获取它的重试次数；如果不存在，就默认设置重试次数为 0。
                            //3.最终，retryCount 变量包含了当前消息的重试次数。
                            String key = record.key();
                            int retryCount = retryCounts.getOrDefault(key,0);
                            if(retryCount < MAX_RETRIES){
                                retryCounts.put(key,retryCount + 1);
                            }else {
                                try {
                                    callApiWithData(messageJson);
                                } catch (IOException ex) {
                                    throw new RuntimeException(ex);
                                }
                            }
                        }
                    }
                });
            }catch (Exception e){
                // 处理 Kafka 异常，可以选择执行 API 调用等操作
                System.err.println("Kafka 异常: " + e.getMessage());
                callApiWithData(messageJson);
/*
                if( e instanceof TimeoutException || e instanceof DisconnectException){
                    //在连接错误时执行 API 调用
                    System.out.println(1);
                }
 */
            }
        }

        //3.关闭资源
        kafkaProducer.close();

        long endTime = System.currentTimeMillis(); // 记录程序结束时间
        long totalTime = endTime - startTime; // 计算总运行时间
        System.out.println("程序运行总时间（毫秒）: " + totalTime);

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


    private static void callApiWithData(JSONObject data) throws IOException {
        //此处为执行api call的方法

        // 定义请求的URL
        String hostUrl = "http://192.168.0.106:83/AssemblyCount/assemblycount";
        URL url = new URL(hostUrl);

        // 创建连接
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        // 将JSON数据写入请求的body
        try (OutputStream outputStream = conn.getOutputStream()) {
            byte[] input = data.toString().getBytes("utf-8");
            outputStream.write(input, 0, input.length);
        }

        // 发送请求并获取响应
        int responseCode = conn.getResponseCode();
        System.out.println("Response Code: " + responseCode);

        conn.disconnect();

    }
}



