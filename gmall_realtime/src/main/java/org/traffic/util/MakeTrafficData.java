package org.traffic.util;

import org.apache.commons.math3.random.GaussianRandomGenerator;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.utils.DateTimeUtil;

import java.io.PrintWriter;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * 制造数据---》kafka
 */
public class MakeTrafficData {
    public static void main(String[] args) throws Exception {

        PrintWriter pw = new PrintWriter("D:\\project\\gmall_flink\\gmall2023\\gmall_realtime\\src\\main\\java\\org\\traffic\\data");


        //模拟车辆
        String[] locations = new String[]{"京", "津", "冀", "鲁", "京", "京", "京", "京", "京", "京"};
        Random random = new Random();

        GaussianRandomGenerator generator = new GaussianRandomGenerator(new JDKRandomGenerator());


        String topic = "traffic_monoitor";
        String broker = "192.168.120.181:9092";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", broker);
        prop.setProperty("key.serializer", StringSerializer.class.getName());
        prop.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);


        for (int i = 0; i < 3000; i++) {
            int index = random.nextInt(10);
            //模拟车牌号
            String carId = locations[index] + (char) (65 + random.nextInt(26)) + String.format("%05d", random.nextInt(99999));

            //模拟每辆车通过的卡口数，每辆车通过的卡口数符合高斯分布
            int thrould = (int) Math.abs(generator.nextNormalizedDouble() * 100);
            for (int j = 0; j < thrould; j++) {

                //通过的区域
                String areaId = String.format("%02d", random.nextInt(8));

                //通过的道路
                String roudId = String.format("%02d", random.nextInt(50));

                //通过的卡口
                String monitorId = String.format("%04d", random.nextInt(9999));

                //通过的摄像头
                String cameraId = String.format("%05d", random.nextInt(99999));

                //通过的拍摄时间
                String actionTime = DateTimeUtil.toYMDhms(new Date()).split(" ")[0] + " "
                        + String.format("%02d", random.nextInt(24)) + ":"
                        + String.format("%02d", random.nextInt(60)) + ":"
                        + String.format("%02d", random.nextInt(60));

                //拍摄速度,大部分车辆速度位于60，符合正态分布
                String speed = String.format("%.2f", Math.abs(generator.nextNormalizedDouble() * 60));

                String info = areaId + "\t" + roudId + "\t" + monitorId + "\t" + cameraId + "\t" + actionTime + "\t" + carId + "\t" + speed;

                //数据写入文件中
                pw.println(info);

                //消息发送到kafka
                producer.send(new ProducerRecord<String, String>(topic, info));

            }
        }

        producer.close();
        pw.close();

    }
}
