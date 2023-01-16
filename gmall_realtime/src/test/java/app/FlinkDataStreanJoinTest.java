package app;

import bean.Bean1;
import bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 双流join test watermark传递问题
 */
public class FlinkDataStreanJoinTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置与kafka主题分区数一致
        env.setParallelism(2);

        SingleOutputStreamOperator<Bean1> stream1 = env.socketTextStream("localhost", 8888).map(line -> {
            String[] split = line.split(",");
            return new Bean1(split[0], split[1], Long.parseLong(split[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
            @Override
            public long extractTimestamp(Bean1 bean1, long l) {
                return bean1.getTs() * 1000L;
            }
        }));

        SingleOutputStreamOperator<Bean2> stream2 = env.socketTextStream("localhost", 8889).map(line -> {
            String[] split = line.split(",");
            return new Bean2(split[0], split[1], Long.parseLong(split[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
            @Override
            public long extractTimestamp(Bean2 bean2, long l) {
                return bean2.getTs() * 1000L;
            }
        }));

        //todo 双流 join watermark按照双流中最小的值作为join后的流的watermark值（watermark传递问题）
        SingleOutputStreamOperator<Tuple2<Bean1, Bean2>> joinDS = stream1.keyBy(Bean1::getId).
                intervalJoin(stream2.keyBy(Bean2::getId))
                .between(Time.seconds(-5), Time.seconds(5))
//                .lowerBoundExclusive()   左开( ]   两个都不加为[ ],两个都加( )
//                .upperBoundExclusive() 右开[ )
                .process(new ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>() {
                    @Override
                    public void processElement(Bean1 left, Bean2 right, ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>.Context context, Collector<Tuple2<Bean1, Bean2>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        joinDS.print();

        env.execute("FlinkDataStreanJoinTest");
    }
}
