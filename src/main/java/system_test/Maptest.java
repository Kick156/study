package system_test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Maptest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        DataStreamSource<Event> stream = eventDataStreamSource;

        // 进行转换计算，提取user字段
        // 1.使用自定义类，实现MapFunction接口
        SingleOutputStreamOperator<String> result1 = stream.map(new UserExtractor());



        // 2.使用匿名类，实现MapFunction
        SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event e) throws Exception {
                return e.user;
            }
        });

        // 3.传入Lambda表达式
        SingleOutputStreamOperator<String> result3 = stream.map(data -> data.user);
        SingleOutputStreamOperator<String> result4 = stream.map(data -> data.user + "2");

        result4.print();

        env.execute();
    }
    // 自定义MapFunction
    public static class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event e) throws Exception {
            return e.user;
        }
    }
}
