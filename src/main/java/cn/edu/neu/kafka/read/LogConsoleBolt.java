package cn.edu.neu.kafka.read;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * 打印从Kafka中获取的数据
 * @author 32098
 */
public class LogConsoleBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            String value = input.getStringByField("value");
            System.out.println("Received from kafka : "+ value);
            // 必须ack, 否则会重复消费kafka中的消息
            collector.ack(input);
        }catch (Exception e){
            e.printStackTrace();
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
