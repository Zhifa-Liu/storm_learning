package cn.edu.neu.advertise;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author 32098
 * 最近1分钟广告总点击量,每10s计算一次
 */
public class ClickNearlyMinuteBoltA extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            String value = input.getStringByField("value");
            // 必须ack，否则会重复消费kafka中的消息
            collector.ack(input);
            System.out.println("Received from kafka: "+ value);
            String[] strs = value.split(" ");
            collector.emit(new Values(strs[0], strs[4]));
        }catch (Exception e){
            e.printStackTrace();
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time_stamp", "aid"));
    }
}

