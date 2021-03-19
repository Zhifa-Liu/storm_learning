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
 * 实现实时的动态黑名单机制: 把每天对某个广告点击超过100次的用户拉黑，黑名单用户ID存入Mysql
 */
public class BlackListBoltA extends BaseRichBolt {
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
            collector.emit(new Values(strs[0], strs[3], strs[4]));
        }catch (Exception e){
            e.printStackTrace();
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time_stamp", "uid", "aid"));
    }
}
