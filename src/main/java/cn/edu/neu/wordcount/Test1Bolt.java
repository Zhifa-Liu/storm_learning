package cn.edu.neu.wordcount;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 *
 * Title: TestBolt
 * Description:
 * 对单词进行分割
 * Version:1.0.0
 * @author peiyy
 * @date 2021年3月6日
 */
public class Test1Bolt extends BaseRichBolt{

    /**
     *
     */
    private static final long serialVersionUID = 4743224635827696343L;

    private OutputCollector collector;

    /**
     * 在Bolt启动前执行，提供Bolt启动环境配置的入口
     * 一般对于不可序列化的对象进行实例化。
     * 注:如果是可以序列化的对象，那么最好是使用构造函数。
     */
    @Override
    public void prepare(Map map, TopologyContext arg1, OutputCollector collector) {
        System.out.println("prepare:"+map.get("test"));
        this.collector=collector;
    }

    /**
     * execute()方法是Bolt实现的核心。
     * 也就是执行方法，每次Bolt从流接收一个订阅的tuple，都会调用这个方法。
     */
    @Override
    public void execute(Tuple tuple) {
        String msg=tuple.getStringByField("sentence");
        System.out.println("开始分割:"+msg);
        String[] words = msg.toLowerCase().split(" ");
        for (String word : words) {
            // 向下一个bolt发射数据
            this.collector.emit(new Values(word));
        }
    }

    /**
     * 声明数据格式
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    /**
     * cleanup是IBolt接口中定义,用于释放bolt占用的资源。
     * Storm在终止一个bolt之前会调用这个方法。
     */
    @Override
    public void cleanup() {
        System.out.println("Test1Bolt的资源释放");
    }
}
