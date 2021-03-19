package cn.edu.neu.kafka.write;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.*;

/**
 * 产生词频样本的数据源
 * @author 32098
 */
public class DataSourceSpout extends BaseRichSpout {

    private final List<String> list = Arrays.asList("Spark", "Hadoop", "HBase", "Storm", "Flink", "Hive");

    private SpoutOutputCollector spoutOutputCollector;

    /**
     * open()方法中是在ISpout接口中定义，在Spout组件初始化时被调用。
     * 有三个参数:
     * 1.Storm配置的Map;
     * 2.topology中组件的信息;
     * 3.发射tuple的方法;
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    /**
     * nextTuple()方法是Spout实现的核心。
     * 也就是主要执行方法，用于输出信息,通过collector.emit方法发射。
     */
    @Override
    public void nextTuple() {
        // 模拟产生数据
        String lineData = productData();
        spoutOutputCollector.emit(new Values("key",lineData));
        Utils.sleep(1000);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields("key", "message"));
    }

    /**
     * 模拟数据
     */
    private String productData() {
        Collections.shuffle(list);
        Random random = new Random();
        int endIndex = random.nextInt(list.size()) % (list.size()) + 1;
        return StringUtils.join(list.toArray(), "\t", 0, endIndex);
    }
}
