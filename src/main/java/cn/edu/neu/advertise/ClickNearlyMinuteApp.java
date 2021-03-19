package cn.edu.neu.advertise;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;

import java.util.concurrent.TimeUnit;

/**
 * @author 32098
 */
public class ClickNearlyMinuteApp {
    private static final String BOOTSTRAP_SERVERS = "master:9092";
    private static final String TOPIC_NAME = "advertise-user";

    public static void main(String[] args) {

        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig()), 1);
        builder.setBolt("boltA", new ClickNearlyMinuteBoltA()).shuffleGrouping("kafka_spout");
        builder.setBolt("boltB", new ClickNearlyMinuteBoltB().withWindow(new Duration(60, TimeUnit.SECONDS), new Duration(10, TimeUnit.SECONDS)),1).setNumTasks(1).fieldsGrouping("boltA", new Fields("time_stamp", "aid"));

        Config config = new Config();
        config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 70000);
        // 如果外部传参cluster则代表线上环境启动,否则代表本地启动
        if (args.length > 0 && "cluster".equals(args[0])) {
            try {
                StormSubmitter.submitTopology("Cluster-ClickNearlyMinuteApp", config, builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Local-ClickNearlyMinuteApp",
                    config, builder.createTopology());
        }
    }

    private static KafkaSpoutConfig<String, String> getKafkaSpoutConfig() {
        return KafkaSpoutConfig.builder(ClickNearlyMinuteApp.BOOTSTRAP_SERVERS, ClickNearlyMinuteApp.TOPIC_NAME)
                // 除了分组ID,以下配置都是可选的。分组ID必须指定,否则会抛出InvalidGroupIdException异常
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
                // 定义重试策略
                .setRetry(getRetryService())
                // 定时提交偏移量的时间间隔,默认是15s
                .setOffsetCommitPeriodMs(10_000)
                .build();
    }

    /**
     * 定义重试策略
     * @return KafkaSpoutRetryService
     */
    private static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }
}
