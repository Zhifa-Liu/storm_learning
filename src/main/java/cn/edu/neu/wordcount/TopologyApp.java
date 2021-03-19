package cn.edu.neu.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 *
 * Title: TopologyApp
 * Description:
 * storm wordcount 测试
 * Version:1.0.0
 * @author peiyy
 * @date 2021年3月6日
 */
public class TopologyApp {

    private static final String TEST_SPOUT ="test_spout";
    private static final String TEST1_BOLT ="test1_bolt";
    private static final String TEST2_BOLT ="test2_bolt";

    public static void main(String[] args){
        // 定义一个拓扑
        TopologyBuilder builder = new TopologyBuilder();
        // 设置Spout数据源: 两个Executeor(线程)，默认一个
        builder.setSpout(TEST_SPOUT, new TestSpout(),1);
        // shuffleGrouping:表示是随机分组
        builder.setBolt(TEST1_BOLT, new Test1Bolt(),2).setNumTasks(2).shuffleGrouping(TEST_SPOUT);
        // fieldsGrouping:表示是按字段分组
        builder.setBolt(TEST2_BOLT, new Test2Bolt(),2).setNumTasks(2).fieldsGrouping(TEST1_BOLT, new Fields("word"));

        Config conf = new Config();
        conf.put("test", "test");
        try{
            // 运行拓扑：有参数时，表示向集群提交作业，并把第一个参数当做topology名称；没有参数时，本地提交
            if(args != null && args.length>0){
                System.out.println("运行远程模式");
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } else{
                // 启动本地模式
                System.out.println("运行本地模式");
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("Word-counts", conf, builder.createTopology());
                Thread.sleep(20000);
                cluster.killTopology("Word-counts");
                // 关闭本地集群
                cluster.shutdown();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
