package cn.edu.neu.advertise;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @author 32098
 */
public class ClickCountBoltB extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String ts = tuple.getStringByField("time_stamp");
        String province = tuple.getStringByField("province");
        String city = tuple.getStringByField("city");
        String aid = tuple.getStringByField("aid");
        String value = "1";
        collector.ack(tuple);

        String day = new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.parseLong(ts.trim())));

        try {
            Connection conn = MysqlUtil.getConnection();
            assert conn != null;
            PreparedStatement ps = conn.prepareStatement(
                    "insert into province_city_advertise(day,province,city,aid,count) values (?,?,?,?,?) on duplicate key update count=count+?"
            );
            ps.setString(1, day);
            ps.setString(2, province);
            ps.setString(3, city);
            ps.setString(4, aid);
            ps.setString(5, value);
            ps.setString(6, value);
            ps.executeUpdate();
            ps.close();
            conn.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
