package cn.edu.neu.advertise;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @author 32098
 * 实现实时的动态黑名单机制: 把每天对某个广告点击超过100次的用户拉黑，黑名单用户ID存入Mysql
 */
public class BlackListBoltB extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String ts = tuple.getStringByField("time_stamp");
        String uid = tuple.getStringByField("uid");
        String aid = tuple.getStringByField("aid");
        String value = "1";
        collector.ack(tuple);

        try {
            Connection conn = MysqlUtil.getConnection();
            assert conn != null;
            PreparedStatement ps = conn.prepareStatement("select uid from black_list where uid=?");
            ps.setString(1, uid);
            ResultSet rs = ps.executeQuery();
            if(!rs.next()){
                String day = new SimpleDateFormat("yyyy-MM-dd").format(new Date(Long.parseLong(ts.trim())));
                ps = conn.prepareStatement(
                        "select * from user_advertise where day=? and uid=? and aid=?"
                );
                ps.setString(1, day);
                ps.setString(2, uid);
                ps.setString(3, aid);
                rs = ps.executeQuery();
                if(rs.next()){
                    PreparedStatement psA = conn.prepareStatement(
                            "update user_advertise set count = count + ? where day=? and uid=? and aid=?"
                    );
                    // psA.setInt(1, 1);
                    psA.setString(1, value);
                    psA.setString(2, day);
                    psA.setString(3, uid);
                    psA.setString(4, aid);
                    psA.executeUpdate();
                    psA.close();
                }else{
                    PreparedStatement psB = conn.prepareStatement("insert into user_advertise(day,uid,aid,count) values (?,?,?,?)");
                    psB.setString(1, day);
                    psB.setString(2, uid);
                    psB.setString(3, aid);
                    psB.setString(4, value);
                    psB.executeUpdate();
                    psB.close();
                }
                ps = conn.prepareStatement(
                        "select * from user_advertise where day=? and uid=? and aid=? and count>60"
                );
                ps.setString(1, day);
                ps.setString(2, uid);
                ps.setString(3, aid);
                rs = ps.executeQuery();
                if(rs.next()){
                    PreparedStatement psC = conn.prepareStatement("insert into black_list(uid) value(?) on duplicate key update uid=?");
                    psC.setString(1, uid);
                    psC.setString(2, uid);
                    psC.executeUpdate();
                    psC.close();
                }
                ps.close();
            }
            conn.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
