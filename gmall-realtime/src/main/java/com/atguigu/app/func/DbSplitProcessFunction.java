package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class DbSplitProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    //定义属性
    private OutputTag<JSONObject> outputTag;

    //定义配置信息的Map
    private HashMap<String, TableProcess> tableProcessHashMap;

    //定义Set用于记录当前Phoenix中已经存在的表
    private HashSet<String> existTables;

    //定义Phoenix的连接
    private Connection connection = null;

    public DbSplitProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        //初始化配置信息的Map
        tableProcessHashMap = new HashMap<>();

        //初始化Phoenix已经存在的表的Set
        existTables = new HashSet<>();

        //初始化Phoenix的连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //读取配置信息
        refreshMeta();

        //开启定时调度任务,周期性执行读取配置信息方法
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        }, 10000L, 5000L);
    }

    /**
     * 周期性调度的方法
     * 1.读取MySQL中的配置信息
     * 2.将查询结果封装为Map,以便于后续每条数据获取
     * 3.检查Phoenix中是否存在该表,如果不存在,则在Phoenix中创建该表
     */
    private void refreshMeta() {

        System.out.println("开始读取MySQL配置信息！");

        //1.读取MySQL中的配置信息
        List<TableProcess> tableProcesses = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);

        //2.将查询结果封装为Map,以便于后续每条数据获取
        for (TableProcess tableProcess : tableProcesses) {

            //获取源表
            String sourceTable = tableProcess.getSourceTable();
            //获取操作类型
            String operateType = tableProcess.getOperateType();

            String key = sourceTable + ":" + operateType;
            tableProcessHashMap.put(key, tableProcess);

            //3.检查Phoenix中是否存在该表,如果不存在,则在Phoenix中创建该表
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {

                //校验Phoenix中是否已经存在该表
                boolean notExist = existTables.add(tableProcess.getSinkTable());

                if (notExist) {
                    checkTable(tableProcess.getSinkTable(),
                            tableProcess.getSinkColumns(),
                            tableProcess.getSinkPk(),
                            tableProcess.getSinkExtend());
                }
            }
        }

        //校验
        if (tableProcessHashMap == null || tableProcessHashMap.size() == 0) {
            throw new RuntimeException("读取MySQL配置信息失败！");
        }
    }

    /**
     * Phoenix建表
     *
     * @param sinkTable   表名       test
     * @param sinkColumns 表名字段   id,name,sex
     * @param sinkPk      表主键     id
     * @param sinkExtend  表扩展字段 ""
     *                    create table if not exists mydb.test(id varchar primary key,name varchar,sex varchar) ...
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        //给主键以及扩展字段赋默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        //封装建表SQL
        StringBuilder createSql = new StringBuilder("create table if not exists ").append(GmallConfig.HBASE_SCHEMA).append(".").append(sinkTable).append("(");
        //遍历添加字段信息
        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {

            //取出字段
            String field = fields[i];

            //判断当前字段是否为主键
            if (sinkPk.equals(field)) {
                createSql.append(field).append(" varchar primary key ");
            } else {
                createSql.append(field).append(" varchar ");
            }

            //如果当前字段不是最后一个字段,则追加","
            if (i < fields.length - 1) {
                createSql.append(",");
            }
        }

        createSql.append(")");
        createSql.append(sinkExtend);

        System.out.println(createSql);

        //执行建表SQL
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createSql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建Phoenix表" + sinkTable + "失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
        collector.collect(jsonObject);
    }
}
