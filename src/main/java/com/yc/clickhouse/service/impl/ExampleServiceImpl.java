package com.yc.clickhouse.service.impl;

import com.yc.clickhouse.dao.TTestDao;
import com.yc.clickhouse.po.TTest;
import com.yc.clickhouse.service.ExampleService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * clickHouse 服务实现类
 */
@Component
public class ExampleServiceImpl implements ExampleService {

    @Resource
    private TTestDao tTestDao;

    @Override
    public void test() {
        /**
         *  查
         */
        Object[] params = null;
        String sql = "SELECT * FROM t_test";
        List<TTest> l = tTestDao.selectListObj(sql, params);

        params = null;
        sql = "SELECT * FROM t_test";
        List<Map<String, Object>> maps = tTestDao.selectListMap(sql, params);

        params = null;
        sql = "SELECT * FROM t_test";
        List list = tTestDao.execute(sql);

        sql = "where id = ?";
        params = new Object[]{1};
        int a = tTestDao.selectCount(sql,params);

        sql = "SELECT * FROM t_test";
        int b = tTestDao.selectCount(sql);

        sql = "where id = ?";
        params = new Object[]{1};
        l = tTestDao.selectPage(0,10,sql,params);

        sql = "SELECT * FROM t_test where id = ?";
        params = new Object[]{1};
        TTest obj = tTestDao.selectOne(sql, params);

        /**
         * gai
         */
//        List<TTest> list = new ArrayList<>();
//        tTestDao.batchInsert(list);
//        tTestDao.executeInsertUpdateDelete(sql);
//        List<Map<String, Object>> list1 = new ArrayList<>();
//        tTestDao.batchInsertExt(list1);
    }


}
