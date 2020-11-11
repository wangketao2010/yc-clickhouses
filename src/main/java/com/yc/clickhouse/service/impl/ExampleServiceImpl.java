package com.yc.clickhouse.service.impl;

import com.yc.clickhouse.dao.TTestDao;
import com.yc.clickhouse.entity.TTest;
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
        String sql = "SELECT * FROM t_test";
        List<TTest> a = tTestDao.selectListObj(sql, null);
        TTest obj = tTestDao.selectOne(sql, null);
        List<TTest> list = new ArrayList<>();
        tTestDao.batchInsert(list);
        tTestDao.executeInsertUpdateDelete(sql);
        Object[] params = new Object[]{1, 2};
        tTestDao.selectListObj(sql, params);
        params = new Object[]{3, 2};
        List<Map<String, Object>> b = tTestDao.selectListMap(sql, params);
        List<Map<String, Object>> list1 = new ArrayList<>();
        tTestDao.batchInsertExt(list1);
    }


}
