package com.yc.clickhouse.service.impl;

import com.yc.clickhouse.dao.TDao;
import com.yc.clickhouse.entity.T;
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
    private TDao tDao;

    @Override
    public void test() {
        String sql = "SELECT * FROM t_test";
        List<T> a = tDao.selectListObj(sql, null);
        T obj = tDao.selectOne(sql, null);
        List<T> list = new ArrayList<>();
        tDao.batchInsert(list);
        tDao.executeInsertUpdateDelete(sql);
        Object[] params = new Object[]{1, 2};
        tDao.selectListObj(sql, params);
        params = new Object[]{3, 2};
        List<Map<String, Object>> b = tDao.selectListMap(sql, params);
        List<Map<String, Object>> list1 = new ArrayList<>();
        tDao.batchInsertExt(list1);
    }


}
