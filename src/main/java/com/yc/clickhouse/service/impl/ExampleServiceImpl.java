package com.yc.clickhouse.service.impl;

import com.yc.clickhouse.dao.TDao;
import com.yc.clickhouse.entity.T;
import com.yc.clickhouse.service.ExampleService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * clickHouse 服务实现类
 *
 */
@Component
public class ExampleServiceImpl implements ExampleService {

    @Resource
    private TDao tDao;

    /**
     * 根据传入SQL条件，返回集合数据
     *
     * @param sql
     * @return
     */
    @Override
    public List<T> selectList( String sql) {
        return tDao.selectListObj(sql,null);
    }

    /**
     * 根据传入SQL条件，返回一条数据
     *
     * @param sql
     * @return
     */
    @Override
    public T selectInfo( String sql) {
        return (T) tDao.selectOne(sql,null);
    }

    /**
     * 批量插入实体集合对象数据
     *
     * @param list
     * @param tableName
     */
    @Override
    public void batchInsert(List<T> list, String tableName) {
        tDao.batchInsert(list);
    }

    /**
     * 根据传入SQL执行操作（如：删除表，删除数据，创建表）
     *
     * @param sql
     */
    @Override
    public void executeBySql(String sql) {
        tDao.executeInsertUpdateDelete(sql);
    }

    /**
     * 根据传入SQL条件以及参数，返回集合数据
     *
     * @param sql
     * @param params
     * @return
     */
    @Override
    public List<T> selectListObj( String sql, Object[] params) {
        return tDao.selectListObj(sql, params);
    }

    /**
     * 根据传入SQL条件以及参数，返回一条数据
     *
     * @param sql
     * @param params
     * @return
     */
    @Override
    public T selectInfo( String sql, Object[] params) {
        return tDao.selectOne( sql, params);
    }

    /**
     * 根据传入SQL条件以及参数，返回List对象Map集合数据
     *
     * @param sql
     * @param params
     * @return
     */
    @Override
    public List<Map<String, Object>> selectListMap(String sql, Object[] params) {
        return tDao.selectListMap(sql, params);
    }

    /**
     * 批量插入List对象集合map数据
     *
     * @param list
     * @param tableName
     */
    @Override
    public void batchInsertExt(List<Map<String, Object>> list, String tableName) {
        tDao.batchInsertExt(list);
    }
}
