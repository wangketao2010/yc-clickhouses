package com.yc.clickhouse.service.impl;

import com.yc.clickhouse.service.ClickHouseService;
import com.yc.clickhouse.dao.ClickHouseDaoBase;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * clickHouse 服务实现类
 *
 * @param <T>
 */
@Component
public class ClickHouseServiceImpl<T> implements ClickHouseService<T> {

    @Resource
    private ClickHouseDaoBase clickHouseDaoBase;

    /**
     * 根据传入SQL条件，返回集合数据
     *
     * @param clazz
     * @param sql
     * @return
     */
    @Override
    public List<T> selectList(Class<T> clazz, String sql) {
        return clickHouseDaoBase.selectListObj(sql,null);
    }

    /**
     * 根据传入SQL条件，返回一条数据
     *
     * @param clazz
     * @param sql
     * @return
     */
    @Override
    public T selectInfo(Class<T> clazz, String sql) {
        return (T) clickHouseDaoBase.selectOne(sql,null);
    }

    /**
     * 批量插入实体集合对象数据
     *
     * @param list
     * @param tableName
     */
    @Override
    public void batchInsert(List<T> list, String tableName) {
        clickHouseDaoBase.batchInsert(list);
    }

    /**
     * 根据传入SQL执行操作（如：删除表，删除数据，创建表）
     *
     * @param sql
     */
    @Override
    public void executeBySql(String sql) {
        clickHouseDaoBase.executeInsertUpdateDelete(sql);
    }

    /**
     * 根据传入SQL条件以及参数，返回集合数据
     *
     * @param clazz
     * @param sql
     * @param params
     * @return
     */
    @Override
    public List<T> selectList(Class<T> clazz, String sql, Object[] params) {
        return clickHouseDaoBase.selectListObj(sql, params);
    }

    /**
     * 根据传入SQL条件以及参数，返回一条数据
     *
     * @param clazz
     * @param sql
     * @param params
     * @return
     */
    @Override
    public T selectInfo(Class<T> clazz, String sql, Object[] params) {
        return (T) clickHouseDaoBase.selectOne( sql, params);
    }

    /**
     * 根据传入SQL条件以及参数，返回List对象Map集合数据
     *
     * @param sql
     * @param params
     * @return
     */
    @Override
    public List<Map<String, Object>> selectList(String sql, Object[] params) {
        return clickHouseDaoBase.selectListObj(sql, params);
    }

    /**
     * 批量插入List对象集合map数据
     *
     * @param list
     * @param tableName
     */
    @Override
    public void batchInsertExt(List<Map<String, Object>> list, String tableName) {
        clickHouseDaoBase.batchInsertExt(list);
    }
}
