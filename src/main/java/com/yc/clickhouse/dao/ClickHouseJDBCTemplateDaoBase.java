package com.yc.clickhouse.dao;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 功能描述
 *
 * @author wangkt
 * @date 2020/11/11
 */
@Slf4j
public class ClickHouseJDBCTemplateDaoBase<T> {
    private Lock lock = new ReentrantLock();// 可重入锁
    private Class<T> tClass;// 当前实体类对应泛型

    /**
     * 获取T的class实例
     *
     * @return
     */
    private Class<T> gettClass() {
        if (tClass == null) {
            lock.lock();
            try {
                if (tClass == null) {
                    tClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
                }
            } finally {
                lock.unlock();
            }
        }
        return tClass;
    }
    @Autowired
    private JdbcTemplate jdbcTemplate;
    public List<T> selectListObj(String sql, Object[] params){
//        jdbcTemplate.query(sql,params);
        List a=  jdbcTemplate.queryForList(sql,params);
        return null;
    }

}
