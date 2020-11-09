package com.yc.clickhouse.dao;

import com.yc.clickhouse.config.annotation.ClickHouseTable;
import com.yc.clickhouse.constant.Constant;
import com.yc.clickhouse.generator._BaseEntity;
import com.yc.clickhouse.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.sql.DataSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhanglei
 * @date 2019-04-15
 * @desc clickHouse连接配置信息处理类
 */
@Slf4j
@Component
public class ClickHouseDaoBase<T extends _BaseEntity> {
    private Lock lock = new ReentrantLock();// 可重入锁
    private Class<T> tClass;// 当前实体类对应泛型

    /**
     * 注入数据源
     */
    @Resource(name = "dataSource")
    private DataSource clickHouseDatasource;

    /**
     * 获取创建CK连接
     *
     * @return
     * @throws SQLException
     */
    private Connection getConnection() {
        Connection connection = null;
        try {
            connection = clickHouseDatasource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("连接clickHouse服务异常：{}", e.getMessage());
        }
        return connection;
    }
    //################################# merginng #################################
    /**
     * 获取当前实体类对应的表名
     * @return
     */
    private String getTableName(){
        ClickHouseTable table =  this.getTClass().getAnnotation(ClickHouseTable.class);
        if(table == null){
            throw new RuntimeException(" no Annotation 'javax.persistence.Table' in clazz  ");
        }
        return table.name();
    }
    /**
     * 获取T的class实例
     * @return
     */
    private Class<T> getTClass(){
        if(tClass == null){
            lock.lock();
            try {
                if(tClass == null){
                    tClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
                }
            }finally {
                lock.unlock();
            }
        }
        return tClass;
    }
    /**
     * 不同于普通SQL的修改和删除，alter table [tableName] update……where……
     * 根据构造的对象，以ID为条件来修改
     * @param entity：只针对注解声明的属性
     * @return
     */
    public int updateById(T entity){
        if(entity == null){
            throw new RuntimeException(" entity is null ");
        }
        // 构建修改语句sql
        StringBuffer sql = new StringBuffer("alter table ").append(this.getTableName()).append(" update ");
        String where = "";

        // 获取类的所有字段，并逐一填充sql，只针对注解声明的属性,
        Field[] fieldList = this.getAllFieldList();
        List<Object> setValue = new ArrayList<>();
        for(Field field:fieldList){
            ClickHouseColumn column = field.getAnnotation(ClickHouseColumn.class);
            if(column != null){
                Object getValue = this.getMethodGetValue(field.getName(), entity);//反射获取到属性的值

                // ID条件判断
                ClickHouseID id = field.getAnnotation(ClickHouseID.class);
                if(id != null){
                    where = " where "+column.name()+" = "+getValue;
                }else{
                    sql.append(column.name()).append(" = ?,");
                    setValue.add(getValue);
                }
            }
        }

        // 完成SQL的构建
        sql.deleteCharAt(sql.length()-1).append(where);

        logger.info(sql.toString());

        // 开始修改
        Connection connection = getConnection();
        try {
            PreparedStatement pst = connection.prepareStatement(sql.toString());
            for(Object value:setValue){
                int index = 1;//preparedStatement的占位
                pst.setObject(index,value);
                index++;
            }
            int res = pst.executeUpdate();//提交修改
            connection.commit();// 执行
            logger.info("修改结果：[{}]",res);
            return res;
        } catch (Exception e) {
            try {
                connection.rollback();//异常回滚
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }
        return 0;
    }
    /**
     * 指定的SQL语句修改，一般是范围修改
     * @param sql
     * @return
     */
    public int updateBySql(String sql){
        log.info(sql);

        Connection connection = getConnection();
        try {
            Statement statement = getConnection().createStatement();
            int update = statement.executeUpdate(sql);
            connection.commit();// 执行
            return update;
        } catch (Exception e) {
            try {
                connection.rollback();//异常回滚
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }
        return 0;
    }
    /**
     * 不同于普通SQL的修改和删除，alter table [tableName] delete where……
     * 根据ID删除，要获取被ID注解声明的字段名称
     * @param id
     * @return
     */
    public int deleteById(Object id){
        String columnID = "";//字段ID
        Field[] fieldList = this.getAllFieldList();
        for(Field field:fieldList){
            ClickHouseID annotationID = field.getAnnotation(ClickHouseID.class);
            if(annotationID != null){
                ClickHouseColumn annotationColumn = field.getAnnotation(ClickHouseColumn.class);
                columnID = annotationColumn.name();
            }
        }
        String sql = "alter table "+this.getTableName()+" delete where "+columnID+" = ?";

        logger.info(sql);

        Connection connection = getConnection();
        try {
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setObject(1,id);
            int res = pst.executeUpdate();//提交删除
            connection.commit();// 执行
            return res;
        } catch (Exception e) {
            try {
                connection.rollback();//异常回滚
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }
        return 0;
    }
    /**
     * 根据SQL语句来删除
     * @param sql
     * @return
     */
    public int deleteBySql(String sql){
        logger.info(sql);

        Connection connection = getConnection();
        try {
            Statement statement = getConnection().createStatement();
            int update = statement.executeUpdate(sql);
            connection.commit();// 执行
            return update;
        } catch (Exception e) {
            try {
                connection.rollback();//异常回滚
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }
        return 0;
    }
    /**
     * 构建条件，获取统计数量
     * @param entity
     * @return
     */
    public int selectCount(T entity){
        if(entity == null){
            return this.selectCountAll();
        }
        // 构建条件对象
        ConditionEntity conditionEntity = this.getWhereSqlAndValue(entity, CONDITION_EQUAL);

        // 构建SQL
        StringBuffer sql = new StringBuffer("select count(*) as count from ").append(this.getTableName());
        sql.append(" where ").append(conditionEntity.getSql());

        logger.info(sql.toString());

        try {
            PreparedStatement preparedStatement = this.createPreparedStatement(sql.toString(), conditionEntity.getValues());
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                return resultSet.getInt("count");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
    public int selectCount(String sql) {
        sql = "select count(t.*) as count from ("+sql+") t";//手动构造统计

        logger.info(sql);

        ResultSet resultSet = this.executeQuery(sql);
        if(resultSet != null){
            try {
                if(resultSet.next()){
                    return resultSet.getInt("count");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return 0;
    }
    /**
     * 分页查询，构造条件，排序
     * @param start
     * @param size
     * @param entity
     * @param orderByFieldAndIsAsc：eg：field asc/desc
     * @return
     */
    public List<T> selectPage(int start,int size,T entity,String orderByFieldAndIsAsc){
        if(entity == null){
            return this.selectPage(start,size,orderByFieldAndIsAsc);
        }
        // 构造条件对象
        ConditionEntity conditionEntity = this.getWhereSqlAndValue(entity, CONDITION_EQUAL);

        // 构造SQL语句
        StringBuffer sql = new StringBuffer("select * from ").append(this.getTableName());
        sql.append(" where ").append(conditionEntity.getSql());
        if(orderByFieldAndIsAsc != null && orderByFieldAndIsAsc != ""){
            sql.append(" order by ").append(orderByFieldAndIsAsc);
        }
        sql.append(" limit ").append(start).append(",").append(size);

        logger.info(sql.toString());

        try {
            PreparedStatement preparedStatement = this.createPreparedStatement(sql.toString(), conditionEntity.getValues());
            ResultSet resultSet = preparedStatement.executeQuery();
            return this.convertSetToEntity(resultSet);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<T>(0);//直接返回空list，防止NullPointException
    }
    //################################# merginng #################################

    /**
     * 根据传入SQL条件，查询对象集合数据
     *
     * @param clazz
     * @param sql
     * @return
     */
    public List<T> selectList(Class<T> clazz, String sql) {
        log.info("clickHouse 查询集合数据执行sql：" + sql);
        Statement statement = null;
        ResultSet results = null;
        Connection conn = null;
        try {
            conn = getConnection();
            statement = conn.createStatement();
            results = statement.executeQuery(sql.toString());
            List<T> list = mapResultSetToObject(results, clazz);
            if (list != null) {
                log.debug("查询出数据size：{}", list.size());
            } else {
                log.debug("ResultSet is empty. Please check if database table is empty");
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {//关闭连接
            close(statement, conn, results);
        }
        return null;
    }

    /**
     * 添加传入参数
     *
     * @param statement
     * @param params
     * @throws SQLException
     */
    private void putParams(PreparedStatement statement, Object[] params) throws SQLException {
        if (params != null && params.length > 0) {
            for (int i = 0; i < params.length; i++) {
                if (params[i] instanceof Integer) {
                    statement.setInt(i + 1, (Integer) params[i]);
                } else if (params[i] instanceof Long) {
                    statement.setLong(i + 1, (Long) params[i]);
                } else if (params[i] instanceof String) {
                    statement.setString(i + 1, params[i].toString());
                } else if (params[i] instanceof Double) {
                    statement.setDouble(i + 1, (Double) params[i]);
                } else if (params[i] instanceof BigDecimal) {
                    statement.setBigDecimal(i + 1, (BigDecimal) params[i]);
                } else if (params[i] instanceof Float) {
                    statement.setFloat(i + 1, (Float) params[i]);
                } else {
                    statement.setObject(i + 1, params);
                }
            }
        }
    }

    /**
     * 根据传入SQL条件以及参数，查询集合数据
     *
     * @param clazz
     * @param sql
     * @param params
     * @return
     */
    public List<T> selectList(Class<T> clazz, String sql, Object[] params) {
        log.info("clickHouse 查询集合数据执行sql：" + sql);
        PreparedStatement statement = null;
        ResultSet results = null;
        Connection conn = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(sql);
            putParams(statement, params);
            results = statement.executeQuery();
            List<T> list = mapResultSetToObject(results, clazz);
            if (list != null) {
                log.debug("查询出数据size：{}", list.size());
            } else {
                log.debug("ResultSet is empty. Please check if database table is empty");
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {//关闭连接
            close(statement, conn, results);
        }
        return null;
    }

    /**
     * 根据传入SQL条件以及参数，返回List对象Map集合数据
     *
     * @param sql
     * @param params
     * @return
     */
    public List<Map<String, Object>> selectList(String sql, Object[] params) {
        log.info("clickHouse 查询集合数据执行sql：" + sql);
        PreparedStatement statement = null;
        ResultSet results = null;
        Connection conn = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(sql);
            putParams(statement, params);
            results = statement.executeQuery();
            List<Map<String, Object>> list = mapResultSet(results);
            if (list != null) {
                log.debug("查询出数据size：{}", list.size());
            } else {
                log.debug("ResultSet is empty. Please check if database table is empty");
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {//关闭连接
            close(statement, conn, results);
        }
        return null;
    }

    /**
     * 根据传入SQL条件，返回一条对象数据
     *
     * @param clazz
     * @param sql
     * @return
     */
    public T selectOne(Class<T> clazz, String sql) {
        log.info("clickHouse 查询单条数据执行sql：" + sql);
        Statement statement = null;
        ResultSet results = null;
        Connection conn = null;
        try {
            conn = getConnection();
            statement = conn.createStatement();
            results = statement.executeQuery(sql.toString());
            if (Number.class.isAssignableFrom(clazz) || Date.class.isAssignableFrom(clazz) ||
                    String.class.isAssignableFrom(clazz)) {
                if (results.next()) {
                    return (T) results.getObject(1);
                }
            } else {
                List<T> list = mapResultSetToObject(results, clazz);
                return list == null || list.size() == 0 ? null : list.get(0);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {//关闭连接
            close(statement, conn, results);
        }
        return null;
    }

    /**
     * 根据传入SQL条件以及参数，返回一条对象数据
     *
     * @param clazz
     * @param sql
     * @param params
     * @return
     */
    public T selectOne(Class<T> clazz, String sql, Object[] params) {
        log.info("clickHouse 查询单条数据执行sql：" + sql);
        PreparedStatement statement = null;
        ResultSet results = null;
        Connection conn = null;
        try {
            conn = getConnection();
            statement = conn.prepareStatement(sql);
            putParams(statement, params);
            results = statement.executeQuery();
            if (Number.class.isAssignableFrom(clazz) || Date.class.isAssignableFrom(clazz) ||
                    String.class.isAssignableFrom(clazz)) {
                if (results.next()) {
                    return (T) results.getObject(1);
                }
            } else {
                List<T> list = mapResultSetToObject(results, clazz);
                return list == null || list.size() == 0 ? null : list.get(0);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {//关闭连接
            close(statement, conn, results);
        }
        return null;
    }

    /**
     * 批量插入实体对象集合数据
     *
     * @param list
     */
    public void batchInsert(List<T> list) {
        String tableName = getTableName();
        if (list == null || list.size() <= 0 || StringUtil.isEmpty(tableName))
            return;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        List<Field> fieldList = new ArrayList<>();
        Field[] fields = null;
        int fieldSize = 0;
        Connection conn = null;
        try {
            Long startTime = System.currentTimeMillis();
            // 此处查询一次，只为了获取对应列名索引，进而插入对应值
            conn = getConnection();
            resultSet = conn.createStatement().executeQuery(Constant.QUERY + Constant.WILDCARD + Constant.FROM + tableName + Constant.LIMIT + 1);
            Map<String, Integer> indexMap = new HashMap<>();
            // 将列名和对应索引存入indexMap
            for (int a = 1; a <= resultSet.getMetaData().getColumnCount(); a++) {
                indexMap.put(resultSet.getMetaData().getColumnName(a), a);
            }
            int batch = 0;
            for (T obj : list) {
                batch++;
                if (null == fields || fieldSize == 0) {
                    StringBuffer sql = new StringBuffer(Constant.ADD + tableName + " values(");
                    Class clazz = obj.getClass();
                    while (clazz != null) {   //当父类为null的时候说明到达了最上层的父类(Object类).
                        fieldList.addAll(Arrays.asList(clazz.getDeclaredFields()));
                        clazz = clazz.getSuperclass(); //得到父类,然后赋给自己
                    }
                    fields = fieldList.toArray(new Field[fieldList.size()]);
                    fieldSize = fields.length;
                    for (int i = 0; i < fieldSize; i++) {
                        if (!fields[i].isAnnotationPresent(Column.class)) {
                            continue;
                        }
                        if (fields[i].getAnnotation(Column.class) == null) {
                            continue;
                        }
                        sql.append("?,");
                    }
                    sql.deleteCharAt(sql.length() - 1);
                    sql.append(")");
                    ps = conn.prepareStatement(sql.toString());
                    log.info("批量插入" + tableName + "打印执行sql: " + sql);
                }

                for (int j = 0; j < fieldSize; j++) {
                    fields[j].setAccessible(true);
                    if (!fields[j].isAnnotationPresent(Column.class)) {
                        continue;
                    }
                    if (fields[j].getAnnotation(Column.class) == null) {
                        continue;
                    }
                    // 获取当前需要插入的列名
                    String columnName = fields[j].getAnnotation(Column.class).name();
                    // 将value set到对应的列位
                    if (StringUtil.isNotNull(fields[j].get(obj))) {
                        if (resultSet.getMetaData().getColumnType(indexMap.get(columnName)) == Types.TIMESTAMP) {
                            if (fields[j].get(obj) instanceof Timestamp) {
                                ps.setTimestamp(indexMap.get(columnName), (Timestamp) fields[j].get(obj));
                            } else if (fields[j].get(obj) instanceof Date) {
                                ps.setTimestamp(indexMap.get(columnName), new java.sql.Timestamp(((Date) fields[j].get(obj)).getTime()));
                            } else {
                                ps.setTimestamp(indexMap.get(columnName), null);
                            }
                        } else if (resultSet.getMetaData().getColumnType(indexMap.get(columnName)) == Types.DATE) {
                            if (fields[j].get(obj) instanceof java.sql.Date) {
                                ps.setDate(indexMap.get(columnName), (java.sql.Date) fields[j].get(obj));
                            } else if (fields[j].get(obj) instanceof Date) {
                                ps.setDate(indexMap.get(columnName), new java.sql.Date(((Date) fields[j].get(obj)).getTime()));
                            } else if (fields[j].get(obj) instanceof String) {
                                ps.setString(indexMap.get(columnName), fields[j].get(obj).toString());
                            } else {
                                ps.setDate(indexMap.get(columnName), null);
                            }
                        } else if (resultSet.getMetaData().getColumnType(indexMap.get(columnName)) == Types.DECIMAL) {
                            if (fields[j].get(obj) instanceof BigDecimal) {
                                ps.setBigDecimal(indexMap.get(columnName), (BigDecimal) fields[j].get(obj));
                            } else if (fields[j].get(obj) instanceof String) {
                                ps.setBigDecimal(indexMap.get(columnName), new BigDecimal(fields[j].get(obj).toString()));
                            } else if (fields[j].get(obj) instanceof Number) {
                                ps.setBigDecimal(indexMap.get(columnName), BigDecimal.valueOf((Double) fields[j].get(obj)));
                            } else {
                                ps.setBigDecimal(indexMap.get(columnName), null);
                            }
                        } else {
                            ps.setObject(indexMap.get(columnName), fields[j].get(obj));
                        }
                    } else {
                        ps.setObject(indexMap.get(columnName), null);
                    }
                }
                ps.addBatch();
                // 每2000插入一次
                if (batch % 2000 == 0) {
                    ps.executeBatch();
                }
            }
            // 插入剩余数量不足2000的
            ps.executeBatch();
            indexMap = null;
            Long endTime = System.currentTimeMillis();
            log.info("集合size：{}，批量插入{}成功,耗时{}ms......", list.size(), tableName, (endTime - startTime));
        } catch (Exception e1) {
            log.error("集合size：{}，批量插入{}异常：{}", list.size(), tableName, e1.getMessage());
        } finally {
            close(ps, conn, resultSet);
        }
    }

    /**
     * 批量插入List对象集合map数据
     *
     * @param list
     */
    public void batchInsertExt(List<Map<String, Object>> list) {
        String tableName = getTableName();
        if (list == null || list.size() <= 0 || StringUtil.isEmpty(tableName))
            return;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        String[] fields = null;
        int fieldSize = 0;
        Connection conn = null;
        try {
            Long startTime = System.currentTimeMillis();
            // 此处查询一次，只为了获取对应列名索引，进而插入对应值
            conn = getConnection();
            resultSet = conn.createStatement().executeQuery(Constant.QUERY + Constant.WILDCARD + Constant.FROM + tableName + Constant.LIMIT + 1);
            Map<String, Integer> indexMap = new HashMap<>();
            // 将列名和对应索引存入indexMap
            for (int a = 1; a <= resultSet.getMetaData().getColumnCount(); a++) {
                indexMap.put(resultSet.getMetaData().getColumnName(a), a);
            }
            int batch = 0;
            Object value;
            for (Map<String, Object> map : list) {
                batch++;
                if (null == fields || fieldSize == 0) {
                    StringBuffer sql = new StringBuffer(Constant.ADD + tableName + " (");
                    fields = map.keySet().toArray(new String[map.keySet().size()]);
                    fieldSize = fields.length;
                    for (int i = 0; i < fieldSize; i++) {
                        sql.append(fields[i] + ",");
                    }
                    sql.deleteCharAt(sql.length() - 1);
                    sql.append(") values(");
                    for (int i = 0; i < fieldSize; i++) {
                        sql.append("?,");
                    }
                    sql.deleteCharAt(sql.length() - 1);
                    sql.append(")");
                    ps = conn.prepareStatement(sql.toString());
                    log.info("批量插入" + tableName + "打印执行sql: " + sql);
                }
                for (int j = 0; j < fieldSize; j++) {
                    // 获取当前需要插入的列名
                    value = map.get(fields[j]);
                    // 将value set到对应的列位
                    if (value instanceof Integer) {
                        ps.setInt(j + 1, (Integer) value);
                    } else if (value instanceof Long) {
                        ps.setLong(j + 1, (Long) value);
                    } else if (value instanceof String) {
                        ps.setString(j + 1, value.toString());
                    } else if (value instanceof Double) {
                        ps.setDouble(j + 1, (Double) value);
                    } else if (value instanceof BigDecimal) {
                        ps.setBigDecimal(j + 1, (BigDecimal) value);
                    } else if (value instanceof Float) {
                        ps.setFloat(j + 1, (Float) value);
                    } else {
                        ps.setObject(j + 1, value);
                    }
                }
                ps.addBatch();
                // 每2000插入一次
                if (batch % 2000 == 0) {
                    ps.executeBatch();
                }
            }
            // 插入剩余数量不足2000的
            ps.executeBatch();
            indexMap = null;
            Long endTime = System.currentTimeMillis();
            log.info("集合size：{}，批量插入{}成功,耗时{}ms......", list.size(), tableName, (endTime - startTime));
        } catch (Exception e1) {
            log.error("集合size：{}，批量插入{}异常：{}", list.size(), tableName, e1);
        } finally {
            close(ps, conn, resultSet);
        }
    }

    /**
     * 根据传入SQL做执行操作（如：删除表，删除数据，创建表）
     *
     * @param sql
     */
    public void executeBySql(String sql) {
        log.info("clickhouse 输出执行sql：" + sql);
        Statement stmt = null;
        Connection conn = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            int count = stmt.executeUpdate(sql);
            if (count > 0) {
                log.info("执行成功！");
            } else {
                log.info("执行失败！");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {//关闭连接
            close(stmt, conn, null);
        }
    }

    /**
     * 将查询的返回结果信息封装实体对象
     *
     * @param rs
     * @param outputClass
     * @return
     */
    @SuppressWarnings("unchecked")
    public List<T> mapResultSetToObject(ResultSet rs, Class outputClass) {
        List<T> outputList = null;
        try {
            // make sure resultset is not null
            if (rs != null) {
                // check if outputClass has 'Entity' annotation
                if (outputClass.isAnnotationPresent(Entity.class)) {
                    // get the resultset metadata
                    ResultSetMetaData rsmd = rs.getMetaData();
                    // get all the attributes of outputClass
//                    Field[] fields = outputClass.getDeclaredFields();
                    Class currClass = outputClass;
                    List<Field> fields = new ArrayList<>();
                    while (outputClass != null) {   //当父类为null的时候说明到达了最上层的父类(Object类).
                        fields.addAll(Arrays.asList(outputClass.getDeclaredFields()));
                        outputClass = outputClass.getSuperclass(); //得到父类,然后赋给自己
                    }
                    while (rs.next()) {
                        T bean = (T) currClass.newInstance();
                        for (int _iterator = 0; _iterator < rsmd.getColumnCount(); _iterator++) {
                            // getting the SQL column name
                            String columnName = rsmd.getColumnName(_iterator + 1);
                            // reading the value of the SQL column
                            Object columnValue = rs.getObject(_iterator + 1);
                            // iterating over outputClass attributes to check if
                            // any attribute has 'Column' annotation with
                            // matching 'name' value
                            for (Field field : fields) {
                                if (field.isAnnotationPresent(Column.class)) {
                                    Column column = field.getAnnotation(Column.class);
                                    if (column.name().equalsIgnoreCase(columnName) && columnValue != null) {
                                        BeanUtils.setProperty(bean, field.getName(), columnValue);
                                        break;
                                    }
                                }
                            }
                        }
                        if (outputList == null) {
                            outputList = new ArrayList<T>();
                        }
                        outputList.add(bean);
                    }
                } else {
                    // throw some error
                }
            } else {
                return null;
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return outputList;
    }


    /**
     * 返回结果数据封装成List对象map集合格式返回
     *
     * @param rs
     * @return
     */
    public List<Map<String, Object>> mapResultSet(ResultSet rs) {
        List<Map<String, Object>> outputList = null;
        try {
            if (rs != null) {
                ResultSetMetaData rsmd = rs.getMetaData();
                Map<String, Object> resultMap;
                while (rs.next()) {
                    resultMap = new HashMap<>();
                    for (int _iterator = 0; _iterator < rsmd.getColumnCount(); _iterator++) {
                        resultMap.put(rsmd.getColumnName(_iterator + 1), rs.getObject(_iterator + 1));
                    }
                    if (outputList == null) {
                        outputList = new ArrayList<Map<String, Object>>();
                    }
                    outputList.add(resultMap);
                }
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return outputList;
    }

    /**
     * 关闭流公共处理
     *
     * @param stmt
     * @param conn
     * @param resultSet
     */
    public void close(Statement stmt, Connection conn, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
