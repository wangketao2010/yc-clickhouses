package com.yc.clickhouse.dao;

import com.yc.clickhouse.config.annotation.ClickHousePrimaryKey;
import com.yc.clickhouse.config.annotation.ClickHouseTable;
import com.yc.clickhouse.constant.Constant;
import com.yc.clickhouse.entity._BaseEntity;
import com.yc.clickhouse.utils.ClickHouseBaseUtil;
import com.yc.clickhouse.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.springframework.util.CollectionUtils;

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
public class ClickHouseDaoBase<T extends _BaseEntity> {
    private Lock lock = new ReentrantLock();// 可重入锁
    private Class<T> tClass;// 当前实体类对应泛型
    private Field[] allFieldList;// 当前实体类属性数组
    private Connection conn;//数据库连接

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
    private Connection getConnection() throws SQLException {
        try {
            lock.lock();
            if( conn == null || conn.isClosed() ) {
                conn = clickHouseDatasource.getConnection();
//                conn.setAutoCommit(false);//关闭自动提交，有异常直接回滚
            }
        } catch (SQLException e) {
            log.error("连接clickHouse服务异常：{}", e.getMessage());
            throw new SQLException("获取连接出错",e);
        }finally {
            lock.unlock();
        }
        return conn;
    }
    //################################# merginng #################################

    /**
     * 获取当前实体类对应的表名
     *
     * @return
     */
    private String getTableName() {
        ClickHouseTable table = this.getTClass().getAnnotation(ClickHouseTable.class);
        if (table == null) {
            throw new RuntimeException(" no Annotation 'javax.persistence.Table' in clazz  ");
        }
        return table.name();
    }

    /**
     * 获取T的class实例
     *
     * @return
     */
    private Class<T> getTClass() {
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

    /**
     * 获取当前T所有的属性数组
     *
     * @return
     */
    private Field[] getAllFieldList() {
        if (allFieldList == null) {
            lock.lock();
            try {
                if (allFieldList == null) {
                    //one
//                    List<Field> fieldList = new ArrayList<>();
//                    Class clazz = this.getTClass();
//                    while (clazz != null) {   //当父类为null的时候说明到达了最上层的父类(Object类).
//                        fieldList.addAll(Arrays.asList(clazz.getDeclaredFields()));
//                        clazz = clazz.getSuperclass(); //得到父类,然后赋给自己
//                    }
//                    allFieldList = fieldList.toArray(new Field[fieldList.size()]);
                    //two
                    allFieldList = this.getTClass().getDeclaredFields();
                }
            } finally {
                lock.unlock();
            }
        }
        return allFieldList;
    }

    /**
     * 不同于普通SQL的修改和删除，alter table [tableName] update……where……
     * 根据构造的对象，以ID为条件来修改
     *
     * @param entity：只针对注解声明的属性
     * @return
     */
    public int updateByPrimaryKey(T entity) throws Exception {
        if (entity == null) {
            throw new RuntimeException(" entity is null ");
        }
        // 构建修改语句sql
        StringBuffer sql = new StringBuffer("alter table ").append(this.getTableName()).append(" update ");
        String where = "";

        // 获取类的所有字段，并逐一填充sql，只针对注解声明的属性,
        Field[] fieldList = this.getAllFieldList();
        List<Object> setValue = new ArrayList<>();
        for (Field field : fieldList) {
            Column column = field.getAnnotation(Column.class);
            if (column != null) {
                Object getValue = entity.getClass().getMethod("get" + ClickHouseBaseUtil.capitalize(field.getName())).invoke(entity);
                // ID条件判断
                ClickHousePrimaryKey id = field.getAnnotation(ClickHousePrimaryKey.class);
                if (id != null) {
                    where = " where " + column.name() + " = " + getValue;
                } else {
                    sql.append(column.name()).append(" = ?,");
                    setValue.add(getValue);
                }
            }
        }

        // 完成SQL的构建
        sql.deleteCharAt(sql.length() - 1).append(where);
        log.info("updateByPrimaryKey " + sql.toString());

        // 开始修改
        try {
            conn = getConnection();
            PreparedStatement preparedStatement = conn.prepareStatement(sql.toString());
            putPrepareStatementParams(preparedStatement,setValue.toArray());
            int res = preparedStatement.executeUpdate();//提交修改
//            conn.commit();// 执行
            log.info("修改结果：[{}]", res);
            return res;
        } catch (Exception e) {
//            try {
//                conn.rollback();//异常回滚
//            } catch (Exception e1) {
//                log.error("发生了异常",e);
//            }
            log.error("发生了异常",e);
        }
        return 0;
    }

    /**
     * 指定的SQL语句修改，一般是范围修改
     *
     * @param fullSql
     * @return
     */
    public int updateBySql(String fullSql) {
        log.info("updateBySql " + fullSql);
        return this.executeInsertUpdateDelete(fullSql);
    }

    /**
     * 不同于普通SQL的修改和删除，alter table [tableName] delete where……
     * 根据ID删除，要获取被ID注解声明的字段名称
     *
     * @param primaryKey
     * @return
     */
    public int deleteByPrimaryKey(Object primaryKey) throws SQLException {
        String columnID = "";//字段ID
        Field[] fieldList = this.getAllFieldList();
        for (Field field : fieldList) {
            ClickHousePrimaryKey annotationID = field.getAnnotation(ClickHousePrimaryKey.class);
            if (annotationID != null) {
                Column annotationColumn = field.getAnnotation(Column.class);
                columnID = annotationColumn.name();
            }
        }
        String sql = "alter table " + this.getTableName() + " delete where " + columnID + " = ?";

        log.info("deleteByPrimaryKey " + sql);

        try {
            conn = getConnection();
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setObject(1, primaryKey);
            int res = preparedStatement.executeUpdate();//提交删除
//            conn.commit();// 执行
            return res;
        } catch (Exception e) {
//            try {
//                conn.rollback();//异常回滚
//            } catch (Exception e1) {
//                log.error("发生了异常",e);
//            }
            log.error("发生了异常",e);
        }
        return 0;
    }

    /**
     * 根据SQL语句来删除
     *
     * @param fullSql
     * @return
     */
    public int deleteBySql(String fullSql){
        log.info("deleteBySql " + fullSql);
        return executeInsertUpdateDelete(fullSql);
    }

    /**
     * 简单count查询；单表查询
     *
     * @param sqlWhere
     * @param params sql语句参数，没有传 null
     * @return
     */
    public int selectCount(String sqlWhere, Object[] params) {
        StringBuffer stringBuffer = new StringBuffer("select count(*) as count from ").append(this.getTableName());
        if (sqlWhere != null) {
            stringBuffer.append(sqlWhere);
        }
        log.info(stringBuffer.toString());
        try {
            conn = getConnection();
            PreparedStatement preparedStatement = conn.prepareStatement(stringBuffer.toString());
            putPrepareStatementParams(preparedStatement,params);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("count");
            }
        } catch (Exception e) {
            log.error("发生了异常",e);
        }finally {
//            close(preparedStatement,conn,null);
        }
        return 0;
    }

    /**
     * 复杂count查询；多表查询
     * @param tableSql
     * @return
     */
    public int selectCount(String tableSql) {
        tableSql = "select count(t.*) as count from (" + tableSql + ") t";//手动构造统计
        log.info("selectCount " + tableSql);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            conn = getConnection();
            statement = conn.createStatement();

            resultSet = statement.executeQuery(tableSql);
            if (resultSet != null) {
                try {
                    if (resultSet.next()) {
                        return resultSet.getInt("count");
                    }
                } catch (Exception e) {
                    log.error("发生了异常",e);
                }
            }
            return 0;
        } catch (SQLException e) {
            log.error("发生了异常",e);
        } finally {//关闭连接
            //close(statement, conn, resultSet);
        }
        return -1;
    }

    /**
     * 分页查询，构造条件，排序
     *
     * @param start
     * @param size
     * @param sqlWhere：eg：field asc/desc
     * @return
     */
    public List<T> selectPage(int start, int size, String sqlWhere, Object[] params) {//} T entity, String orderByFieldAndIsAsc) {
        StringBuffer sql = new StringBuffer("select * from ").append(this.getTableName());
        if (sqlWhere == null) {
            sql.append(" limit ").append(start).append(",").append(size);
        } else {
            sql.append(sqlWhere).append(" limit ").append(start).append(",").append(size);
        }

        log.info("selectPage " + sql.toString());

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            conn = getConnection();
            preparedStatement = conn.prepareStatement(sql.toString());
            putPrepareStatementParams(preparedStatement, params);
            resultSet = preparedStatement.executeQuery();
            return mapResultSetToObject(resultSet);
        } catch (Exception e) {
            log.error("发生了异常",e);
        } finally {
            //close(preparedStatement, conn, resultSet);
        }
        return new ArrayList<T>(0);//直接返回空list，防止NullPointException
    }
    //################################# merginng #################################

    /**
     * 添加传入参数
     *
     * @param preparedStatement
     * @param params sql语句参数，没有传 null
     * @throws SQLException
     */
    private void putPrepareStatementParams(PreparedStatement preparedStatement, Object[] params) throws SQLException {
        if (params != null && params.length > 0) {
            for (int i = 0; i < params.length; i++) {
                if (params[i] instanceof Integer) {
                    preparedStatement.setInt(i + 1, (Integer) params[i]);
                } else if (params[i] instanceof Long) {
                    preparedStatement.setLong(i + 1, (Long) params[i]);
                } else if (params[i] instanceof String) {
                    preparedStatement.setString(i + 1, params[i].toString());
                } else if (params[i] instanceof Double) {
                    preparedStatement.setDouble(i + 1, (Double) params[i]);
                } else if (params[i] instanceof BigDecimal) {
                    preparedStatement.setBigDecimal(i + 1, (BigDecimal) params[i]);
                } else if (params[i] instanceof Float) {
                    preparedStatement.setFloat(i + 1, (Float) params[i]);
                } else {
                    preparedStatement.setObject(i + 1, params[i]);
                }
            }
        }
    }

    /**
     * 返回结果数据封装成List对象map集合格式返回
     *
     * @param rs
     * @return
     */
    private List<Map<String, Object>> mapResultSetToMap(ResultSet rs) {
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
                        outputList = new ArrayList<>();
                    }
                    outputList.add(resultMap);
                }
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error("发生了异常",e);
        }
        return outputList;
    }

    /**
     * 将查询的返回结果信息封装实体对象
     *
     * @param rs
     * @return
     */
    @SuppressWarnings("unchecked")
    private List<T> mapResultSetToObject(ResultSet rs) {
        List<T> outputList = null;
        try {
            // make sure resultSetet is not null
            if (rs != null) {
                // check if outputClass has 'Entity' annotation
                Class outputClass = getTClass();
                if (outputClass.isAnnotationPresent(Entity.class)) {
                    // get the resultSetet metadata
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
            log.error("发生了异常",e);
        } catch (SQLException e) {
            log.error("发生了SQLException异常",e);
        } catch (InstantiationException e) {
            log.error("发生了 InstantiationException 异常",e);
        } catch (InvocationTargetException e) {
            log.error("发生了 InvocationTargetException 异常",e);
        }
        return outputList;
    }

    /**
     * 关闭流公共处理
     *
     * @param statement
     * @param conn
     * @param resultSet
     */
    private void close(Statement statement, Connection conn, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            log.error("发生了异常",e);
        }
    }

//    /**
//     * 根据传入SQL条件，查询对象集合数据
//     *
//     * @param sql
//     * @return
//     */
//    public List<T> selectList(String sql) {
//        log.info("clickHouse 查询集合数据执行sql：" + sql);
//        Statement statement = null;
//        ResultSet resultSet = null;
//        try {
//            conn = getConnection();
//            statement = conn.createStatement();
//            resultSet = statement.executeQuery(sql.toString());
//            List<T> list = mapResultSetToObject(resultSet);
//            if (list != null) {
//                log.debug("查询出数据size：{}", list.size());
//            } else {
//                log.debug("ResultSet is empty. Please check if database table is empty");
//            }
//            return list;
//        } catch (SQLException e) {
//            log.error("发生了异常",e);
//        } finally {//关闭连接
//            close(statement, conn, resultSet);
//        }
//        return null;
//    }

    /**
     * 根据传入SQL条件以及参数，查询集合数据
     *
     * @param sql
     * @param params sql语句参数，没有传 null
     * @return
     */
    public List<T> selectListObj(String sql, Object[] params) {
        return selectListCommon("obj",sql,params);
    }

    /**
     * 根据传入SQL条件以及参数，返回List对象Map集合数据
     *
     * @param sql
     * @param params sql语句参数，没有传 null
     * @return
     */
    public List<Map<String, Object>> selectListMap(String sql, Object[] params) {
        return selectListCommon("map",sql,params);
    }

    private List selectListCommon(String type, String sql, Object[] params) {
        log.info("clickHouse 查询集合数据执行sql：" + sql);
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            conn = getConnection();
            preparedStatement = conn.prepareStatement(sql);
            putPrepareStatementParams(preparedStatement, params);
            resultSet = preparedStatement.executeQuery();
            List backList = null;
            if ("map".equals(type)) {
                backList = mapResultSetToMap(resultSet);
            } else if ("obj".equals(type)) {
                backList = mapResultSetToObject(resultSet);
            }
            if (backList != null) {
                log.debug("查询出数据size：{}", backList.size());
            } else {
                log.debug("ResultSet is empty. Please check if database table is empty");
            }
            return backList;
        } catch (SQLException e) {
            log.error("发生了异常", e);
        } finally {//关闭连接
            //close(preparedStatement, conn, resultSet);
        }
        return null;
    }
//    /**
//     * 根据传入SQL条件，返回一条对象数据
//     *
//     * @param sql
//     * @return
//     */
//    public T selectOne(String sql) {
//        log.info("clickHouse 查询单条数据执行sql：" + sql);
//        Statement statement = null;
//        ResultSet resultSet = null;
//        try {
//            conn = getConnection();
//            statement = conn.createStatement();
//            resultSet = statement.executeQuery(sql);
//            Class<T> clazz = getTClass();
//            if (Number.class.isAssignableFrom(clazz)
//                    || Date.class.isAssignableFrom(clazz)
//                    || String.class.isAssignableFrom(clazz)) {
//                if (resultSet.next()) {
//                    return (T) resultSet.getObject(1);
//                }
//            } else {
//                List<T> list = mapResultSetToObject(resultSet);
//                return CollectionUtils.isEmpty(list) ? null : list.get(0);
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {//关闭连接
//            close(statement, conn, resultSet);
//        }
//        return null;
//    }

    /**
     * 根据传入SQL条件以及参数，返回一条对象数据
     *
     * @param selectSql
     * @param params sql语句参数，没有传 null
     * @return
     */
    public T selectOne(String selectSql, Object[] params) {
        log.info("clickHouse 查询单条数据执行sql：" + selectSql);
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            conn = getConnection();
            preparedStatement = conn.prepareStatement(selectSql);
            putPrepareStatementParams(preparedStatement, params);
            resultSet = preparedStatement.executeQuery();
            Class<T> clazz = getTClass();
            if (Number.class.isAssignableFrom(clazz)
                    || Date.class.isAssignableFrom(clazz)
                    || String.class.isAssignableFrom(clazz)) {
                if (resultSet.next()) {
                    return (T) resultSet.getObject(1);
                }
            } else {
                List<T> list = mapResultSetToObject(resultSet);
                return CollectionUtils.isEmpty(list) ? null : list.get(0);
            }
        } catch (SQLException e) {
            log.error("发生了异常",e);
        } finally {//关闭连接
            //close(preparedStatement, conn, resultSet);
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
        if (CollectionUtils.isEmpty(list) || StringUtil.isEmpty(tableName)){
            return;
        }
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Field[] fields = null;
        int fieldSize = 0;
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
                    fields =  this.getAllFieldList();
                    fieldSize = fields.length;
                    for (int i = 0; i < fieldSize; i++) {
                        //如果没有注解或者注解的列是空
                        if (!fields[i].isAnnotationPresent(Column.class) || fields[i].getAnnotation(Column.class) == null) {
                            continue;
                        }
                        sql.append("?,");
                    }
                    sql.deleteCharAt(sql.length() - 1);
                    sql.append(")");
                    preparedStatement = conn.prepareStatement(sql.toString());
                    log.info("批量插入" + tableName + "打印执行sql: " + sql);
                }

                for (int j = 0; j < fieldSize; j++) {
                    fields[j].setAccessible(true);
                    //如果没有注解或者注解的列是空
                    if (!fields[j].isAnnotationPresent(Column.class) || fields[j].getAnnotation(Column.class) == null) {
                        continue;
                    }
                    // 获取当前需要插入的列名
                    String columnName = fields[j].getAnnotation(Column.class).name();
                    // 将value set到对应的列位
                    if (StringUtil.isNotNull(fields[j].get(obj))) {
                        if (resultSet.getMetaData().getColumnType(indexMap.get(columnName)) == Types.TIMESTAMP) {
                            if (fields[j].get(obj) instanceof Timestamp) {
                                preparedStatement.setTimestamp(indexMap.get(columnName), (Timestamp) fields[j].get(obj));
                            } else if (fields[j].get(obj) instanceof Date) {
                                preparedStatement.setTimestamp(indexMap.get(columnName), new java.sql.Timestamp(((Date) fields[j].get(obj)).getTime()));
                            } else {
                                preparedStatement.setTimestamp(indexMap.get(columnName), null);
                            }
                        } else if (resultSet.getMetaData().getColumnType(indexMap.get(columnName)) == Types.DATE) {
                            if (fields[j].get(obj) instanceof java.sql.Date) {
                                preparedStatement.setDate(indexMap.get(columnName), (java.sql.Date) fields[j].get(obj));
                            } else if (fields[j].get(obj) instanceof Date) {
                                preparedStatement.setDate(indexMap.get(columnName), new java.sql.Date(((Date) fields[j].get(obj)).getTime()));
                            } else if (fields[j].get(obj) instanceof String) {
                                preparedStatement.setString(indexMap.get(columnName), fields[j].get(obj).toString());
                            } else {
                                preparedStatement.setDate(indexMap.get(columnName), null);
                            }
                        } else if (resultSet.getMetaData().getColumnType(indexMap.get(columnName)) == Types.DECIMAL) {
                            if (fields[j].get(obj) instanceof BigDecimal) {
                                preparedStatement.setBigDecimal(indexMap.get(columnName), (BigDecimal) fields[j].get(obj));
                            } else if (fields[j].get(obj) instanceof String) {
                                preparedStatement.setBigDecimal(indexMap.get(columnName), new BigDecimal(fields[j].get(obj).toString()));
                            } else if (fields[j].get(obj) instanceof Number) {
                                preparedStatement.setBigDecimal(indexMap.get(columnName), BigDecimal.valueOf((Double) fields[j].get(obj)));
                            } else {
                                preparedStatement.setBigDecimal(indexMap.get(columnName), null);
                            }
                        } else {
                            preparedStatement.setObject(indexMap.get(columnName), fields[j].get(obj));
                        }
                    } else {
                        preparedStatement.setObject(indexMap.get(columnName), null);
                    }
                }
                preparedStatement.addBatch();
                // 每2000插入一次
                if (batch % 2000 == 0) {
                    preparedStatement.executeBatch();
                }
            }
            if(batch % 2000 != 0){
                // 插入剩余数量不足2000的
                preparedStatement.executeBatch();
            }
            indexMap = null;
            Long endTime = System.currentTimeMillis();
            log.info("集合size：{}，批量插入{}成功,耗时{}ms......", list.size(), tableName, (endTime - startTime));
        } catch (Exception e1) {
            log.error("集合size：{}，批量插入{}异常：{}", list.size(), tableName, e1.getMessage());
        } finally {
            //close(preparedStatement, conn, resultSet);
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
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String[] fields = null;
        int fieldSize = 0;
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
                    preparedStatement = conn.prepareStatement(sql.toString());
                    log.info("批量插入" + tableName + "打印执行sql: " + sql);
                }
                for (int j = 0; j < fieldSize; j++) {
                    // 获取当前需要插入的列名
                    value = map.get(fields[j]);
                    // 将value set到对应的列位
                    if (value instanceof Integer) {
                        preparedStatement.setInt(j + 1, (Integer) value);
                    } else if (value instanceof Long) {
                        preparedStatement.setLong(j + 1, (Long) value);
                    } else if (value instanceof String) {
                        preparedStatement.setString(j + 1, value.toString());
                    } else if (value instanceof Double) {
                        preparedStatement.setDouble(j + 1, (Double) value);
                    } else if (value instanceof BigDecimal) {
                        preparedStatement.setBigDecimal(j + 1, (BigDecimal) value);
                    } else if (value instanceof Float) {
                        preparedStatement.setFloat(j + 1, (Float) value);
                    } else {
                        preparedStatement.setObject(j + 1, value);
                    }
                }
                preparedStatement.addBatch();
                // 每2000插入一次
                if (batch % 2000 == 0) {
                    preparedStatement.executeBatch();
                }
            }
            if(batch % 2000 != 0){
                // 插入剩余数量不足2000的
                preparedStatement.executeBatch();
            }
            indexMap = null;
            Long endTime = System.currentTimeMillis();
            log.info("集合size：{}，批量插入{}成功,耗时{}ms......", list.size(), tableName, (endTime - startTime));
        } catch (Exception e1) {
            log.error("集合size：{}，批量插入{}异常：{}", list.size(), tableName, e1);
        } finally {
            //close(preparedStatement, conn, resultSet);
        }
    }

    /**
     * 根据传入SQL做执行操作（如：INSERT，UPDATE，DELETE）
     *
     * @param sql
     */
    public Integer executeInsertUpdateDelete(String sql) {
        log.info("clickhouse 输出执行sql：" + sql);
        try {
            conn = getConnection();
            Statement statement = conn.createStatement();
            int count = statement.executeUpdate(sql);
//            conn.commit();// 执行
            if (count > 0) {
                log.info("执行成功！");
            } else {
                log.info("执行失败！");
            }
            return count;
        } catch (Exception e) {
//            try {
//                conn.rollback();//异常回滚
//            } catch (Exception e1) {
//                log.error("发生了异常",e);
//            }
            log.error("执行 sql 发生了异常",e);
            return -1;
        } finally {//关闭连接
//            close(statement, conn, null);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        close(null, conn, null);
        super.finalize();
    }
}
