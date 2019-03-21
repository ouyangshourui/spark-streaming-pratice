package com.whz.platform.sparkKudu.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.whz.platform.sparkKudu.model.JdbcModel;


public class JdbcUtils {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcUtils.class);
    private Connection con;

    public JdbcUtils(JdbcModel model) {
        try {
            Class.forName(model.getDriverClassName());
            con = DriverManager.getConnection(model.getUrl(), model.getUsername(), model.getPassword());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("创建Connection失败:" + e.getMessage() + ",model:" + JSONObject.toJSONString(model));
        } catch (SQLException e) {
            LOG.info("数据库连接获取失败！" + e.getMessage() + ",model:" + JSONObject.toJSONString(model));
            throw new RuntimeException("创建Connection失败:" + e.getMessage());
        }

    }

    /**
     * 得到连接查询对象
     *
     * @return
     */
    public PreparedStatement getPreparedStatement(String sql) {
        try {
            return con.prepareStatement(sql);
        } catch (SQLException e) {
            LOG.info("创建PreparedStatement失败" + e.getMessage());
            throw new RuntimeException("创建PreparedStatement失败:" + e.getMessage());
        }
    }

    /**
     * 执行查询 返回以数据库列名为key值为value的List<Map<String, Object>>
     *
     * @param sql
     * @return
     */
    public List<Map<String, Object>> getResultSet(String sql, Object... params) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = getPreparedStatement(sql);
            if (null != params) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rs = pstmt.executeQuery();
            ResultSetMetaData rsm = rs.getMetaData();
            int clms = rsm.getColumnCount();
            List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
            while (rs.next()) {
                Map<String, Object> map = new LinkedHashMap<String, Object>();
                for (int i = 1; i <= clms; i++) {
                    String value = rs.getString(i);
                    String key = rsm.getColumnLabel(i);
                    map.put(key, value);
                }
                list.add(map);
            }
            return list;
        } catch (SQLException e) {
            LOG.info("数据操作执行错误：" + e.getMessage());
            throw new RuntimeException("数据操作执行错误：" + e.getMessage());
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 执行update/insert/delete操作
     *
     * @param sql
     */
    public void excuteSql(String sql, Object... params) {
        PreparedStatement pstmt = null;
        try {
            pstmt = getPreparedStatement(sql);
            if (null != params) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            pstmt.execute();
            
        } catch (SQLException e) {
        	e.printStackTrace();
            LOG.info("数据操作执行错误：" + e.getMessage());
            throw new RuntimeException("数据操作执行错误：" + e.getMessage());
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void excuteSqlBatch(String sql, List<Object[]> zwfList) {
        PreparedStatement pstmt = null;
        try {
            pstmt = getPreparedStatement(sql);
            if (null != zwfList && zwfList.size() != 0) {
                for (int i = 0; i < zwfList.size(); i++) {
                    Object[] params = zwfList.get(i);
                    for (int j = 0; j < params.length; j++) {
                        pstmt.setObject(params.length * i + j + 1, params[j]);
                    }
                }
            }
            pstmt.execute();
        } catch (SQLException e) {
            LOG.info("数据操作执行错误：" + e.getMessage());
            throw new RuntimeException("数据操作执行错误：" + e.getMessage());
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 添加批处理
     *
     * @param sql
     */
    public void addBatch(PreparedStatement stmt, Object... params) {
        try {
            if (null != params) {
                for (int i = 0; i < params.length; i++) {
                    stmt.setObject(i + 1, params[i]);
                }
            }
            stmt.addBatch();
        } catch (SQLException e) {
            LOG.info("将sql添加批处理失败:" + e.getMessage());
            throw new RuntimeException("将sql添加批处理失败:" + e.getMessage());
        }
    }

    public Connection getConnection(){
    	return con;
    }
    
    /**
     * 执行批处理
     */
    public void executeBatch(PreparedStatement stmt) {
        try {
            stmt.executeBatch();
        } catch (SQLException e) {
            LOG.info("执行批处理失败:" + e.getMessage());
            throw new RuntimeException("执行批处理失败:" + e.getMessage());
        }
    }

    /**
     * 执行查询返回 记录总条数
     *
     * @param sql
     * @return
     * @throws SQLException 
     */
    public int getToalCount(String sql, Object... params){
    	PreparedStatement pstmt = null;
    	ResultSet rs = null;
        int count = 0;
        try {
        	pstmt = getPreparedStatement(sql);
            if (null != params) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                count = rs.getInt(1);
            }
        } catch (Exception e) {
            throw new RuntimeException("执行查询错误:[" + sql + "]" + e.getMessage());
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (pstmt != null) {
                	pstmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return count;
    }
    
    public Set<String> getColumnInfoList(String database, String table) {
        Set<String> columns = new HashSet<String>();
        ResultSet rsm = null;
        try {
            rsm = getColumnModelRs(database, table);
            while (rsm.next()) {
                String columnName = rsm.getString("COLUMN_NAME");
                columns.add(columnName.toLowerCase());
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        } finally {
            if (rsm != null) {
            	try {
					rsm.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
            }
        }
        return columns;
    }
    
    public Map<String, String> getColumnTypeList(String database, String table) {
        Map<String, String> map = new HashMap<String, String>();
        ResultSet rsm = null;
        try {
            rsm = getColumnModelRs(database, table);
            while (rsm.next()) {
                String columnName = rsm.getString("COLUMN_NAME");
                int columnType = rsm.getInt("DATA_TYPE");
                String columnTypeName = "varchar";
                switch (columnType) {
				case -6:
					columnTypeName = "tinyint";
					break;
				case 8:
					columnTypeName = "double";
					break;
				case 1:
					columnTypeName = "char";
					break;
				case 7:
					columnTypeName = "float";
					break;
				case 4:
					columnTypeName = "int";
					break;
				case -1:
					columnTypeName = "text";
					break;
				case 91:
					columnTypeName = "date";
					break;
				case 93:
					columnTypeName = "datetime";
					break;
				case 12:
					columnTypeName = "varchar";
					break;
				default:
					break;
				}
                map.put(columnName.toLowerCase(),columnTypeName);
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        } finally {
            if (rsm != null) {
            	try {
					rsm.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
            }
        }
        return map;
    }

    private ResultSet getColumnModelRs(String database, String tableName) {
        try {
            DatabaseMetaData databaseMetaData = con.getMetaData();
            ResultSet columnSet = databaseMetaData.getColumns("`" + database + "`", "%", "`" + tableName + "`", "%");
            return columnSet;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
    
    /**
     * 关闭数据库连接connection
     */
    public void closeCon() {
        LOG.info("数据库connection连接关闭！");
        try {
            if (con != null) {
                con.close();
            }
        } catch (SQLException e) {
            LOG.info(e.getMessage());
        }
    }

    /**
     * 关闭数据库连接Statement
     */
    public void closeStmt(Statement stmt) {
        LOG.info("数据库Statement连接关闭！");
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            LOG.info(e.getMessage());
        }
    }

    /**
     * 判断连接是否关闭
     *
     * @return 连接是否关闭
     */
    public boolean isConnClosed() {
        try {
            return con.isClosed();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }


    /**
     * 判断是否有链接
     */
    public boolean hasConn() {
        return con != null;
    }
}
