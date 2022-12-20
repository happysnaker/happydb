package happydb.optimizer;

import happydb.common.Database;
import happydb.exception.DbException;
import happydb.storage.BufferPool;
import happydb.storage.Record;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 操作 {@link TableState} 的唯一入口
 *
 * @Author happysnaker
 * @Date 2022/11/26
 * @Email happysnaker@foxmail.com
 */
public class TableStateView {
    /**
     * 全局单例实例
     */
    private final static TableStateView instance = new TableStateView();
    /**
     * 修改阈值，达到阈值后需要重建 TableState
     */
    public static int MODIFY_THRESHOLD = 1000;
    /**
     * 表中记录小于此参数时，每一次获取都应该重建
     */
    public static int MIN_THRESHOLD = 5;

    private final int ioCostFactorPerPage;

    private final Map<String, TableState> tableStateMap = new ConcurrentHashMap<>();
    private final Map<String, Integer> modifyCountMap = new ConcurrentHashMap<>();

    private TableStateView() {
        this.ioCostFactorPerPage = (BufferPool.getPageSize() / BufferPool.DEFAULT_PAGE_SIZE) * 500;
    }

    /**
     * 获取此单例实例
     */
    public static TableStateView getInstance() {
        return instance;
    }

    /**
     * 根据表名，获取 {@link TableState}，由此函数自行决定是否需要重建 {@link TableState}
     *
     * @param tableName 表名
     * @return 表状态
     */
    public TableState getTableState(String tableName) throws DbException {
        TableState state = tableStateMap.get(tableName);
        if (state == null) {
            synchronized (this) {
                if ((state = tableStateMap.get(tableName)) == null) {
                    state = new TableState(tableName, ioCostFactorPerPage);
                    tableStateMap.put(tableName, state);
                }
            }
        } else {
            if (state.getNumRecords() <= MIN_THRESHOLD) {
                synchronized (this) {
                    if (state == tableStateMap.get(tableName)) {
                        tableStateMap.remove(tableName);
                    }
                    return getTableState(tableName);
                }
            }
        }
        return state;
    }

    /**
     * 返回一些表对应的 TableState
     *
     * @param tableNames 表名集合
     * @return stateMap, 使用者应该仅仅只是查阅 TableState，而不应该修改 TableState
     * @throws DbException 如果创建 TableState 失败
     */
    public Map<String, TableState> getTableStateMap(Set<String> tableNames) throws DbException {
        Map<String, TableState> map = new HashMap<>();
        for (String tableName : tableNames) {
            map.put(tableName, getTableState(tableName));
        }
        return map;
    }

    /**
     * 一个帮助方法，维护 TableState 视图，并在修改次数达到阈值后重建 TableState
     *
     * @param tableName 表
     * @param record    记录
     * @param insert    是插入还是删除
     * @throws DbException
     */
    private void updateTableState(String tableName, Record record, boolean insert) throws DbException {
        TableState state = getTableState(tableName);

        modifyCountMap.put(tableName, modifyCountMap.getOrDefault(tableName, 0) + 1);
        if (modifyCountMap.get(tableName) == MODIFY_THRESHOLD) {
            synchronized (this) {
                if (modifyCountMap.get(tableName) == MODIFY_THRESHOLD) {
                    tableStateMap.remove(tableName);
                    state = getTableState(tableName);
                    assert state != null;
                    modifyCountMap.put(tableName, 0);
                }
            }
        }

        if (insert) {
            state.insertRecord(record);
        } else {
            state.deleteRecord(record);
        }
    }


    /**
     * 插入一个元组，修改状态视图
     *
     * @param tableName 真实的表名
     * @param record    元组记录
     * @throws DbException
     */
    public void insertRecord(@NonNull String tableName, @NonNull Record record) throws DbException {
        if (!record.getTableDesc().getTableName().equals(tableName)) {
            throw new DbException("记录不属于此表 " + tableName);
        }
        updateTableState(tableName, record, true);
    }


    /**
     * 删除一个元组，修改状态视图
     *
     * @param tableName 真实的表名
     * @param record    元组记录
     * @throws DbException
     */
    public void deleteRecord(@NonNull String tableName, @NonNull Record record) throws DbException {
        if (!record.getTableDesc().getTableName().equals(tableName)) {
            throw new DbException("记录不属于此表 " + tableName);
        }
        updateTableState(tableName, record, false);
    }


    /**
     * 更新一个元组，修改状态视图
     *
     * @param tableName 真实的表名
     * @param oldRecord 旧记录
     * @param newRecord 新记录
     * @throws DbException
     */
    public void updateRecord(@NonNull String tableName, @NonNull Record oldRecord, @NonNull Record newRecord) throws DbException {
        deleteRecord(tableName, oldRecord);
        insertRecord(tableName, newRecord);
    }
}
