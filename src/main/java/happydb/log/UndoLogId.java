package happydb.log;

import happydb.storage.PageId;
import happydb.storage.Record;
import lombok.Getter;
import lombok.Setter;

/**
 * @Author happysnaker
 * @Date 2022/11/29
 * @Email happysnaker@foxmail.com
 */

public record UndoLogId(@Getter@Setter PageId pid, @Getter@Setter int undoLogNumber) {

    public static final String UNDO_LOG_TABLE_NAME_SUFFIX = "-undo";
    /**
     * 表示一个空的 ID，由 {@link Record#getLogPointer()} 保存
     */
    @Deprecated
    public static final UndoLogId NULL_ID = new UndoLogId(new PageId("", -1), 0);

    public String getReallyTableName() {
        return this.pid.getTableName().replace(UNDO_LOG_TABLE_NAME_SUFFIX, "");
    }
}
