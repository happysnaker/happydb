package happydb.optimizer;

import lombok.Getter;

/**
 * @Author happysnaker
 * @Date 2022/11/27
 * @Email happysnaker@foxmail.com
 */
@Getter
public class LogicalSelectListNode {

    /**
     * 被选中的字段名，a.x 形式，如果要输出的是 *, 则此字段为 null.*
     */
    public final String fieldQuantifiedName;

    /** 字段上的聚合操作，没有则 null */
    public final String aggOp;

    /**
     * 用户指定的打印的名字
     */
    public final String as;

    public LogicalSelectListNode(String fName, String aggOp, String as) {
        this.aggOp = aggOp;
        this.fieldQuantifiedName = fName;
        this.as = as;
    }

    /**
     * 返回 select 字段对应的表
     * @return 如果是选择全部（*），则返回 null
     */
    public String getFieldTable() {
        String s = fieldQuantifiedName.split("[.]")[0];
        return s.equals("null") ? null : s;
    }

    /**
     * 返回 select 字段纯净名称
     */
    public String getFieldPureName() {
        return fieldQuantifiedName.split("[.]")[1];
    }
}
