package happydb.optimizer;

import happydb.execution.Predicate;
import lombok.Getter;

/**
 * @Author happysnaker
 * @Date 2022/11/26
 * @Email happysnaker@foxmail.com
 */
@Getter
public class LogicalJoinNode {

    /**
     * 要连接的第一个表（可能为空）。它是表的别名（如果没有别名，则为真实的表名）
     */
    private final String t1Alias;

    /**
     * 要连接的第二个表（可能为空）。它是表的别名，（如果没有别名，则为真实的表名）.
     */
    private final String t2Alias;

    /**
     * t1 中要加入的字段的名称。它是字段的纯名称，而不是 alias.field。
     */
    private final String f1PureName;

    /**
     * t1 中 a.f 形式的名称
     */
    private final String f1QuantifiedName;

    /**
     * t2 中要加入的字段的名称。它是字段的纯名称。
     */
    private final String f2PureName;

    /**
     * t2 中 a.f 形式的名称
     */
    private final String f2QuantifiedName;

    /**
     * 连接谓词
     */
    private final Predicate.Op op;


    /**
     * 创建一个连接节点
     * @param table1 左表别名
     * @param table2 右表别名
     * @param joinField1 a.x 形式字段或 x 形式字段
     * @param joinField2 a.x 形式字段或 x 形式字段
     * @param pred 操作符
     */
    public LogicalJoinNode(String table1, String table2, String joinField1, String joinField2, Predicate.Op pred) {
        t1Alias = table1;
        t2Alias = table2;
        String[] tmps = joinField1.split("[.]");
        if (tmps.length > 1)
            f1PureName = tmps[tmps.length - 1];
        else
            f1PureName = joinField1;
        tmps = joinField2.split("[.]");
        if (tmps.length > 1)
            f2PureName = tmps[tmps.length - 1];
        else
            f2PureName = joinField2;
        op = pred;
        this.f1QuantifiedName = t1Alias + "." + this.f1PureName;
        this.f2QuantifiedName = t2Alias + "." + this.f2PureName;
    }

    /**
     * R返回一个交换了内表和外表（t1.f1 和 t2.f2）的新逻辑连接节点。
     */
    public LogicalJoinNode swapInnerOuter() {
        Predicate.Op newp = op.getReverseOp();
        return new LogicalJoinNode(t2Alias, t1Alias, f2PureName, f1PureName, newp);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LogicalJoinNode)) return false;
        LogicalJoinNode j2 = (LogicalJoinNode) o;
        return (j2.t1Alias.equals(t1Alias) || j2.t1Alias.equals(t2Alias)) && (j2.t2Alias.equals(t1Alias) || j2.t2Alias.equals(t2Alias));
    }

    @Override
    public String toString() {
        return t1Alias + "[" + op + "]" + t2Alias;
    }

    @Override
    public int hashCode() {
        return t1Alias.hashCode() + t2Alias.hashCode() + f1PureName.hashCode() + f2PureName.hashCode();
    }
}

