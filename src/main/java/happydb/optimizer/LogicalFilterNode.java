package happydb.optimizer;

import happydb.execution.Predicate;
import happydb.storage.*;
import lombok.Getter;

/**
 * @Author happysnaker
 * @Date 2022/11/27
 * @Email happysnaker@foxmail.com
 */
@Getter
public class LogicalFilterNode {
    /**
     * 过滤器范围内的表的别名（如果没有别名，则为名称）
     */
    public final String tableAlias;

    /**
     * 过滤器中的谓词
     */
    public final Predicate.Op op;

    /**
     * 过滤器右侧的常量
     */
    public final String c;

    /**
     * 过滤器中来自 t 的字段。纯名称，没有别名或表名
     */
    public final String fieldPureName;
    /**
     * a.x 字段形式
     */
    public final String fieldQuantifiedName;
    /**
     * 过滤器是否以 and 驱动
     */
    public final boolean isAnd;

    public LogicalFilterNode(String tableAlias, String field, Predicate.Op pred, String constant, boolean isAnd) {
        this.tableAlias = tableAlias;
        this.op = pred;
        this.c = constant;
        this.isAnd = isAnd;
        String[] tmp = field.split("[.]");
        if (tmp.length > 1)
            fieldPureName = tmp[tmp.length - 1];
        else
            fieldPureName = field;
        this.fieldQuantifiedName = tableAlias + "." + fieldPureName;
    }


    public Field parseConstant(TableDesc td) {
        int index = td.fieldNameToIndex(getFieldPureName());
        if (td.getFieldType(index) == Type.INT_TYPE) {
            return new IntField(Integer.parseInt(c));
        } else if (td.getFieldType(index) == Type.DOUBLE_TYPE) {
            return new DoubleField(Double.parseDouble(c));
        } else {
            return new StringField(c);
        }
    }
}
