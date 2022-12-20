package happydb.index;

import happydb.common.Catalog;
import happydb.storage.AbstractPage;
import happydb.storage.PageId;
import happydb.storage.Type;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * 叶子节点和内部节点继承的类，封装了一些通用的方法
 *
 * @Author happysnaker
 * @Date 2022/11/21
 * @Email happysnaker@foxmail.com
 */
public abstract class BTreePage extends AbstractPage {
    public static final byte INTERNAL = 0;
    public static final byte LEAF = 1;

    /**
     * 父节点，如果此类是根节点则为空
     */
    @Getter @Setter protected PageId parent;
    /**
     * 索引字段的类型
     */
    @Getter @Setter protected Type type;
    /**
     * 此页面的类型
     */
    @Getter @Setter protected byte category;


    public BTreePage(byte category, PageId parent, PageId pid) {
        super.pid = pid;
        this.parent = parent;
        this.category = category;
        this.type = Catalog.getFieldTypeFromIndexTableName(pid.getTableName());
    }
}
