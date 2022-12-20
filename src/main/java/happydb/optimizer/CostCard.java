package happydb.optimizer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @Author happysnaker
 * @Date 2022/11/26
 * @Email happysnaker@foxmail.com
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CostCard {
    /** 最优子计划的IO成本*/
    public double cost;
    /** 最优子计划的基数 */
    public int card;
    /** 最优子计划 */
    public List<LogicalJoinNode> plan;
}
