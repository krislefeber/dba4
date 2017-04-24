/**
 * Conditional object comprising an "equal to" test
 *
 */
public class EQConditional extends ComparisonConditional implements Conditional {

    /**
     * @param left - the left operand of this equal to operator.  
     * @param right - the right operand of this equal to operator
     *
     * Both operands may be a constant ColumnValue or a table attribute
     */   
    public EQConditional(CondLeaf left, CondLeaf right) {
	super(left,right);
    }

    /**
     * @return the true/false value of this EQ conditional for the Tuple t.
     *
     */
    public boolean truthVal(Tuple t) {
	return super.truth_val_as_int(t) == 0;
    }

    public String toString() {
	return "EQ " + left.toString() + " " + right.toString();
    }
}