/**
 * Conditional object comprising a "greater than" test
 *
 */
public class GTConditional extends ComparisonConditional implements Conditional {

    /**
     * @param left - the left operand of this greater than operator.  
     * @param right - the right operand of this greater than operator
     *
     * Both operands may be a constant ColumnValue or a table attribute
     */   
    public GTConditional(CondLeaf left, CondLeaf right) {
	super(left,right);
    }

    /**
     * @return the true/false value of this GT conditional for the Tuple t.
     *
     */
    public boolean truthVal(Tuple t) {
	return super.truth_val_as_int(t) > 0;
    }

    public String toString() {
	return "GT " + left.toString() + " " + right.toString();
    }
}