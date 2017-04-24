/**
 * Conditional object comprising a "less than" test
 *
 */
public class LTConditional extends ComparisonConditional implements Conditional {

    /**
     * @param left - the left operand of this less than operator.  
     * @param right - the right operand of this less thann operator
     *
     * Both operands may be a constant ColumnValue or a table attribute
     */   
    public LTConditional(CondLeaf left, CondLeaf right) {
	super(left,right);
    }

    /**
     * @return the true/false value of this LT conditional for the Tuple t.
     *
     */
    public boolean truthVal(Tuple t) {
	return super.truth_val_as_int(t) < 0;
    }

    public String toString() {
	return "LT " + left.toString() + " " + right.toString();
    }
}