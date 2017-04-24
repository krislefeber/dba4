/**
 * The abstract base class that LT (less than), EQ (equal), and GT (greater than) operators must implement
 *
 */
public abstract class ComparisonConditional {

    CondLeaf left;
    CondLeaf right;

    /**
     * @param left - the left operand of this comparison operator.  
     * @param right - the right operand of this comparison operator
     *
     * Both operands may be a constant ColumnValue or a table attribute
     */   
    public ComparisonConditional(CondLeaf left, CondLeaf right) {
	this.left = left;
	this.right = right;
    }

    /**
     * Internal method used to compare values based on whether they
     * are constants or table attributes for the Tuple t
     *
     * @return -1 if the left operand precedes the right, 0 if they
     * are equal, 1 if the left operand follows the right
     */
    int truth_val_as_int(Tuple t) {
	ColumnValue left_val;
	ColumnValue right_val;
	if (left.isConstant()) {
	    left_val = left.evaluate();
	}
	else {
	    left_val = left.evaluate(t);
	}
	if (right.isConstant()) {
	    right_val = right.evaluate();
	}
	else {
	    right_val = right.evaluate(t);
	}
	return left_val.compareTo(right_val);
    }

    /**
     * Used to set the table that the left leaf of the conditional is
     * associated with.  This may need to be done when, during the
     * evaluation of a Select or Join, the conditional is being
     * evaluated on a table other than the one for which it was
     * originally constructed.
     * @param t - the table to associate with the conditional leaf
     *
     */
    public void set_left_leaf_table (Table t) {
	left.my_table = t;
    }

    /**
     * Used to set the table that the right leaf of the conditional is
     * associated with.  This may need to be done when, during the
     * evaluation of a Select or Join, the conditional is being
     * evaluated on a table other than the one for which it was
     * originally constructed.
     * @param t - the table to associate with the conditional leaf
     *
     */
    public void set_right_leaf_table (Table t) {
	right.my_table = t;
    }

    /**
     * Used to set the table that both leave of the conditional is
     * associated with.  This may need to be done when, during the
     * evaluation of a Select or Join, the conditional is being
     * evaluated on a table other than the one for which it was
     * originally constructed.
     * @param t - the table to associate with the conditional leaf
     *
     */
    public void set_both_leaves_table (Table t) {
	left.my_table = t;
	right.my_table = t;
    }

    /**
     * @return the true/false value of this comparison conditional for
     * the Tuple t.
     *
     */
    public abstract boolean truthVal(Tuple t);
}