/**
 * CondLeaf.java -- the class comprising objects that are the operands for ComparisonConditionals
 *
 */
public class CondLeaf {

    boolean is_constant;
    String attrib_name;
    ColumnValue col_val;
    Table my_table;

    /**
     * The constructor to use for an operand that is a table attribute
     * @param t - the table of the operand
     * @param attrib_name - the attribute of the operand
     *
     */
    public CondLeaf (Table t, String attrib_name) {
	my_table = t;
	this.attrib_name = attrib_name;
	is_constant = false;
    }
	
    /**
     * The constructor to use for an operand that is a constant ColumnValue
     * @param col_val - the constant value of the operand.  Must be a FloatValue, IntValue, or StringValue
     *
     */
    public CondLeaf (ColumnValue col_val) {
	this.col_val = col_val;
	is_constant = true;
    }

    /**
     * @return true if this operand is a constant and false if it is a table attribute name
     *
     */
    public boolean isConstant() {
	return is_constant;
    }

    /**
     * Used to set the table that a leaf is associated with.  This may
     * need to be done when, during the evaluation of a Select or
     * Join, the conditional is being evaluated on a table other than
     * the one for which it was originally constructed.
     * @param t - the table to associate with the conditional leaf
     *
     */

    public void set_table (Table t) {
	my_table = t;
    }

    /**
     * Only to be called for operands that are not constants
     * @return the table associated with the operand
     *
     */
    public Table which_table () {
	return my_table;
    }
	
    /**
     * Only to be called for operands that are not constants
     * @return the attribute associated with the operand
     *
     */
    public String which_attrib () {
	return attrib_name;
    }
	
    public ColumnValue evaluate() {
	if (is_constant)
	    return col_val;
	else
	    return null;
    }

    /**
     * Only to be called for operands that are not constants
     * @param t - the current tuple of the table
     * @return the value associated with this tuple for the attribute
     * of this operand
     *
     */
    public ColumnValue evaluate(Tuple t) {
	if (is_constant)
	    return null;
	else
	    return t.get_val(attrib_name);
    }

    public String toString() {
	if (is_constant)
	    return col_val.toString();
	else
	    return attrib_name;
		
    }
}