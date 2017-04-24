/**
 * The class to use for constructing an AND conditional test
 *
 */
public class ANDConditional implements Conditional {
    
    ComparisonConditional my_conds [];

    /**
     * @param c - an array of comparison conditionals (LT, EQ, GT)
     * that are to be ANDed together
     *
     */
    public ANDConditional(ComparisonConditional [] c) throws QueryException {
	if (c.length < 2) throw new QueryException("Too few arguments for AND conditional");
	my_conds = new ComparisonConditional [c.length];
	System.arraycopy(c, 0, my_conds, 0, c.length);
    }

    /**
     * @return the true/false value of the array of conjuncts in this
     * AND Conditional for the Tuple t.
     *
     */
    public boolean truthVal(Tuple t) {
	boolean and_val = my_conds[0].truthVal(t);
	int i = 1;
	while ( (i < my_conds.length) & and_val ) {
	    and_val = and_val & my_conds[i].truthVal(t);
	    i++;
	}
	return and_val;
    }

    public String toString() {
	String s = new String ("AND ");
	for (int i = 0; i < my_conds.length; i++)
	    s += " " + my_conds[i].toString();
	return s;
    }
}