import java.util.*;

/**
 * SelectTable - this class implements the select operation of a
 * relational DB
 *
 */

public class SelectTable extends Table {

    Table tab_selecting_on; 
    Conditional select_cond;;

    /**
     * @param t - the table we are selecting from
     * @param c - the conditional used to make the selection
     *
     */
    public SelectTable(Table t, Conditional c) {

	super("Select on " + t.toString() + " on condition " + c.toString());
	tab_selecting_on = t;
	select_cond = c;

	attr_names = t.attrib_names();
	attr_types = t.attrib_types();
	
    }

    public Table [] my_children () {
	return new Table [] { tab_selecting_on };
    }

    public Table optimize() {
	return this;
    }

    public ArrayList<Tuple> evaluate() {

	if (select_cond instanceof ANDConditional) {
	    for (int i = 0; i < ((ANDConditional) select_cond).my_conds.length; i++)
		((ANDConditional)select_cond).my_conds[i].set_both_leaves_table(tab_selecting_on);
	}
	else
	    ((ComparisonConditional) select_cond).set_both_leaves_table(tab_selecting_on);
	ArrayList<Tuple> tuples1 = tab_selecting_on.evaluate();
	ArrayList<Tuple> tuples_to_return = new ArrayList<Tuple>();
	ListIterator iterate_tuples = tuples1.listIterator(0);

	while (iterate_tuples.hasNext()) {
	    Tuple x = (Tuple) iterate_tuples.next();
	    if (select_cond.truthVal(x)) {
		tuples_to_return.add(x);
	    }
	}
	profile_intermediate_tables(tuples_to_return);
	return tuples_to_return;
    }

}