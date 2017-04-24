import java.util.*;

/**
 * The abstract that all relational tables must extend.
 */
public abstract class Table {

    protected String attr_names []; // The names of the attributes in the table
    protected String attr_types []; // The types of the attributes in the table - FloatValue, IntValue, StringValue
    private static long total_intermediate_trees; // Accumulates total of sizes of intermediate trees

    private String name;

    /**
     * Constructor
     * @param name - an identifying name for the table
     *
     */
    public Table (String name) {
	this.name = name; 
    }

    /**
     * Attribute names for the table.
     * @return the array of attribute names in this table
     *
     */
    public String [] attrib_names() {
	return attr_names;
    }

    /**
     * Attribute types for the table
     * @return the array of attribute types in this table -- each type
     * must be one of "FloatValue", "IntValue", "StringValue"
     *
     */
    public String [] attrib_types() {
	return attr_types;
    }

    /**
     * Identifying string
     * @return the identifying string for the table
     *
     */
    public String toString() {return name;}

    /**
     * Used to profile the sizes of the intermediate tables, determine
     * the number of tuple accesses used in evaluating the query tree,
     * and display the final results of that evaluation.
     */
    public void profile_and_display() {

	total_intermediate_trees = 0;
	
	Tuple.reset_access_counter();
	ArrayList<Tuple> displayList = evaluate();
	System.out.println("\nExecuting and profiling the query \n");
	for (int i = 0; i < attr_names.length; i++) {
	    System.out.print(String.format("%15s", attr_names[i]));
	}
	System.out.println("");

	for (int i = 0; i < attr_names.length; i++) {
	    System.out.print(String.format("%15s", "----------"));
	}
	System.out.println("");

	ListIterator iterate_tuples = displayList.listIterator(0);
	
	while (iterate_tuples.hasNext()) {
	    Tuple x = (Tuple) iterate_tuples.next();
	    for (int i = 0; i < attr_names.length; i++) {
		System.out.print(String.format("%15s", x.get_val(attr_names[i])));
	    }
	    System.out.println("");
	}
	System.out.println("\n\n");
	System.out.println("Sum of intermediate tree sizes " + total_intermediate_trees);
	System.out.println("Tuple accesses in evaluating " + Tuple.tuple_accesses());
    }

    /**
     * Helper for profile_and_display.  MUST be called by all
     * implementers of evaluate functions immediately before they
     * return their evaluate ArrayList, e.g.:
     * 
     * total_intermediate_trees += tuples_to_return.size();
     * return tuples_to_return;
     *
     * Failure to follow this protocol in your implementations of
     * evaluate will result in your automatically finishing in last
     * place in the optimization competition
     */
    protected void profile_intermediate_tables(ArrayList<Tuple> a) {
	total_intermediate_trees = total_intermediate_trees + a.size();
    }

    /**
     * Display the query tree in preorder form
     *
     */
    public void pretty_print_tree() {

	System.out.println("Pretty-printing the query tree: \n");
	pretty_print_tree_helper("");
    }

    /**
     * Recursive backend for pretty_print_tree
     *
     */
    public void pretty_print_tree_helper(String indent) {

	System.out.println(indent + name);
	if (!(this instanceof TableInDB)) {
	    Table [] t = my_children();
	    for (int i = 0; i < t.length; i++)
		t[i].pretty_print_tree_helper(indent + "----");
	}
    }

    /**
     * The children of this node in the query tree
     * @return array of the children of this table in the query tree
     */
    public abstract Table [] my_children ();

    /**
     * Optimize a query tree -- you'll spend lots of your time here to
     * get a good grade.
     *
     * @return a query tree that is functionally equivalent to the
     * original but has been optimized for efficiency in terms of the
     * sizes of intermediate tables
     *
     */
    public abstract Table optimize();

    /**
     * Evaluate a query tree -- you'll also spend lots of your time
     * here to get a good grade.  This will typically be called after
     * optimize is called.
     *
     * @return an ArrayList of Tuples that results of evaluating the
     * tree.
     *
     */
    public abstract ArrayList<Tuple> evaluate();
}
