
public class TestProjection {
    public static void main (String args []) throws QueryException {
		
	TableInDB tb = new TableInDB("Table1");
	ProjectionTable pt = new ProjectionTable (
						  new ProjectionTable (tb,
								       new String [] {"Table1.foo1", "Table1.foo3", "Table1.foo4"}),
						  new String [] { "Table1.foo3" });
	pt.pretty_print_tree();
	pt.profile_and_display();

	System.out.println("\nThen we optimize the table before evaluating\n");
	pt = (ProjectionTable) pt.optimize();
	pt.pretty_print_tree();
	pt.profile_and_display();
    }
}
