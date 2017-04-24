
class TestSelect {
    
    public static void main (String args []) throws QueryException {
	
	TableInDB tb = new TableInDB("Table1");
	ComparisonConditional c [] = new ComparisonConditional[2];
	c[0] = new EQConditional(new CondLeaf(tb, "Table1.foo2"), new CondLeaf( new IntValue("4") ) );
	c[1] = new EQConditional(new CondLeaf(tb, "Table1.foo3"), new CondLeaf( new IntValue("1") ) );
	ANDConditional a = new ANDConditional(c);
	ProjectionTable t = new ProjectionTable(new SelectTable (tb, a),
					      new String [] { "Table1.foo1",
							      "Table1.foo2",
							      "Table1.foo3"} );
	t = (ProjectionTable) t.optimize();
	t.pretty_print_tree();
	t.profile_and_display();
    }
}
