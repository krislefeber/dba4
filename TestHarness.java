import java.util.Scanner;

public class TestHarness {

    static Table t1;
    static Table t2;
    static Table t3;
    static ComparisonConditional c [];
    static EQConditional e;
    static ANDConditional a;

    static void menu () {
	System.out.println("Test 1 -- Simple projection");
	System.out.println("Test 2 -- Simple selection");
	System.out.println("Test 3 -- Simple dup removal");
	System.out.println("Test 4 -- Simple selection");
	System.out.println("Test 5 -- Simple selection");
	System.out.println("Test 6 -- Simple join");
	System.out.println("Test 7 -- Simple join");
	System.out.println("Test 8 -- Simple join");
	System.out.println("Test 9 -- Simple join");
	System.out.println("Test 10 -- Select, dups, and project");
	System.out.println("Test 11 -- Dups and join");
	System.out.println("Test 12 -- Straight-forward join that can be significantly optimized");
	System.out.println("Test 13 -- Complicated join that can be significantly optimized");
	System.out.println("Test 14 -- Select on a join that produces many tuples");
	System.out.println("Test 15 -- A big, ugly test");
	System.out.println("Test 16 -- Another big, ugly test");
	System.out.print("Enter number of test to run, 0 to exit ");
    }

    public static void main (String args []) throws QueryException {
		
	Scanner in = new Scanner(System.in);
	int i;
	
	Table [] tests;
	tests = new Table[16];
	tests[0] = BuildTest1();
	tests[1] = BuildTest2();
	tests[2] = BuildTest3();
	tests[3] = BuildTest4();
	tests[4] = BuildTest5();
	tests[5] = BuildTest6();
	tests[6] = BuildTest7();
	tests[7] = BuildTest8();
	tests[8] = BuildTest9();
	tests[9] = BuildTest10();
	tests[10] = BuildTest11();
	tests[11] = null;	// Until you get join implemented, don't build this test
	//	tests[11] = BuildTest12();
	tests[12] = null;	// Until you get join implemented, don't build this test
	//	tests[12] = BuildTest13();
	tests[13] = null;	// Until you get join implemented, don't build this test
	//	tests[13] = BuildTest14();
	tests[14] = null;	// Until you get join implemented, don't build this test
	//	tests[14] = BuildTest15();
	tests[15] = null;	// Until you get join implemented, don't build this test
	//	tests[15] = BuildTest16();
	
	menu();
	i = in.nextInt();
	while (i > 0) {
		i--;
		tests[i].pretty_print_tree();
		tests[i].profile_and_display();
		System.out.println("\nThen we optimize the table before evaluating\n");
		tests[i] = tests[i].optimize();
		tests[i].pretty_print_tree();
		tests[i].profile_and_display();
		menu();
		i = in.nextInt();
	    }
    }

    // 1  -- Simple projection
    static Table BuildTest1 ()  throws QueryException {

	TableInDB tb = new TableInDB("Table1");
	ProjectionTable pt = new ProjectionTable (
		new ProjectionTable (tb,
				     new String [] {"Table1.foo1", "Table1.foo3", "Table1.foo4"}),
		new String [] { "Table1.foo3" });
	return pt;
    }

    // 2 -- Simple selection
    static Table BuildTest2 ()  throws QueryException {

	TableInDB tb = new TableInDB("Table1");
	ComparisonConditional c [] = new ComparisonConditional[2];
	c[0] = new EQConditional(new CondLeaf(tb, "Table1.foo2"), new CondLeaf( new IntValue("4") ) );
	c[1] = new EQConditional(new CondLeaf(tb, "Table1.foo3"), new CondLeaf( new IntValue("1") ) );
	ANDConditional a = new ANDConditional(c);
	ProjectionTable t = new ProjectionTable(new SelectTable (tb, a),
					      new String [] { "Table1.foo1",
							      "Table1.foo2",
							      "Table1.foo3"} );
	return t;
    }

    // 3 -- Simple remove dups
    static Table BuildTest3 ()  throws QueryException {

	Table t1 = new TableInDB("Table5");
	Table t2 = new DupsRemovedTable (t1);
	return t2;
    }

    // 4 -- Simple selection
    static Table BuildTest4 ()  throws QueryException {

	t1 = new TableInDB("Table3");
	c = new ComparisonConditional[2];
	c[0] = new EQConditional(new CondLeaf(t1, "Table3.foo2"), new CondLeaf( new IntValue("4") ) );
	c[1] = new EQConditional(new CondLeaf(t1, "Table3.foo3"), new CondLeaf( new IntValue("1") ) );
	a = new ANDConditional(c);
	t2 = new SelectTable (t1, a);
	return t2;
    }

    // 5 -- Simple Selection
    static Table BuildTest5 ()  throws QueryException {

	TableInDB tb = new TableInDB("Table3");
	ProjectionTable t = new ProjectionTable(tb, new String [] { "Table3.foo1",
								    "Table3.foo2",
								    "Table3.foo3"} );
	Conditional c = new EQConditional(new CondLeaf(tb, "Table3.foo2"), new CondLeaf( new IntValue("1") ) );
	SelectTable st = new SelectTable(t, c);
	return st;
    }

    // 6 -- Simple Join
    static Table BuildTest6 ()  throws QueryException {
	t1 = new TableInDB("Table5");
	t2 = new TableInDB("Table6");
	c = new ComparisonConditional[2];
	c[1] = new EQConditional(new CondLeaf(t1, "Table5.foo2"), new CondLeaf(t2, "Table6.foo2") );
	c[0] = new EQConditional(new CondLeaf(t1, "Table5.foo3"), new CondLeaf(t2, "Table6.foo3") );
	a = new ANDConditional(c);
	t3 = new JoinTable(t1,t2,a);
	return t3;
    }

    // 7 -- Simple Join
    static Table BuildTest7 ()  throws QueryException {
	TableInDB tb1 = new TableInDB("Table1");
	TableInDB tb2 = new TableInDB("Table2");

	Conditional c = new EQConditional(new CondLeaf(tb1, "Table1.foo2"), new CondLeaf(tb1, "Table1.foo3"));
	JoinTable t = new JoinTable(tb1, tb2, c);
	return t;
    }

    // 8 -- Simple join
    static Table BuildTest8 ()  throws QueryException {
	TableInDB tb1 = new TableInDB("Table1");
	TableInDB tb2 = new TableInDB("Table2");

	ComparisonConditional c [] = new ComparisonConditional[2];
	c[0] = new EQConditional(new CondLeaf(tb1, "Table1.foo3"), new CondLeaf(new IntValue("1")));
	c[1] = new EQConditional(new CondLeaf(tb2, "Table2.foo6"), new CondLeaf(new IntValue("3")));
	ANDConditional a = null;
	try {a = new ANDConditional(c);} catch(QueryException q) { }
	JoinTable t = new JoinTable(tb1, tb2, a);

	return t;
    }

    // 9 -- Simple join
    static Table BuildTest9 ()  throws QueryException {
	TableInDB tb1 = new TableInDB("Table1");
	TableInDB tb2 = new TableInDB("Table2");

	Conditional c = new EQConditional(new CondLeaf(tb1, "Table1.foo3"), new CondLeaf( new IntValue( "1" )));
	JoinTable t = new JoinTable(tb1, tb2, c);

	return t;
    }
    
    // 10 -- Dups, selection, and projection
    static Table BuildTest10 ()  throws QueryException {
	t1 = new TableInDB("Table3");
	c = new ComparisonConditional[2];
	c[0] = new EQConditional(new CondLeaf(t1, "Table3.foo2"), new CondLeaf( new IntValue("4") ) );
	c[1] = new EQConditional(new CondLeaf(t1, "Table3.foo3"), new CondLeaf( new IntValue("1") ) );
	a = new ANDConditional(c);
	t2 = new SelectTable (new DupsRemovedTable (new ProjectionTable (t1, new String [] {"Table3.foo1", "Table3.foo2", "Table3.foo3"} )), a);

	return t2;
    }

    // 11 - Dups and join
    static Table BuildTest11 ()  throws QueryException {
	TableInDB tb1 = new TableInDB("Table1");
	TableInDB tb2 = new TableInDB("Table2");
	EQConditional c = new EQConditional(new CondLeaf(tb1, "Table1.foo2"), new CondLeaf(tb2, "Table2.foo6") );
	JoinTable joiner = new JoinTable(tb1, tb2, c);
	DupsRemovedTable dt = new DupsRemovedTable (joiner);
	Conditional finalCond = new EQConditional(new CondLeaf(tb1, "Table1.foo1"), new CondLeaf(new StringValue("Winkler") ) );
	SelectTable st = new SelectTable(dt, finalCond);

	return st;
    }

    // 12 - Join that can be significantly optimized
    static Table BuildTest12 ()  throws QueryException {
	TableInDB tb1 = new TableInDB("Table1");
	TableInDB tb2 = new TableInDB("Table2");
	Conditional c = new EQConditional(new CondLeaf(tb1, "Table1.foo1"), new CondLeaf(new StringValue("West")));

	JoinTable jt = new JoinTable(tb1, tb2, c);
	ProjectionTable pt = new ProjectionTable (jt, 
						  new String[] {"Table2.foo5", "Table1.foo4"});
	return pt;
    }

    // 13 - Complicated Join that can be optimized
    static Table BuildTest13 ()  throws QueryException {
	TableInDB tb1 = new TableInDB("Table1");
	TableInDB tb2 = new TableInDB("Table2");
	ComparisonConditional c [] = new ComparisonConditional[4];
	c[0] = new EQConditional(new CondLeaf(tb1, "Table1.foo2"), new CondLeaf( new IntValue("4") ) );
	c[1] = new EQConditional(new CondLeaf(tb1, "Table1.foo3"), new CondLeaf( new IntValue("1") ) );
	c[2] = new EQConditional(new CondLeaf(tb2, "Table2.foo6"), new CondLeaf( new IntValue("4") ) );
	c[3] = new EQConditional(new CondLeaf(tb2, "Table2.foo7"), new CondLeaf( new IntValue("8") ) );

	ANDConditional a = null;

	try {
	    a = new ANDConditional(c);
	}
	catch(QueryException q)
	{	}

	JoinTable jt = new JoinTable(tb1, tb2, null);
	SelectTable st = new SelectTable(jt, a);

	return st;
    }

    // 14 -- This select produces lots of tuples
    static Table BuildTest14 ()  throws QueryException {
	TableInDB tb1 = new TableInDB("Table1");
	TableInDB tb2 = new TableInDB("Table2");
	JoinTable jt = new JoinTable(tb1, tb2, null);
	Conditional c = new EQConditional(new CondLeaf(tb1, "Table1.foo2"), new CondLeaf( new IntValue("4") ) );
	SelectTable st = new SelectTable(jt, c);

	return st;
    }

    static Table BuildTest15 ()  throws QueryException {
	t1 = new TableInDB("Table3");
	t2 = new TableInDB("Table4");
	c = new ComparisonConditional[4];
	c[0] = new EQConditional(new CondLeaf(t1, "Table3.foo2"), new CondLeaf( new IntValue("4") ) );
	c[1] = new EQConditional(new CondLeaf(t1, "Table3.foo3"), new CondLeaf( new IntValue("1") ) );
	c[2] = new EQConditional(new CondLeaf(t2, "Table4.foo6"), new CondLeaf( new IntValue("4") ) );
	c[3] = new EQConditional(new CondLeaf(t2, "Table4.foo7"), new CondLeaf( new IntValue("4") ) );
	e = new EQConditional(new CondLeaf(t1, "Table3.foo2"), new CondLeaf(t2, "Table4.foo6") );
	a = new ANDConditional(c);
	t3 = new SelectTable (
			      new DupsRemovedTable (
						    new ProjectionTable (
									 new JoinTable(t1,t2,e), new String [] {"Table3.foo1", "Table3.foo2", "Table3.foo3", "Table4.foo5", "Table4.foo6", "Table4.foo7" } )), a);


	return t3;
    }

    static Table BuildTest16 ()  throws QueryException {
	//Let's build a ridiculously large tree and see if we can break the optimizer or evaluator

	//Start at the bottom with two simple joins
	TableInDB tb1 = new TableInDB("Table1");
	TableInDB tb2 = new TableInDB("Table2");
	TableInDB tb3 = new TableInDB("Table3");
	TableInDB tb4 = new TableInDB("Table4");

	Conditional c1 = new EQConditional(new CondLeaf(tb1, "Table1.foo2"), new CondLeaf( new IntValue("4") ) );
	JoinTable jt1 = new JoinTable(tb1, tb2, c1);

	ComparisonConditional c [] = new ComparisonConditional[2];
	c[0] = new EQConditional(new CondLeaf(tb1, "Table3.foo2"), new CondLeaf( new IntValue("3") ) );
	c[1] = new EQConditional(new CondLeaf(tb1, "Table4.foo7"), new CondLeaf(tb1, "Table4.foo6") );

	ANDConditional a = null;

	try {
	    a = new ANDConditional(c);
	}
	catch(QueryException q)
	{	}

	JoinTable jt2 = new JoinTable(tb3, tb4, a);

    
	JoinTable jt3 = new JoinTable(jt1, jt2, null);

	ProjectionTable pt = new ProjectionTable (jt3, 
						  new String[] {"Table2.foo5", "Table3.foo2", "Table1.foo1", "Table3.foo2", "Table4.foo7"});

	Conditional c3 = (Conditional) new EQConditional(new CondLeaf(tb1, "Table1.foo1"), new CondLeaf( new StringValue("Atlas") ) );

	SelectTable s2 = new SelectTable(pt, c3);

	DupsRemovedTable dt = new DupsRemovedTable(s2);

	return dt;
    }



}
