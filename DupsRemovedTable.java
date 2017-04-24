import java.util.*;

/**
 * DupsRemovedTable - this class implements the duplicate removal operation of a
 * relational DB
 *
 */

public class DupsRemovedTable extends Table {

    Table tab_dups_removed_from;

    /**
     * @param t - the table from which duplicates are to be removed
     *
     */
    public DupsRemovedTable(Table t) {

	super("Removing duplicates from " + t.toString());
	tab_dups_removed_from = t;

    }

    @Override
    public Table [] my_children () {
	return new Table [] { tab_dups_removed_from };
    }

    @Override
    public Table optimize() {
	// Right now no optimization is done -- you'll need to improve this
	return this;
    }	

    public ArrayList<Tuple> evaluate() {
	ArrayList<Tuple> tuples_to_return = new ArrayList<Tuple>();

	// Here you need to add the correct tuples to tuples_to_return
	// for this operation

	// It should be done with an efficient algorithm based on
	// sorting or hashing

	profile_intermediate_tables(tuples_to_return);
	return tuples_to_return;

    }	

}