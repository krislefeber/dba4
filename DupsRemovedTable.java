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
        attr_names = t.attr_names;
        attr_types = t.attr_types;
    }

    @Override
    public Table [] my_children () {
	return new Table [] { tab_dups_removed_from };
    }

    @Override
    public Table optimize() {
	
        // Right now no optimization is done -- you'll need to improve this
	if(this.tab_dups_removed_from instanceof DupsRemovedTable)
            return this.tab_dups_removed_from;
        else if(this.tab_dups_removed_from instanceof ProjectionTable) {
            ProjectionTable parent = (ProjectionTable)this.tab_dups_removed_from;
            Table child = parent.tab_projecting_on;
            this.tab_dups_removed_from = child;
            parent.tab_projecting_on = this;
            return parent;
        }
        return this;
    }	

    @Override
    public ArrayList<Tuple> evaluate() {
	ArrayList<Tuple> tuples_to_return = new ArrayList<>();
        
        ArrayList<Tuple> array = tab_dups_removed_from.evaluate();
        HashSet<Integer> hashes = new HashSet<>();
        ListIterator iterate_tuples = array.listIterator(0);

	while (iterate_tuples.hasNext()) {
	    Tuple x = (Tuple) iterate_tuples.next();
	    if (hashes.add(x.my_data.hashCode())) {
		tuples_to_return.add(x);
	    }
	}
	profile_intermediate_tables(tuples_to_return);
	return tuples_to_return;

    }	

}