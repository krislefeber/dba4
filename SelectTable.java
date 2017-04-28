
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SelectTable - this class implements the select operation of a relational DB
 *
 */
public class SelectTable extends Table {

    Table tab_selecting_on;
    Conditional select_cond;

    /**
     * @param t - the table we are selecting from
     * @param c - the conditional used to make the selection
     *
     */
    public SelectTable(Table t, Conditional c) {
        super("Select on " + t.toString() + " on condition " + c.toString());
        String s = c.toString();
        tab_selecting_on = t;
        select_cond = c;

        this.attr_names = t.attrib_names();
        this.attr_types = t.attrib_types();

    }

    public Table[] my_children() {
        return new Table[]{tab_selecting_on};
    }

    public Table optimize() {
        if (this.tab_selecting_on instanceof ProjectionTable) {
            ProjectionTable parent = (ProjectionTable) this.tab_selecting_on;
            Table child = parent.tab_projecting_on;
            this.tab_selecting_on = child;
            parent.tab_projecting_on = this;
            return parent;
        } //if selecting on a select, group them into an AND condition
        else if (this.tab_selecting_on instanceof SelectTable) {
            SelectTable st = (SelectTable) this.tab_selecting_on;
            Table child = st.tab_selecting_on;
            Conditional childSelection = st.select_cond;
            if (this.select_cond instanceof ANDConditional) {
                ANDConditional ac = (ANDConditional) this.select_cond;
                List<ComparisonConditional> conds = Arrays.asList(ac.my_conds);
                if (childSelection instanceof ANDConditional) {
                    ANDConditional childAnd = (ANDConditional) childSelection;
                    conds.addAll(Arrays.asList(childAnd.my_conds));

                } else if (childSelection instanceof ComparisonConditional) {
                    ComparisonConditional childComp = (ComparisonConditional) childSelection;
                    conds.add(childComp);
                }
                try {
                    this.select_cond = new ANDConditional(conds.toArray(new ComparisonConditional[conds.size()]));
                } catch (QueryException ex) {
                    Logger.getLogger(SelectTable.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else if(this.select_cond instanceof ComparisonConditional) {
                ComparisonConditional cc = (ComparisonConditional)this.select_cond;               
                List<ComparisonConditional> conds = new ArrayList<>();
                conds.add(cc);
                if (childSelection instanceof ANDConditional) {
                    ANDConditional childAnd = (ANDConditional) childSelection;
                    conds.addAll(Arrays.asList(childAnd.my_conds));

                } else if (childSelection instanceof ComparisonConditional) {
                    ComparisonConditional childComp = (ComparisonConditional) childSelection;
                    conds.add(childComp);
                }
                try {
                    this.tab_selecting_on = st.tab_selecting_on;
                    this.select_cond = new ANDConditional(conds.toArray(new ComparisonConditional[conds.size()]));
                } catch (QueryException ex) {
                    Logger.getLogger(SelectTable.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

        return this;
    }

    @Override
    public ArrayList<Tuple> evaluate() {
        if (select_cond instanceof ANDConditional) {
            for (int i = 0; i < ((ANDConditional) select_cond).my_conds.length; i++) {
                ((ANDConditional) select_cond).my_conds[i].set_both_leaves_table(tab_selecting_on);
            }
        } else {
            ((ComparisonConditional) select_cond).set_both_leaves_table(tab_selecting_on);
        }
        ArrayList<Tuple> tuples1 = tab_selecting_on.evaluate();
        ArrayList<Tuple> tuples_to_return = new ArrayList<>();
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
