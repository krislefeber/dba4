
import java.util.*;

/**
 * JoinTable - this class implements the join operation of a relational DB
 *
 */
public class JoinTable extends Table {

    Table first_join_tab;
    Table second_join_tab;
    Conditional cond;

    /**
     * @param t1 - One of the tables for the join
     * @param t2 - The other table for the join. You are guaranteed that the
     * tables do not share any common attribute names.
     * @param c - the conditional used to make the join
     *
     */
    public JoinTable(Table t1, Table t2, Conditional c) {

        super("Joining " + t1.toString() + " " + t2.toString() + " on condiition " + (c == null ? "": c.toString()));
        first_join_tab = t1;
        second_join_tab = t2;
        this.cond = c;
        System.out.println("Join table condition type: " + c);
        this.attr_names = getAttrNames(t1, t2);
        this.attr_types = getAttrTypes(t1, t2);
    }

    String[] getAttrNames(Table t1, Table t2) {
        String[] a = Arrays.copyOf(t1.attr_names, t1.attr_names.length + t2.attr_names.length);
        int j = 0;
        for (int i = t1.attr_names.length; i < a.length; i++) {
            a[i] = t2.attr_names[j++];
        }
        return a;
    }

    String[] getAttrTypes(Table t1, Table t2) {
        String[] a = Arrays.copyOf(t1.attr_types, t1.attr_types.length + t2.attr_types.length);
        int j = 0;
        for (int i = t1.attr_types.length; i < a.length; i++) {
            a[i] = t2.attr_types[j++];
        }
        return a;
    }

    public Table[] my_children() {
        return new Table[]{first_join_tab, second_join_tab};
    }

    public Table optimize() {
        // Right now no optimization is done -- you'll need to improve this
        return this;
    }

    public ArrayList<Tuple> evaluate() {
        ArrayList<Tuple> tuples_to_return = new ArrayList<Tuple>();
        //HashMap<String, String> // Here you need to add the correct tuples to tuples_to_return
        // for this operation
        CondLeaf[][] matches = null;
        if (this.cond instanceof ANDConditional) {
            ANDConditional a = (ANDConditional) this.cond;
            matches = new CondLeaf[a.my_conds.length][2];
            for (int i = 0; i < a.my_conds.length; i++) {
                matches[i][0] = a.my_conds[i].left;
                matches[i][1] = a.my_conds[i].right;
            }

        } else if (this.cond instanceof ComparisonConditional) {
            ComparisonConditional c = (ComparisonConditional) this.cond;
            matches = new CondLeaf[1][2];
            matches[0][0] = c.left;
            matches[0][1] = c.right;
        }
        // It should be done with an efficient algorithm based on
        // sorting or hashing

        profile_intermediate_tables(tuples_to_return);
        return joinTables(this.first_join_tab, this.second_join_tab, matches);

    }

    private ArrayList<Tuple> joinTables(Table left, Table right, CondLeaf[][] columnPairs) {
        ArrayList<Tuple> joinedTable = new ArrayList<>();
        Iterator<Tuple> iter1 = left.evaluate().iterator();
        while (iter1.hasNext()) {
            Tuple leftTuple = (Tuple) iter1.next();
            Iterator<Tuple> iter2 = right.evaluate().iterator();
            while (iter2.hasNext()) {
                Tuple rightTuple = (Tuple) iter2.next();
                boolean tablesMatch = true;
                if(columnPairs != null)
                for (CondLeaf[] pair : columnPairs) {
                    Object leftVal = leftTuple.my_data.get(pair[0].attrib_name);
                    if (leftVal == null) {
                        leftVal = rightTuple.my_data.get(pair[0].attrib_name);
                    }
                    Object rightVal = leftTuple.my_data.get(pair[1].attrib_name);
                    
                    if (rightVal == null) {
                        rightVal = rightTuple.my_data.get(pair[1].attrib_name);
                    }
                    
                    if(pair[1].is_constant) {
                        if(!leftVal.equals(pair[1].col_val))
                            tablesMatch = false;
                    }
                    else if (!leftVal.equals(rightVal)) {
                        tablesMatch = false;
                    }
                }
                if (tablesMatch) {
                    Iterator temp1 = leftTuple.my_data.entrySet().iterator();
                    Iterator temp2 = rightTuple.my_data.entrySet().iterator();
                    LinkedHashMap<String, ColumnValue> newMap = new LinkedHashMap<>();
                    while (temp1.hasNext()) {
                        Map.Entry<String, ColumnValue> val = (Map.Entry<String, ColumnValue>) temp1.next();
                        newMap.put(val.getKey(), val.getValue());
                    }
                    while (temp2.hasNext()) {
                        Map.Entry<String, ColumnValue> val = (Map.Entry<String, ColumnValue>) temp2.next();
                        newMap.put(val.getKey(), val.getValue());
                    }
                    String[] vals = new String[this.attr_names.length];
                    for (int i = 0; i < this.attr_names.length; i++) {
                        vals[i] = newMap.get(this.attr_names[i]).toString();
                    }
                    Tuple t = new Tuple(this.attr_names, this.attr_types, vals);
                    joinedTable.add(t);
                }
            }
        }
        return joinedTable;
    }

}
