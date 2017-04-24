import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * TableInDB - this class emulates and implements a table from the database
 * itself, as opposed to an intermediate table. All leaf nodes in a query tree
 * will be of this type.
 */
public class TableInDB extends Table {

    private List<String[]> allElements = null;
    private ListIterator iterate_csv_vals = null;

    /**
     * @param csv_file - the name of a CSV file that is a simulated table in the
     * database
     *
     */
    public TableInDB(String csv_file) {

        super("Table from DB itself " + csv_file);
        try {
            CSVReader reader = new CSVReader(new FileReader("./" + csv_file));
            allElements = reader.readAll();
        } catch (java.io.IOException e) {
            
            System.out.println("reading from file " + csv_file);
            System.out.println(e.toString());
        }
        attr_names = allElements.remove(0);
        attr_types = allElements.remove(0);
    }

    @Override
    public Table[] my_children() {
        return null;
    }

    @Override
    public Table optimize() {
        return this;
    }

    @Override
    public ArrayList<Tuple> evaluate() {

        ListIterator iterate_csv_vals = allElements.listIterator(0);
        ArrayList<Tuple> tuples_to_return = new ArrayList<Tuple>();
        while (iterate_csv_vals.hasNext()) {
            String s[] = (String[]) iterate_csv_vals.next();
            Tuple t = new Tuple(attr_names, attr_types, s);
            tuples_to_return.add(t);
        }
        profile_intermediate_tables(tuples_to_return);
        return tuples_to_return;
    }
}
