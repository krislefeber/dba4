/**
 * Tuple.java - base class for all DB tuples
 *
 */

import java.lang.reflect.*;
import java.util.HashMap;

public class Tuple {
    private static long tuple_counter = 0;
    HashMap my_data;
    int num_attribs;

    /**
     * Constructor for the Tuple class
     * @param attrib_names - array of attribute names
     * @param attrib_types - array of attribute types.  Each type must
     * be one of "FloatValue", "IntValue", "StringValue"
     * @param attrib_values - the values of the attributes in their string representation
     *
     */
    public Tuple(String attrib_names [], String attrib_types [], String attrib_values []) {

	tuple_counter++;
	num_attribs = attrib_types.length;
	my_data = new HashMap();
	
	for (int i = 0; i < num_attribs; i++) {
	try {
	    Class cl = Class.forName(attrib_types[i]);
	    Constructor constructor =
		cl.getConstructor(new Class[] { String.class });
	    my_data.put ( attrib_names[i],
		(ColumnValue) constructor.newInstance
		(new Object[] { attrib_values[i] }));
	}
	catch (java.lang.ClassNotFoundException e)
	    { System.out.println(e.toString()); } 
	catch (java.lang.NoSuchMethodException e)
	    { System.out.println(e.toString()); }
	catch (java.lang.reflect.InvocationTargetException e)
	    { System.out.println(e.toString()); }
	catch (java.lang.InstantiationException e)
	    { System.out.println(e.toString()); }
	catch (java.lang.IllegalAccessException e)
	    {  System.out.println(e.toString()); }

	}
    }

    /**
     * @param attrib - the name of the attribute you want
     * @return the value of the desired attribute
     *
     */
    public ColumnValue get_val(String attrib) {
	tuple_counter++;
	return (ColumnValue) my_data.get(attrib);
    }

    /**
     * @return the total number of tuple accesses across all tuples
     *
     */
    public static long tuple_accesses() { return tuple_counter; }

    /**
     * Reset the tuple access counter to zero
     *
     */
    public static void reset_access_counter() { tuple_counter = 0; }
}
