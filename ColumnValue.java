/**
 * The interface that denotes values stored in the database.
 */
public interface ColumnValue extends Comparable<ColumnValue> {
   
   /**
    * Returns the Java object corresponding to this column's value.
    * @return the Java value of the column in the tuple
    */
   public Object  asJavaVal();
}
