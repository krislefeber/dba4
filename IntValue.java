/**
 * The class that wraps Java Integers as database Values.
 */
public class IntValue implements ColumnValue {
   private Integer val;
   
   /**
    * Create a constant by wrapping the specified Integer.
    * @param n the Integer value represented as a String
    */
   public IntValue(String n) {
      val = new Integer(n);
   }
   
   /**
    * Unwraps the Integer and returns it.
    */
   public Object asJavaVal() {
      return val;
   }
   
    /**
     * @param obj -- the object to compare to this object
     * @return true if this object and obj are equal
     *
     */
   public boolean equals(Object obj) {
      IntValue ic = (IntValue) obj;
      return ic != null && val.equals(ic.val);
   }
   
    /**
     * @param c - the value to compare to this object
     * @return -1 if this object precedes c, 0 if they are equal, 1 if
     * this object follows c
     *
     */
   public int compareTo(ColumnValue c) {
      IntValue ic = (IntValue) c;
      return val.compareTo(ic.val);
   }
   
   public int hashCode() {
      return val.hashCode();
   }
   
   public String toString() {
      return val.toString();
   }
}
