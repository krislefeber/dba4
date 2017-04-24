/**
 * The class that wraps Java strings as database values.
 */
public class StringValue implements ColumnValue {
   private String val;
   
   /**
    * Create a value by wrapping the specified string.
    * @param s the string value
    */
   public StringValue(String s) {
      val = s;
   }
   
   /**
    * Unwraps the string and returns it.
    */
   public String asJavaVal() {
      return val;
   }
   
    /**
     * @param obj -- the object to compare to this object
     * @return true if this object and obj are equal
     *
     */
   public boolean equals(Object obj) {
      StringValue sc = (StringValue) obj;
      return sc != null && val.equals(sc.val);
   }
   
    /**
     * @param c - the value to compare to this object
     * @return -1 if this object precedes c, 0 if they are equal, 1 if
     * this object follows c
     *
     */
   public int compareTo(ColumnValue c) {
      StringValue sc = (StringValue) c;
      return val.compareTo(sc.val);
   }
   
   public int hashCode() {
      return val.hashCode();
   }
   
   public String toString() {
      return val;
   }
}
