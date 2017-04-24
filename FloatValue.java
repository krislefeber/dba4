/**
 * The class that wraps Java Floats as database values.
 */
public class FloatValue implements ColumnValue {
   private Float val;
   
   /**
    * Create a constant by wrapping the specified Float.
    * @param f the Float value represented as a String
    */
   public FloatValue(String f) {
       val = new Float(f);
   }
   
   /**
    * Unwraps the Float and returns it.
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
      FloatValue ic = (FloatValue) obj;
      return ic != null && val.equals(ic.val);
   }
   
    /**
     * @param c - the value to compare to this object
     * @return -1 if this object precedes c, 0 if they are equal, 1 if
     * this object follows c
     *
     */
   public int compareTo(ColumnValue c) {
      FloatValue ic = (FloatValue) c;
      return val.compareTo(ic.val);
   }
   
   public int hashCode() {
      return val.hashCode();
   }
   
   public String toString() {
      return val.toString();
   }
}
