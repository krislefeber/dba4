// Needs to be implemented by all conditional tests -- AND, LT, GT, EQ

public interface Conditional {

    /**
     * @return the true/false value of this conditional for the Tuple t.
     *
     */
    public boolean truthVal(Tuple t);

}