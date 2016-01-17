package graphdb.util;


/**
 * The class <code>Pair</code> models a container for two objects wherein the
 * object order is of no consequence for equality and hashing. An example of
 * using Pair would be as the return type for a method that needs to return two
 * related objects. Another good use is as entries in a Set or keys in a Map
 * when only the unordered combination of two objects is of interest.<p>
 * The term "object" as being a one of a Pair can be loosely interpreted. A
 * Pair may have one or two <code>null</code> entries as values. Both values
 * may also be the same object.<p>
 * Mind that the order of the type parameters T and U is of no importance. A
 * Pair&lt;T, U> can still return <code>true</code> for method <code>equals</code>
 * called with a Pair&lt;U, T> argument.<p>
 * Instances of this class are immutable, but the provided values might not be.
 * This means the consistency of equality checks and the hash code is only as
 * strong as that of the value types.<p>
 */
public class Pair<L, R> implements Cloneable {

	/**
     * One of the two values, for the declared type T.
     */
    private final L left;
    /**
     * One of the two values, for the declared type U.
     */
    private final R right;
    private final boolean leftNull;
    private final boolean rightNull;
    private final boolean dualNull;

    /**
     * Constructs a new <code>Pair&lt;T, U&gt;</code> with T left and U right as
     * its values. The order of the arguments is of no consequence. One or both of
     * the values may be <code>null</code> and both values may be the same object.
     *
     * @param left T to serve as one value.
     * @param right U to serve as the other value.
     */
    public Pair(L left, R right) {

        this.left = left;
        this.right = right;
        leftNull = left == null;
        rightNull = right == null;
        dualNull = leftNull && rightNull;

    }

    /**
     * Gets the value of this Pair provided as the first argument in the constructor.
     *
     * @return a value of this Pair.
     */
    public L getLeft() {

        return left;

    }

    /**
     * Gets the value of this Pair provided as the second argument in the constructor.
     *
     * @return a value of this Pair.
     */
    public R getRight() {

        return right;

    }

    /**
     * Returns a shallow copy of this Pair. The returned Pair is a new instance
     * created with the same values as this Pair. The values themselves are not
     * cloned.
     *
     * @return a clone of this Pair.
     */
    @Override
    public Pair<L, R> clone() {

        return new Pair<L, R>(left, right);

    }

    /**
     * Indicates whether some other object is "equal" to this one.
     * This Pair is considered equal to the object if and only if
     * <ul>
     * <li>the Object argument is not null,
     * <li>the Object argument has a runtime type Pair or a subclass,
     * </ul>
     * AND
     * <ul>
     * <li>the Object argument refers to this pair
     * <li>OR this pair's values are both null and the other pair's values are both null
     * <li>OR this pair has one null value and the other pair has one null value and
     * the remaining non-null values of both pairs are equal
     * <li>OR both pairs have no null values and have value tuples &lt;v1, v2> of
     * this pair and &lt;o1, o2> of the other pair so that at least one of the
     * following statements is true:
     * <ul>
     * <li>v1 equals o1 and v2 equals o2
     * <li>v1 equals o2 and v2 equals o1
     * </ul>
     * </ul>
     * In any other case (such as when this pair has two null parts but the other
     * only one) this method returns false.<p>
     * The type parameters that were used for the other pair are of no importance.
     * A Pair&lt;T, U> can return <code>true</code> for equality testing with
     * a Pair&lt;T, V> even if V is neither a super- nor subtype of U, should
     * the the value equality checks be positive or the U and V type values
     * are both <code>null</code>. Type erasure for parameter types at compile
     * time means that type checks are delegated to calls of the <code>equals</code>
     * methods on the values themselves.
     *
     * @param obj the reference object with which to compare.
     * @return true if the object is a Pair equal to this one.
     */
    @Override
    public boolean equals(Object obj) {

        if(obj == null)
            return false;

        if(this == obj)
            return true;

        if(!(obj instanceof Pair<?, ?>))
            return false;

        final Pair<?, ?> otherPair = (Pair<?, ?>)obj;

        if(dualNull)
            return otherPair.dualNull;

        //After this we're sure at least one part in this is not null

        if(otherPair.dualNull)
            return false;

        //After this we're sure at least one part in obj is not null

        if(leftNull) {
            if(otherPair.leftNull) //Yes: this and other both have non-null part2
                return right.equals(otherPair.right);
            else if(otherPair.rightNull) //Yes: this has non-null part2, other has non-null part1
                return right.equals(otherPair.left);
            else //Remaining case: other has no non-null parts
                return false;
        } else if(rightNull) {
            if(otherPair.rightNull) //Yes: this and other both have non-null part1
                return left.equals(otherPair.left);
            else if(otherPair.leftNull) //Yes: this has non-null part1, other has non-null part2
                return left.equals(otherPair.right);
            else //Remaining case: other has no non-null parts
                return false;
        } else {
            //Transitive and symmetric requirements of equals will make sure
            //checking the following cases are sufficient
            if(left.equals(otherPair.left))
                return right.equals(otherPair.right);
            else if(left.equals(otherPair.right))
                return right.equals(otherPair.left);
            else
                return false;
        }

    }

    /**
     * Returns a hash code value for the pair. This is calculated as the sum
     * of the hash codes for the two values, wherein a value that is <code>null</code>
     * contributes 0 to the sum. This implementation adheres to the contract for
     * <code>hashCode()</code> as specified for <code>Object()</code>. The returned
     * value hash code consistently remain the same for multiple invocations
     * during an execution of a Java application, unless at least one of the pair
     * values has its hash code changed. That would imply information used for 
     * equals in the changed value(s) has also changed, which would carry that
     * change onto this class' <code>equals</code> implementation.
     *
     * @return a hash code for this Pair.
     */
    @Override
    public int hashCode() {

        int hashCode = leftNull ? 0 : left.hashCode();
        hashCode += (rightNull ? 0 : right.hashCode());
        return hashCode;

    }

    /**
     * Returns a string containing the toString outputs of the
     * values of Pair object. If custom object didn't implemented toString method
     * object signature is added to the output string.
     *
     * @return string output of this Pair
     */
    @Override
    public String toString() {
    	return new String("Pair{left:" + left + ", right:" + right + "}");
    }
}