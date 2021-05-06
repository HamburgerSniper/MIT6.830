package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    //add by HZH
    //--> 代替tdItems，使用List动态数组方便对数组内容进行操作
    private List<TDItem> descList = new ArrayList<>();
    //--> numFields
    private int fieldNum;

//    private TDItem[] tdItems;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    //工具类，便于组织每个字段的信息
    //field -- 数据字段，元数据
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        //field的类型
        public final Type fieldType;

        /**
         * The name of the field
         * */
        //field的名称
        public final String fieldName;

        //构造器
        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        /**
         * 重写equals()方法，首先进行三步判断：
         *      是否为null
         *      是否和this指向同一对象
         *      是否在TDItem的包含类型(int和String)中
         * 判断结束后 -- 类型转换为TDItem类对象
         * 判断fieldType是否相等，即判断是否为同一类型的对象。
         * @return 对象是否为同一类型
         */
        //some codes here -- (add by HZH)
        public boolean equals(Object o){
            if (o == null)
                return false;
            if (o == this)
                return true;
            if (!(o instanceof TDItem))
                return false;

            TDItem tdIo = (TDItem) o;
            return tdIo.fieldType == this.fieldType;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    //迭代此TupleDesc中包含的所有字段TDItem的迭代器
    public Iterator<TDItem> iterator() {
        // some code goes here(add by HZH)
//        return Arrays.stream(tdItems).iterator();
        return this.descList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    //typeAr: 统计TupleDesc中数据的总类型数
    //fieldAr:统计字段名称数量，可能为空
    public TupleDesc(Type[] typeAr, String[] fieldAr){
        // some code goes here
        if (typeAr.length != fieldAr.length){
            throw new IllegalArgumentException("The typeAr length must be equal to fieldAr length");
        }
        this.descList = new ArrayList<>(typeAr.length);
        this.fieldNum = typeAr.length;

        for (int i = 0; i < typeAr.length; i++) {
            final TDItem item = new TDItem(typeAr[i],fieldAr[i]);
            this.descList.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr){
        // some code goes here
        this(typeAr,new String[typeAr.length]);
    }

    /**
     * add by HZH , 解决报错 --> 新的构造器
     * @param itemList
     */
    public TupleDesc(final List<TDItem> itemList) {
        // some code goes here
        this.descList = new ArrayList<>(itemList);
        this.fieldNum = this.descList.size();
    }


    /**
     * @return the number of fields in this TupleDesc
     */
    //返回TDItem数组的大小
    public int numFields() {
        // some code goes here
        return this.fieldNum;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= this.fieldNum || i < 0){
            throw new NoSuchElementException();
        }
        return this.descList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= this.fieldNum){
            throw new NoSuchElementException();
        }
        return this.descList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null)
            throw new NoSuchElementException();
        for (int i = 0; i < this.fieldNum; i++) {
            if (name.equals(this.descList.get(i).fieldName)){
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i = 0; i < this.fieldNum; i++) {
            size += this.descList.get(0).fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2){
        // some code goes here
        final List<TDItem> newTupleDesc = new ArrayList<>();
        newTupleDesc.addAll(td1.descList);
        newTupleDesc.addAll(td2.descList);
        return new TupleDesc(newTupleDesc);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == null)
            return false;
        if (o == this)
            return true;
        if (!(o instanceof TupleDesc))
            return false;

        TupleDesc o1 = (TupleDesc) o;
        if (this.numFields() != o1.numFields())
            return false;
        Iterator<TDItem> it1 = o1.iterator();
        Iterator<TDItem> it2 = this.iterator();
        while (it2.hasNext()){
            //Override equals in TDItem
            if (!(it1.next().equals(it2.next())))
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        //some code goes here
        return "TupleDesc{" +
                "descList=" + descList +
                ", fieldNum=" + fieldNum +
                '}';
    }
}
