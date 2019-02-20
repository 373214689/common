package com.liuyang.ds.attr;

import com.liuyang.ds.Schema;
import com.liuyang.ds.Type;

import java.util.ArrayList;
import java.util.List;

public class Struct implements Schema {

    /** 属性名称 */
    private String name;

    /** 属性类型： 规定了属性数据类型和取值范围 */
    private Type type = Type.STRUCT;

    /** 可空：确认属性值是否可空，默认可以为空 */
    private boolean nullable = true;

    private int index = 0;

    /** 记录name的hash值, 在hashCode方法中使用该值。 避免频繁调用String.hashCode方法。 */
    private int hash = 0;

    /** 主键 */
    private boolean primary = false;

    /** 子成员 */
    List<Schema> children;

    public Struct(String name) {
        this.name = name;
        this.children = new ArrayList();
    }

    public Struct clone() {
        return null;
    }

    @Override
    public int getIndex() {
        return 0;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Deprecated
    @Override
    public int getScale() {
        return 0;
    }

    @Deprecated
    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public Object getProperty(String name) {
        return null;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean isPrimary() {
        return primary;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public Struct addField(Schema field) {
        boolean exists = children.stream().anyMatch(e -> field.getName().equals(e.getName()));
        if (!exists) {
            field.setIndex(children.size());
            children.add(field);
        }

        return this;
    }

    public Schema getField(int index) {
        return children.get(index);
    }

    /**
     * 在指定索引前插入字段
     * @param index 指定索引
     * @param field 将要插入的字段
     * @return 返回本实例指针
     */
    public Struct insertField(int index, Schema field) {
        int size = children.size();
        if (index < size) {
            for (int i = index; i < size; i++) {
                children.get(i).setIndex(i + 1);
            }
        }
        field.setIndex(index + 1);
        children.add(index, field);
        return this;
    }

    public Schema removeField(int index) {
        int size = children.size();
        if (index < size) {
            for (int i = index; i < size; i++) {
                children.get(i).setIndex(i - 1);
            }
        }
        return children.remove(index);
    }

    public Schema[] getChildren() {
        /*int size = children.size();
        Schema[] retval = new Schema[size];
        for (int i = 0; i < size; i++) {

        }*/
        return children.stream().toArray(Schema[]::new);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("struct ").append(name).append(" {");
        builder.append(String.join(", ", children.stream().map(Schema::toString).toArray(String[]::new)));
        builder.append('}');
        return builder.toString();
    }
}
