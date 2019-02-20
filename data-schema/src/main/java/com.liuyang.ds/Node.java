package com.liuyang.ds;

/**
 *  Node Class
 *  <ul>
 *      <li>2019/1/24 created.</li>
 *  </ul>
 * @param <K> Key Class.
 * @param <V> Value Class.
 *
 * @author liuyang
 * @version 1.0.0
 */
public final class Node<K, V> {

    /**
     * Fast to create one Node.
     * @param key the key.
     * @param value the value.
     * @param <K> the class of Key.
     * @param <V> the class of Value.
     * @return Return a Node.
     */
    public static <K, V> Node <K, V> create(K key, V value) {
        return new Node<>(key, value);
    }

    private K key;
    private V value;

    /**
     * Initial Node
     * @param key the key.
     * @param value the value.
     */
    public Node(K key, V value) {
        this.key   = key;
        this.value = value;
    }

    @Override
    protected final void finalize() {
        key   = null;
        value = null;
    }

    /**
     * Get Key.
     * @return Return Key.
     */
    public final K getKey() {
        return key;
    }

    /**
     * Get Value
     * @return Return Value.
     */
    public final V getValue() {
        return value;
    }
}
