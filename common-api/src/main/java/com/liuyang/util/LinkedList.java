package com.liuyang.util;

import com.sun.istack.internal.NotNull;

import java.util.*;
import java.util.function.Consumer;

/**
 * 链表
 * <ul>
 *     <li>2019/2/12 ver1.0.0 liuyang 创建。</li>
 * </ul>
 * @param <E>
 *
 * @author liuyang
 * @version 1.0.0
 */
public class LinkedList<E> implements List<E> {
    /** 头部节点 */
    private transient volatile Node<E>       head     = null;
    /** 尾部节点 */
    private transient volatile Node<E>       tail     = null;
    /** 当前链表。初始化时指向本链表。*/
    private transient volatile LinkedList<E> current;
    /** 父级链表，此项必须要初始化时指定，指定后该链表作为子链表。*/
    private transient volatile LinkedList<E> parent   = null;
    /** 操作记录数 */
    private transient volatile long          modCount = 0;
    /** 链表长度 */
    private transient volatile int           size     = 0;
    /** 循环链表标识，循环链表头尾相连。 */
    private transient volatile boolean       loop;
    /** 循环链表游标 */
    private transient volatile Node<E>       cursor;

    private static <E> boolean check(E a, Object b) {
        if (a == null) {
            return b == null;
        } else {
            return a.equals(b);
        }
    }

    /**
     * 创建普通链表
     */
    public LinkedList() {
        this.current      = LinkedList.this;
        this.loop         = false;
    }

    /**
     * 建立循环链表
     * @param loop 链表是否可以循环
     */
    public LinkedList(boolean loop) {
        this.current       = LinkedList.this;
        this.loop          = false;
    }

    /**
     * 继承父链表中的一段数据
     * @param parent 指定父链表
     * @param offset 偏移
     * @param fromIndex 指定开始节点的索引
     * @param toIndex   指定结束节点的索引
     */
    private LinkedList(LinkedList<E> parent, int offset, int fromIndex, int toIndex) {
        this.current      = LinkedList.this;
        this.parent       = parent;
        this.size         = toIndex - fromIndex;
        this.modCount     = parent.modCount;
        this.loop         = false;
        // 定位头部节点和尾部节点
        this.head         = parent.search(fromIndex);
        this.tail         = parent.search(toIndex);
    }

    //==================================================================================================================
    // 链表核心功能
    //==================================================================================================================

    // 可用索引范围检查
    private void rangeCheck(int index) {
        if (index >= size && index < 0) {
            throw new IndexOutOfBoundsException("Range out of 0 - " + size + " (" + index + ")");
        }
    }

    // 如果期间父级链表发生改变，则抛出异常。
    private void checkForComodification() {
        if (parent == null)
            return;
        if (parent.modCount != current.modCount)
            throw new ConcurrentModificationException();
    }

    /**
     * 循环链表判定。如果是循环链表，则进行头尾链接。
     */
    private synchronized void loopLinked() {
        if (loop) {
            tail.next = head;
            head.prev = tail;
        }
    }

    // 追加节点。在未尾位置后追加。
    private synchronized Node<E> appendNode(@NotNull Node<E> node) {
        if (head == null) {
            // 当链表为空时，头部节点与尾部节点同时指定一个节点。
            tail = head = node;
            head.next = node;
        } else {
            node.prev = tail;
            // 修改 尾部节点 指向
            tail = tail.next = node;
        }
        // System.out.println("LinkedList.appendNode: " + node);
        loopLinked();
        size++;
        modCount++;
        return node;
    }

    // 追加节点。在指定位置后追加。
    private synchronized Node<E> appendNode(int index, @NotNull Node<E> node) {
        rangeCheck(index);
        if (index == 0) {
            return insertNodeA(head, node);
        } else if (index == size - 1) {
            return appendNode(node);
        } else {
            Node<E> curr = search(index);
            return insertNodeA(curr, node);
        }
    }

    // 在指定节点后面插入
    private synchronized Node<E> insertNodeA(@NotNull Node<E> curr, @NotNull Node<E> node) {
        if (curr == tail) {
            // 最如是将节点插入到末尾之后，则只需要使用 appendNode
            return appendNode(node);
        } else {
            node.prev = curr;
            node.next = curr.next;
            // 修改 当前节点的下一节点 与 当前节点的下一节点的前一个节点 指向为新插入的节点。
            curr.next = curr.next.prev = node;
            size++;
            modCount++;
        }
        return node;
    }

    // 在指定节点前面插入
    @SuppressWarnings({"unused"})
    private synchronized Node<E> insertNodeB(@NotNull Node<E> curr, @NotNull Node<E> node) {
        if (curr == head) {
            node.next = head;
            head = head.prev = node;
            loopLinked();
        } else {
            node.next = curr;
            node.prev = curr.prev;
            // 修改 当前节点的前一节点 与 当前节点的前一节点的下一节点 指向
            curr.prev = curr.prev.next = node;
        }
        size++;
        modCount++;
        return node;
    }

    // 删除头部节点
    private synchronized Node<E> removeHead() {
        Node<E> node = head;
        if (size == 1) {
            head = tail = null;
        } else {
            // 变更头部节点 指向 头部节点的下一节点。
            head = head.next;
            loopLinked();
        }
        size--;
        modCount++;
        return node;
    }

    // ****** 需优化，待修改
    // 删除节点。返回删除该节点之后的节点：<code>node.next</code>。
    private synchronized Node<E> removeNode(@NotNull Node<E> node) {
        if (node == head) {
            removeHead();
        } else if (node == tail) {
            removeTail();
        } else {
            // 指定节点.下一节点.上一节点 指向 指定节点.上一节点
            node.next.prev = node.prev;
            // 指定节点.上一节点.下一节点 指向 指定节点.下一节点
            node.prev.next = node.next;
            size--;
            modCount++;
        }
        return node;
    }

    // 删除尾部节点
    private synchronized Node<E> removeTail() {
        Node<E> node = tail;
        if (size == 1) {
            head = tail = null;
        } else {
            // 变更尾部节点 指向 尾部节点的上一节点
            tail = tail.prev;
            loopLinked();
        }
        size--;
        modCount++;
        return node;
    }

    // ******搜索算法待优化
    // 搜索节点
    // 头部节点和尾部节点可以直接检索，而中间节点可采集二分法检索，因为头部和尾节点是固定的相邻的。
    private Node<E> search(int index) {
        rangeCheck(index);
        if (size == 0)
            return null;
        if (index == 0) {
            return  head;
        } else if (index == size - 1) {
            return tail;
        } else {
            Node<E> curr = head;
            for (int i = 0; i < index; i++)
                curr = curr.next;
            return curr;
        }
            /*else if (index <= size / 2){
            // 二分检索前半部分
            Node<E> curr = head;
            for (int i = 0; i < index; i++)
                curr = curr.next;
            return curr;
        } else  {
            // 二分检索后半部分
            Node<E> curr = tail;
            for (int i = size; i > index; i--)
                curr = curr.next;
            return curr;
        }
        }*/
    }

    private Node<E> search(Object o) {
        if (size == 0)
            return null;
        if (check(head.item, o)) {
            return head;
        } else if (check(tail.item, o)) {
            return tail;
        } else {
            Node<E> curr = head;
            for (int i = 0; i < size; i++) {
                if (check(curr.item, o))
                    return curr;
                curr = curr.next;
            }
            return null;
        }
    }

    //==================================================================================================================
    // 链表接口功能
    //==================================================================================================================

    public final synchronized boolean add(E element) {
        checkForComodification();
        appendNode(new Node<>(element));
        return true;
    }

    public final synchronized void add(int index, E element) {
        rangeCheck(index);
        checkForComodification();
        appendNode(index, new Node<>(element));
    }

    public final synchronized boolean addAll(Collection<? extends E> collection) {
        checkForComodification();
        collection.forEach(this::add);
        return true;
    }

    public final synchronized boolean addAll(int index, Collection<? extends E> collection) {
        rangeCheck(index);
        checkForComodification();
        // 检索指定位置的节点
        Node<E> curr = search(index);
        // 在指定位置的节点后开始插入节点
        for(E element : collection)
            curr = insertNodeA(curr, new Node<>(element));
        return true;
    }

    public final boolean contains(Object o) {
        if (size == 1) {
            return check(head.item, o);
        } else {
            for (E element : current)
                if (check(element, o))
                    return true;
        }
        return false;
    }

    public final boolean containsAll(Collection<?> c) {
        // 遍历传入的集合，如果该链表中不含有指定集合中的某一个元素，则返回 false。
        for (Object o : c) {
            if (!contains(o))
                return false;
        }
        return true;
    }

    public final synchronized void clear() {
        tail.next = null;
        // 删除所有节点
        while (head.next != null) {
            head = head.next;
            size--;
            modCount++;
        }
    }

    public final boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof List))
            return false;
        // 如果比较对像是链表，则需要每个元素都匹配
        ListIterator<E> e1 = current.listIterator();
        ListIterator<?> e2 = ((List<?>) o).listIterator();
        while (e1.hasNext() && e2.hasNext()) {
            E o1 = e1.next();
            Object o2 = e2.next();
            if (!check(o1, o2))
                return false;
        }
        return !(e1.hasNext() || e2.hasNext());
    }

    public final void forEach(Consumer<? super E> action) {
        for(E element : current)
            action.accept(element);

    }

    /**
     * 反序为每个成员执行相同的动作。
     * @param action 指定需要执行的命令。
     */
    public final void forEachOrderReverse(Consumer<? super E> action) {
        ListIterator<E> iter = current.listIterator(current.size);
        while(iter.hasPrevious())
            action.accept(iter.previous());
    }

    public final E get(int index) {
        rangeCheck(index);
        Node<E> node = current.search(index);
        return node != null ? node.item : null;
    }

    /**
     * 获取指定索引位置的数据。
     * @param index 指定索引。
     * @param element 如果指定位置没有数据，则返回该数据。
     * @return 返回一个可用数据。
     */
    public final E get(int index, E element) {
        rangeCheck(index);
        Node<E> node = current.search(index);
        return node != null ? (node.item != null ? node.item : element) : element;
    }

    public int hashCode() {
        int hashCode = 1;
        for (E e : current)
            hashCode = 31 * hashCode + (e == null ? 0 : e.hashCode());
        return hashCode;
    }

    public final synchronized boolean isEmpty() {
        return size == 0;
    }

    public final int indexOf(Object o) {
        Node<E> curr = head;
        for (int i = 0; i < size; i++) {
            if (check(curr.item, o))
                return i;
            curr = curr.next;
        }
        return -1;
    }

    public final synchronized Iterator<E> iterator() {
        return new Itr();
    }

    public final int lastIndexOf(Object o) {
        Node<E> curr = tail;
        for (int i = size - 1; i >= 0; i++) {
            if (check(curr.item, o))
                return i;
            curr = curr.next;
        }
        return -1;
    }

    public final ListIterator<E> listIterator() {
        return new ListItr(0);
    }

    public final  ListIterator<E> listIterator(int index) {
        return new ListItr(index);
    }

    /**
     * 下一个成员
     * <p>
     *     此项功能用于循环链表数据。
     * </p>
     * @return 返回下一个成员。
     */
    @SuppressWarnings({"unused"})
    public final synchronized E next() {
        if (cursor == null)
            cursor = head;
        E value = cursor == null ? null : cursor.item;
        cursor = cursor.next;
        return value;
    }

    public final synchronized E remove(int index) {
        rangeCheck(index);
        if (index == 0) {
            return removeHead().item;
        } else if (index == size - 1) {
            return removeTail().item;
        } else {
            Node<E> curr = search(index);
            removeNode(curr);
            return curr != null ? curr.item : null;
        }
    }

    /**
     * 上一个成员
     * <p>
     *     此项功能用于循环链表。
     * </p>
     * @return 返回上一个成员数据。
     */
    @SuppressWarnings({"unused"})
    public final synchronized E prev() {
        if (cursor == null)
            cursor = tail;
        E value = cursor == null ? null : cursor.item;
        cursor = cursor.prev;
        return value;
    }

    public final synchronized boolean remove(Object o) {
        if (size <= 0) {
            return false;
        } else if (size == 1) {
            if (check(head.item, o))
                return removeHead() != null;
            else
                return false;
        } else {
            Node<E> curr = current.search(o);
            removeNode(curr);
            return curr != null && curr.item != null;
        }
    }

    public final synchronized boolean removeAll(Collection<?> c) {
        // 遍历传入的集合，如果该链表中不含有指定集合中的某一个元素，则返回 false。
        for (Object o : c)
            remove(o);
        return !current.isEmpty();
    }

    public final synchronized boolean retainAll(Collection<?> c) {
        Node<E> curr = head;
        for (int i = 0; i < size; i++) {
            if (!c.contains(curr.item))
                current.removeNode(curr);
            curr = curr.next;
        }
        return !current.isEmpty();
    }

    public final synchronized E set(int index, E element) {
        rangeCheck(index);
        checkForComodification();
        Node<E> curr = search(index);
        E value = null;
        if (curr != null) {
            value = curr.item;
            curr.item = element;
            current.modCount++;
        }
        return value;
    }

    public final int size() {
        return size;
    }

    /*public final Spliterator<E> spliterator() {
        return new ListSpliterator<>(current, 0, -1, modCount);
    }*/

    public final List<E> subList(int fromIndex, int toIndex) {
        return new LinkedList<>(current, 0, fromIndex, toIndex);
    }

    public final synchronized Object[] toArray() {
        if (size <= 0) {
            return new Object[]{};
        }
        Object[] values = new Object[size];
        Node<E> curr = current.head;
        for (int i = 0; i < size; i++) {
            values[i] = curr.item;
            curr = curr.next;
        }
        return values;
    }

    @SuppressWarnings({"unchecked"})
    public final synchronized <T> T[] toArray(@NotNull T[] a) {
        Node<E> curr = current.head;
        int length = a.length > size ? size : a.length;
        for (int i = 0; i < length; i++) {
            a[i] = (T) curr.item;
            curr = curr.next;
        }
        return a;
    }

    //==================================================================================================================
    // 链表内部接口实现
    //==================================================================================================================

    /**
     * 双向节点
     * @author liuyang
     */
    private final static class Node<E> {
        volatile Node<E> prev;
        volatile Node<E> next;
        volatile E item;

        Node(E value) {
            this.item = value;
        }

        @Override
        public boolean equals(Object anObject) {
            if (this == anObject) {
                return true;
            }
            if (anObject == null) {
                return item == null;
            }
            if (anObject == item) {
                return true;
            }
            if (anObject instanceof Node) {
                Node other = (Node) anObject;
                return check(other.item, item);
            }
            return false;
        }

        @Override
        public String toString() {
            return String.valueOf(item);
        }
    }

    /**
     * 迭代器
     */
    private class Itr implements Iterator<E> {
        int cursor  = 0;
        Node<E> curr;

        Itr() {
            curr = current.head;
        }

        public boolean hasNext() {
            //System.out.println("LinkedList.Itr.hasNext.cursor: " + cursor + "/" + current.size);
            return cursor < current.size && cursor >= 0;
        }

        public E next() {
            E next = curr.item;
            curr = curr.next;
            cursor++;
            return next;
        }

        public void remove() {
            curr = removeNode(curr).next;
            cursor--; // 减少cursor
        }
    }

    /**
     * 链表迭代器
     */
    private final class ListItr extends Itr implements ListIterator<E> {
        ListItr(int index) {
            rangeCheck(index);
            if (index >= current.size) {
                curr   = current.tail;
                cursor = current.size;
            } else {
                for (; cursor < index; cursor++) {
                    curr = curr.next;
                }
            }
        }

        public boolean hasPrevious() {
            //System.out.println("LinkedList.ListItr.hasPrevious.cursor: " + cursor + "/" + current.size);
            return cursor > 0;
        }

        public E previous() {
            E prev = curr.item;
            curr = curr.prev;
            cursor--;
            return prev;
        }

        public int nextIndex() {
            return cursor;
        }

        public int previousIndex() {
            return cursor - 1;
        }


        public void set(E e) {
            curr.item = e;
        }

        public void add(E e) {
            insertNodeA(curr, new Node<>(e));
        }
    }

    /*
    static int count = 0;
    private final class ListSpliterator<E> implements Spliterator<E> {

        LinkedList<E> list;
        long modCount;
        int fence;
        int index;
        int origin;
        Node<E> curr;


        ListSpliterator(LinkedList<E> list, int origin, int fence, long modCount) {
            this.list     = list;
            this.origin   = origin;
            this.index    = origin;
            this.fence    = (fence < 0  || fence > current.size) ? current.size : fence;
            this.modCount = (fence < 0  || fence > current.size) ? list.modCount : modCount;
            this.curr     = list.search(origin);
            System.out.println("LinkedList.ListSpliterator.init: (" + count++ + ") origin: " + origin + ", fence: " + fence);
        }

        public boolean tryAdvance(Consumer<? super E> action) {
            if (action == null)
                throw new NullPointerException();
            if (index < fence) {
                index++;
                curr = curr.next;
                action.accept(curr.item);
                if (list.modCount != ListSpliterator.this.modCount)
                    throw new ConcurrentModificationException();
                return true;
            }
            return false;
        }

        public ListSpliterator<E> trySplit() {
            int hi = fence, lo = index, mid = (lo + hi) >>> 1;
            return (lo >= mid) ? null : // divide range in half unless too small
                    new ListSpliterator<E>(list, lo, index = mid, modCount);
        }

        public void forEachRemaining(Consumer<? super E> action) {
            if (action == null)
                throw new NullPointerException();
            // 记录操作数
            long mc = list.modCount;
            if (index >= 0 && fence <= list.size) {
                for (;index < fence; index++) {
                    action.accept(curr.item);
                    curr = curr.next;
                }
            }
            // 如果期间链表操作数改变，则抛出异常。
            if (list.modCount != mc)
                throw new ConcurrentModificationException();
        }

        public long estimateSize() {
            return (long) (fence - index);
        }

        public int characteristics() {
            return Spliterator.ORDERED | Spliterator.SIZED | Spliterator.SUBSIZED;
        }

    }*/

}
