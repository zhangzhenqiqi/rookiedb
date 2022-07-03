package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.RecordId;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * An inner node or a leaf node. See InnerNode and LeafNode for more
 * information.
 */
abstract class BPlusNode {
    // Core API ////////////////////////////////////////////////////////////////

    /**
     * 返回根据key对应查找到的可能驻留的叶子节点
     * <p>
     * n.get(k) returns the leaf node on which k may reside when queried from n.
     * For example, consider the following B+ tree (for brevity, only keys are
     * shown; record ids are omitted).
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |  3 |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * inner.get(x) should return
     * <p>
     * - leaf0 when x < 10,
     * - leaf1 when 10 <= x < 20, and
     * - leaf2 when x >= 20.
     * <p>
     * Note that inner.get(4) would return leaf0 even though leaf0 doesn't
     * actually contain 4.
     */
    public abstract LeafNode get(DataBox key);

    /**
     * 返回以当前节点为根的子树中的最左侧的叶子节点
     * n.getLeftmostLeaf() returns the leftmost leaf in the subtree rooted by n.
     * In the example above, inner.getLeftmostLeaf() would return leaf0, and
     * leaf1.getLeftmostLeaf() would return leaf1.
     */
    public abstract LeafNode getLeftmostLeaf();

    /**
     * 关于返回值<split-key,rid> 中，key表示分裂出的节点对应的键值，rid是这个节点的指针（页号表示）。
     * <br>对于叶子节点分裂，split key就是分裂出的新节点的第一个元素；
     * <br>而对于内部节点分裂，split key是中间元素，但注意它不包含在新节点的keys中。
     * n.put(k, r) inserts the pair (k, r) into the subtree rooted by n. There
     * are two cases to consider:
     *
     *   Case 1: If inserting the pair (k, r) does NOT cause n to overflow, then
     *           Optional.empty() is returned.
     *   Case 2: If inserting the pair (k, r) does cause the node n to overflow,
     *           then n is split into a left and right node (described more
     *           below) and a pair (split_key, right_node_page_num) is returned
     *           where right_node_page_num is the page number of the newly
     *           created right node, and the value of split_key depends on
     *           whether n is an inner node or a leaf node (described more below).
     *
     * Now we explain how to split nodes and which split keys to return. Let's
     * take a look at an example. Consider inserting the key 4 into the example
     * tree above. No nodes overflow (i.e. we always hit case 1). The tree then
     * looks like this:
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |  3 |  4 |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * Now let's insert key 5 into the tree. Now, leaf0 overflows and creates a
     * new right sibling leaf3. d entries remain in the left node; d + 1 entries
     * are moved to the right node. DO NOT REDISTRIBUTE ENTRIES ANY OTHER WAY. In
     * our example, leaf0 and leaf3 would look like this:
     *
     *   +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |    |    |->|  3 |  4 |  5 |    |
     *   +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf3
     *
     * When a leaf splits, it returns the first entry in the right node as the
     * split key. In this example, 3 is the split key. After leaf0 splits, inner
     * inserts the new key and child pointer into itself and hits case 1 (i.e. it
     * does not overflow). The tree looks like this:
     *
     *                          inner
     *                          +--+--+--+--+
     *                          | 3|10|20|  |
     *                          +--+--+--+--+
     *                         /   |  |   \
     *                 _______/    |  |    \_________
     *                /            |   \             \
     *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
     *   | 1| 2|  |  |->| 3| 4| 5|  |->|11|12|13|  |->|21|22|23|  |
     *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
     *   leaf0          leaf3          leaf1          leaf2
     *
     * When an inner node splits, the first d entries are kept in the left node
     * and the last d entries are moved to the right node. The middle entry is
     * moved (not copied) up as the split key. For example, we would split the
     * following order 2 inner node
     *
     *   +---+---+---+---+
     *   | 1 | 2 | 3 | 4 | 5
     *   +---+---+---+---+
     *
     * into the following two inner nodes
     *
     *   +---+---+---+---+  +---+---+---+---+
     *   | 1 | 2 |   |   |  | 4 | 5 |   |   |
     *   +---+---+---+---+  +---+---+---+---+
     *
     * with a split key of 3.
     * <p>
     * DO NOT redistribute entries in any other way besides what we have
     * described. For example, do not move entries between nodes to avoid
     * splitting.
     * <p>
     * Our B+ trees do not support duplicate entries with the same key. If a
     * duplicate key is inserted into a leaf node, the tree is left unchanged
     * and a BPlusTreeException is raised.
     */
    public abstract Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid);

    /**
     * 这个方法和put极为类似，但他巧妙地地方有很多：
     * <br> 思路如下：每次其实就是插入一批元素到B+树，
     * <p></p>在叶子时，一旦把叶子装满仍有data，
     * 那就新建一个叶子，然后往这个新叶子塞一部分data（取决于data的剩余数目以及页大小和fillFactor的限制），
     * 将这个新叶子作为一个splitNode返回相应的信息<新叶子中的第一个key,新叶子地址>
     * <p></p> 在内部节点，肯定是找到最右边那个儿子，然后递归的让这个儿子进行bulkLoad；注意这一步是可以重复的而不只是load一次就返回，
     * 因为内部节点也不是bulk一次key就满了（bulk一次其实就是走到叶子load，基本上一次就是load一个叶子节点那么多的数据），
     * 大多数情况肯定是还有剩余的，此时可以重复多次此操作直至触发split。
     *<p></p>当触发当前节点分裂时返回split info；否则返回Optional.empty
     * n.bulkLoad(data, fillFactor) bulk loads pairs of (k, r) from data into
     * the tree with the given fill factor.
     *
     * This method is very similar to n.put, with a couple of differences:
     *
     * 1. Leaf nodes do not fill up to 2*d+1 and split, but rather, fill up to
     * be 1 record more than fillFactor full, then "splits" by creating a right
     * sibling that contains just one record (leaving the original node with
     * the desired fill factor).
     * <p>
     * 2. Inner nodes should repeatedly try to bulk load the rightmost child
     * until either the inner node is full (in which case it should split)
     * or there is no more data.
     * <p>
     * fillFactor should ONLY be used for determining how full leaf nodes are
     * (not inner nodes), and calculations should round up, i.e. with d=5
     * and fillFactor=0.75, leaf nodes should be 8/10 full.
     * <p>
     * You can assume that 0 < fillFactor <= 1 for testing purposes, and that
     * a fill factor outside of that range will result in undefined behavior
     * (you're free to handle those cases however you like).
     */
    public abstract Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
                                                           float fillFactor);

    /**
     * n.remove(k) removes the key k and its corresponding record id from the
     * subtree rooted by n, or does nothing if the key k is not in the subtree.
     * REMOVE SHOULD NOT REBALANCE THE TREE. Simply delete the key and
     * corresponding record id. For example, running inner.remove(2) on the
     * example tree above would produce the following tree.
     * <p>
     * inner
     * +----+----+----+----+
     * | 10 | 20 |    |    |
     * +----+----+----+----+
     * /     |     \
     * ____/      |      \____
     * /           |           \
     * +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     * |  1 |  3 |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     * +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     * leaf0                  leaf1                  leaf2
     * <p>
     * Running inner.remove(1) on this tree would produce the following tree:
     * <p>
     * inner
     * +----+----+----+----+
     * | 10 | 20 |    |    |
     * +----+----+----+----+
     * /     |     \
     * ____/      |      \____
     * /           |           \
     * +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     * |  3 |    |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     * +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     * leaf0                  leaf1                  leaf2
     * <p>
     * Running inner.remove(3) would then produce the following tree:
     *
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |    |    |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     *
     * Again, do NOT rebalance the tree.
     */
    public abstract void remove(DataBox key);

    // Helpers /////////////////////////////////////////////////////////////////

    /**
     * Get the page on which this node is persisted.
     */
    abstract Page getPage();

    // Pretty Printing /////////////////////////////////////////////////////////

    /**
     * S-expressions (or sexps) are a compact way of encoding nested tree-like
     * structures (sort of like how JSON is a way of encoding nested dictionaries
     * and lists). n.toSexp() returns an sexp encoding of the subtree rooted by
     * n. For example, the following tree:
     *
     *                      +---+
     *                      | 3 |
     *                      +---+
     *                     /     \
     *   +---------+---------+  +---------+---------+
     *   | 1:(1 1) | 2:(2 2) |  | 3:(3 3) | 4:(4 4) |
     *   +---------+---------+  +---------+---------+
     *
     * has the following sexp
     * <p>
     * (((1 (1 1)) (2 (2 2))) 3 ((3 (3 3)) (4 (4 4))))
     * <p>
     * Here, (1 (1 1)) represents the mapping from key 1 to record id (1, 1).
     */
    public abstract String toSexp();

    /**
     * n.toDot() returns a fragment of a DOT file that draws the subtree rooted
     * at n.
     */
    public abstract String toDot();

    // Serialization ///////////////////////////////////////////////////////////

    /**
     * 将当前节点序列化为字节流
     *
     * @return
     */
    public abstract byte[] toBytes();

    /**
     * 根据文件在内存中生成对应的B+树节点，对应这个page
     * BPlusNode.fromBytes(m, p) loads a BPlusNode from page `pageNum`.
     */
    public static BPlusNode fromBytes(BPlusTreeMetadata metadata, BufferManager bufferManager,
                                      LockContext treeContext, long pageNum) {
        Page p = bufferManager.fetchPage(treeContext, pageNum);
        try {
            Buffer buf = p.getBuffer();
            byte b = buf.get();
            if (b == 1) {
                return LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
            } else if (b == 0) {
                return InnerNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
            } else {
                String msg = String.format("Unexpected byte %b.", b);
                throw new IllegalArgumentException(msg);
            }
        } finally {
            p.unpin();
        }
    }


    // List Utils  ////////////////////////////////////////////////////
    static <E> ArrayList<E> subList(List<E> list, int fromIndex, int size) {
        ArrayList<E> newList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            newList.add(list.get(fromIndex + i));
        }
        return newList;
    }

}
