package database;

public class Node {

	public Object[] child;
	public String[] keys;

	public Node parent;
	public int num_keys;
	public boolean is_leaf;
	public Node previous;
	public Node next;

	public Node() {

		this.child = new Object[9];
		this.keys = new String[8];
		this.is_leaf = false;
		this.num_keys = 0;
		this.previous = null;
		this.next = null;
	}
}
