package database;

import java.util.StringTokenizer;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Index {

	private String column;
	private Node root;
	private String oldChildEntry;
	private int indent;
	private String dataFileName;
	private StringBuffer bf;
	private String indexName;
	//private boolean indexModified;
	private ArrayList<IndexIterator> listOfIterators;
	private DataFile datafileObj;


	public Node getRoot() {
		return this.root;
	}

	public void removeIterator(IndexIterator obj) {
		this.listOfIterators.remove(obj);
	}

	public void delete(Node parentPointer, Node nodePointer, String entry ) {


		if ( ! nodePointer.is_leaf ) {
			/* node is not a leaf */
			int i =  0;
			for (i = 0; i < nodePointer.num_keys; i++) {
				if (nodePointer.keys[i].compareTo(entry) > 0) {
					break;
				}
			}
			delete(parentPointer, (Node)nodePointer.child[i], entry );

			if (oldChildEntry == null) {
				return;
			} else {
				// Remove the node for oldChildEntry.
				for (i = 0; i < nodePointer.num_keys; i++) {
					if (nodePointer.keys[i].compareTo(oldChildEntry) == 0) {
						break;
					}
				}
				for ( ; i < nodePointer.num_keys - 1; i++) {
					nodePointer.keys[i] = nodePointer.keys[ i + 1 ];
					nodePointer.child[i + 1] = nodePointer.child[i + 2];
				}
				nodePointer.keys[nodePointer.num_keys - 1] = null;
				nodePointer.child[nodePointer.num_keys] = null;
				nodePointer.num_keys --;

				if (nodePointer.parent == null) {
					if (nodePointer.num_keys == 0) {
						/* no keys in the root */
						this.root = (Node)nodePointer.child[0];
						this.root.parent = null;
					} 
					oldChildEntry = null;
					return;
				}
				if (nodePointer.num_keys >= (nodePointer.keys.length / 2)) {
					oldChildEntry = null;
					return;
				} else {
					Node sibiling = nodePointer.next;
					if (sibiling == null) {
						sibiling = nodePointer.previous;
						if (sibiling == null) {
							oldChildEntry = null;
							return;
						}
					}
					if (sibiling.parent != nodePointer.parent)
						sibiling = nodePointer.previous;
					
					if (sibiling.num_keys > (nodePointer.keys.length / 2)) {
						/* Have keys to share */
						reshuffelNodes(nodePointer, sibiling);
						oldChildEntry = null;
					} else {
						/* sibilings dont have sufficient keys. So merge both nodes */
						Node parent = nodePointer.parent;
						if (nodePointer.keys[nodePointer.num_keys - 1].compareTo(sibiling.keys[0]) < 0) {
							for (i = 0; i < parent.num_keys + 1; i++) {
								if (parent.child[i] == sibiling) {
									oldChildEntry = parent.keys[i - 1];
									break;
								}
							}
						} else {
							for (i = 0; i < parent.num_keys + 1; i++) {
								if (parent.child[i] == nodePointer) {
									oldChildEntry = parent.keys[i - 1];
									break;
								}
							}
							Node temp = sibiling;
							sibiling = nodePointer;
							nodePointer = temp;
						}
						//		if (sibiling.keys[0].compareTo(nodePointer.keys[nodePointer.num_keys - 1]) > 0) {
						nodePointer.keys[nodePointer.num_keys] = oldChildEntry;
						nodePointer.num_keys++;
						mergeNodes(nodePointer, sibiling);
						nodePointer.next = sibiling.next;
						if (sibiling.next != null) {
							((Node)sibiling.next).previous = nodePointer;
						}
						sibiling.previous = null;
						sibiling.next = null;
						sibiling.parent = null;
						/*	} else {
							sibiling.keys[sibiling.num_keys] = oldChildEntry;
							sibiling.num_keys++;
							mergeNodes(sibiling, nodePointer);

						}*/
					}
				}
			}
		} else {
			/* nodePointer is leaf */
			if (nodePointer.parent == null) {
				/* this is the root. So minimum delay doesnot exists. */
				deleteKey(nodePointer, entry);
				oldChildEntry = null;
				return;
			}
			if (nodePointer.num_keys > (nodePointer.keys.length / 2)) {
				/* Node Pointer has enough keys to spare */
				deleteKey(nodePointer, entry);
				oldChildEntry = null;
				return;
			} else {
				Node sibiling = (Node) nodePointer.child[nodePointer.child.length - 1];
				if (sibiling == null) {
					sibiling = nodePointer.previous;
					if (sibiling == null) {
						oldChildEntry = null;
						return;
					}
				} else if (nodePointer.parent != sibiling.parent){
					sibiling = nodePointer.previous;
				}

				if (!deleteKey(nodePointer, entry)) {
					oldChildEntry = null;
					return;
				}

				if (sibiling.num_keys > ( sibiling.keys.length / 2)) {
					// re shuffel
					if (nodePointer.keys[0].compareTo(sibiling.keys[sibiling.num_keys - 1]) < 0) {
						reshuffelKeys(nodePointer, sibiling);
					} else {
						reshuffelKeys(sibiling, nodePointer);
					}
				} else {

					if (nodePointer.keys[0].compareTo(sibiling.keys[sibiling.num_keys - 1]) > 0) {
						Node temp = nodePointer;
						nodePointer = sibiling;
						sibiling = temp;
					}
					Node parent = sibiling.parent;
					for ( int i = 0; i < parent.num_keys + 1; i++) {
						if (parent.child[i] == sibiling) {
							oldChildEntry = parent.keys[i -1];
							break;
						}
					}
					mergeLeaves(nodePointer, sibiling);
					nodePointer.child[nodePointer.child.length - 1] = sibiling.child[sibiling.child.length - 1];
					if (sibiling.child[sibiling.child.length - 1] != null) {
						((Node)sibiling.child[sibiling.child.length - 1]).previous = nodePointer;
					}
					sibiling.next = null;
					sibiling.previous = null;
					sibiling.parent = null;
					int i = 0;
					for (i = 0; i < sibiling.keys.length; i++) {
						sibiling.keys[i] = null;
						sibiling.child[i] = null;
					}
					sibiling.child[i] = null;
					sibiling.num_keys = 0;
				}
			}
		}
	}

	public void mergeNodes(Node nodePointer, Node sibiling) {

		int i = 0;
		String[] tempKeys = new String[nodePointer.keys.length * 2];
		Node[] tempChild = new Node[nodePointer.child.length * 2];

		for (i = 0; i < nodePointer.num_keys; i++) {
			tempKeys[i] = nodePointer.keys[i];
			tempChild[i] = (Node)nodePointer.child[i];
		}
		int j = 0;
		for (; j < sibiling.num_keys; j++) {
			tempKeys[j + i] = sibiling.keys[j];
			tempChild[i + j] = (Node)sibiling.child[j];			
		}
		tempChild[i + j] = (Node) sibiling.child[j];
		int totalKeys = i + j;
		for (i = 0; i < nodePointer.keys.length; i++) {
			nodePointer.keys[i] = null;
			sibiling.keys[i] = null;
			nodePointer.child[i] = null;
			sibiling.child[i] = null;
		}
		nodePointer.child[i] = null;
		sibiling.child[i] = null;
		sibiling.num_keys = 0;

		nodePointer.num_keys = 0;

		for (i = 0; i < totalKeys; i++) {
			nodePointer.keys[i] = tempKeys[i];
			nodePointer.child[i] = tempChild[i];
			nodePointer.num_keys++;
			((Node)nodePointer.child[i]).parent = nodePointer;
		}
		nodePointer.child[i] = tempChild[i];
		((Node)nodePointer.child[i]).parent = nodePointer;
	}

	public void reshuffelNodes(Node nodePointer, Node sibiling) {

		boolean swapped = false;
		if (nodePointer.keys[0].compareTo(sibiling.keys[0]) > 0) {
			Node temp = nodePointer;
			nodePointer = sibiling;
			sibiling = temp;
			swapped = true;

		}
		/* Normal case */
		/* 
		 * Find the parent node which have nodePointer 
		 * and sibiling as child 
		 */
		Node parent = nodePointer.parent;
		String parentKeys = null;
		int i = 0;
		for (i = 0; i < parent.num_keys + 1; i++) {
			if (parent.child[i] == nodePointer) {
				parentKeys = parent.keys[i];
				break;
			}
		}
		int parentIndex = i;
		String[] tempKeys = new String[nodePointer.keys.length * 2];
		Node[] tempChilds = new Node[nodePointer.child.length * 2];

		for (i = 0; i < nodePointer.num_keys; i++) {
			tempKeys[i] = nodePointer.keys[i];
			tempChilds[i] = (Node)nodePointer.child[i];
		}
		tempChilds[i] = (Node)nodePointer.child[i];
		int j;
		for (j = 0; j < sibiling.num_keys; j++) {
			tempKeys[i + j] = sibiling.keys[j];
			tempChilds[i + j + 1] = (Node)sibiling.child[j];
		}
		tempChilds[i + j + 1] = (Node)sibiling.child[j];
		int totalKeys = i + j;
		for (i = 0; i < nodePointer.keys.length; i++) {
			nodePointer.keys[i] = null;
			nodePointer.child[i] = null;
			sibiling.keys[i] = null;
			sibiling.child[i] = null;
		}
		nodePointer.child[i] = null;
		sibiling.child[i] = null;
		nodePointer.num_keys = 0;
		sibiling.num_keys = 0;


		for (i = 0; i < (nodePointer.keys.length / 2); i++) {
			nodePointer.keys[i] = tempKeys[i];
			nodePointer.child[i] = tempChilds[i];
			((Node)nodePointer.child[i]).parent = nodePointer;
			nodePointer.num_keys++;
		}
		nodePointer.child[i] = tempChilds[i];
		((Node)nodePointer.child[i]).parent = nodePointer;

		for (j = 0; i < totalKeys; i++, j++){
			sibiling.keys[j] = tempKeys[i];
			sibiling.child[j] = tempChilds[ i + 1 ];
			((Node)sibiling.child[j]).parent = sibiling;
			sibiling.num_keys++;
		}
		sibiling.child[j] = tempChilds[i + 1];
		((Node)sibiling.child[j]).parent = sibiling;
		if (!swapped) {
			for (i = 0; i < nodePointer.num_keys; i++) {
				if (nodePointer.keys[i].compareTo(parentKeys) > 0){
					String temp = nodePointer.keys[i];
					nodePointer.keys[i] = parentKeys;
					parentKeys = temp;
					break;
				}
			}
			parent.keys[parentIndex] = parentKeys;
		} else {
			for (i = 0; i < sibiling.num_keys; i++) {
				if (sibiling.keys[i].compareTo(parentKeys) >= 0)
					break;
			}
			j = sibiling.num_keys;
			for ( ;j > i; j--) {
				sibiling.keys[j] = sibiling.keys[j - 1];
			}
			sibiling.keys[i] = parentKeys;
			parent.keys[parentIndex] = sibiling.keys[0];
			for (i = 0; i < sibiling.num_keys; i++) 
				sibiling.keys[i] = sibiling.keys[i + 1];
		}

	}

	public void mergeLeaves(Node nodePointer, Node sibiling) {
		int j = nodePointer.num_keys;
		for (int i  = 0 ; i < sibiling.num_keys; i++) {
			nodePointer.keys[j + i] = sibiling.keys[i];
			nodePointer.child[j + i] = sibiling.child[i];
			nodePointer.num_keys++;
		}
		nodePointer.child[nodePointer.child.length - 1] = sibiling.child[sibiling.child.length - 1];
	}

	public boolean deleteKey(Node nodePointer, String entry) {
		int i;
		for (i = 0; i < nodePointer.num_keys; i++) {
			if (nodePointer.keys[i].compareTo(entry) == 0)
				break;
		}
		if (i == nodePointer.num_keys) {
			oldChildEntry = null;
			return false;
		}
		while (i < nodePointer.num_keys - 1) {
			nodePointer.child[i] = nodePointer.child[i + 1];
			nodePointer.keys[i] = nodePointer.keys[i + 1];
			i++;
		}
		nodePointer.keys[i] = null;
		nodePointer.child[i] = null;
		nodePointer.num_keys --;
		return true;
	}

	public void reshuffelKeys (Node nodePointer, Node sibiling) {

		String[] tempKeys = new String[nodePointer.keys.length * 2];
		Object[] tempChilds = new Object[nodePointer.child.length * 2];
		int i = 0; 
		for (i = 0; i < nodePointer.num_keys; i++) {
			tempKeys[i] = nodePointer.keys[i];
			tempChilds[i] = nodePointer.child[i];
		}
		nodePointer.num_keys = 0;
		int j = 0;
		for (j = 0; j < sibiling.num_keys; j++){
			tempKeys[i + j] = sibiling.keys[j];
			tempChilds[i + j] = sibiling.child[j];
		}

		sibiling.num_keys = 0;
		int totalKeys = i + j;
		for (i = 0; i < sibiling.keys.length; i++) {
			sibiling.keys[i] = null;
			sibiling.child[i] = null;
			nodePointer.keys[i] = null;
			nodePointer.child[i] = null;
		}
		for (i = 0; i < totalKeys / 2; i++) {
			nodePointer.keys[i] = tempKeys[i];
			nodePointer.child[i] = tempChilds[i];
			nodePointer.num_keys++;
		}
		for (j = 0; i < totalKeys ; i++, j++) {
			sibiling.keys[j] = tempKeys[i];
			sibiling.child[j] = tempChilds[i];
			sibiling.num_keys++;
		}
		/* Recalculate key for the parent */
		Node parent = sibiling.parent;
		if  (parent != null) {
			for (i = 0; i < parent.num_keys + 1; i++) {
				if  (parent.child[i] == sibiling)
					parent.keys[i - 1] = sibiling.keys[0];
			}
		}
	}

	private class IndexIterator implements Iterator<Integer> {

		private String compareString;
		public boolean indexModified;
		public boolean calledRemove;
		public int numberOfNodesRead;
		public boolean imodefied;

		public IndexIterator(Node curNode, String compareString) {
			/*
			 * Initialize the iterator to point
			 * to the first element in the B+tree
			 */
			this.compareString = compareString;
			this.indexModified = false;
			this.numberOfNodesRead = 0;
			this.imodefied = false;
		}

		public Node getExactPostion(Node temp, Integer[] position, boolean calledByRemove) {
			if (temp == null)
				return null;
			int i = 0;

			while (true) {
				if (i == temp.num_keys) {
					temp = (Node)temp.child[temp.keys.length];
					if (temp == null)
						return null;
					i = 0;
				}
				StringTokenizer st = new StringTokenizer(temp.keys[i], "|");
				String key = st.nextToken().trim();
				if (key.compareTo(this.compareString) == 0)
					break;
				if  (key.compareTo(this.compareString) > 0)
					return null;
				i++;
			}

			int nodesRead = this.numberOfNodesRead;
			if (calledByRemove)
				nodesRead--;

			while (nodesRead > 0) {
				i++;
				nodesRead--;
				if (i == temp.num_keys) {
					temp = (Node)temp.child[temp.keys.length];
					if (temp == null)
						return null;
					i = 0;
				}
			}
			position[0] = i;
			return temp;
		}

		public boolean hasNext() {

			if (this.indexModified) {
				throw new ConcurrentModificationException();
			}
			Integer[] position = new Integer[1];
			Node temp = findLeafToInsert(this.compareString + " | 0");
			temp = getExactPostion(temp, position, false);
			if (temp == null) {
				listOfIterators.remove(this);
				return false;
			}
			StringTokenizer st = new StringTokenizer(temp.keys[position[0]], "|");
			String key = st.nextToken().trim();
			if (key.compareTo(compareString) == 0)
				return true;
			listOfIterators.remove(this);
			return false;
		}

		@Override
		public Integer next() {
			if (this.indexModified) {
				throw new ConcurrentModificationException();
			}		
			Integer[] position = new Integer[1];
			Node temp = findLeafToInsert(compareString + " | 0");
			temp = this.getExactPostion(temp, position, false);
			if (temp == null) {
				listOfIterators.remove(this);
				throw new NoSuchElementException();
			}
			if (position[0] == temp.num_keys) {
				temp = (Node)temp.child[temp.keys.length];
				position[0] = 0;
			}
			if (temp == null){
				listOfIterators.remove(this);
				throw new NoSuchElementException();
			}

			this.calledRemove = false;
			this.imodefied = false;
			StringTokenizer st = new StringTokenizer(temp.keys[position[0]], "|");
			String key = st.nextToken().trim();
			if (key.compareTo(compareString) == 0) {
				this.numberOfNodesRead++;
				return (Integer)temp.child[position[0]];
			}
			listOfIterators.remove(this);
			throw new NoSuchElementException();


		}

		@Override
		public void remove() {

			if (this.indexModified) {
				throw new ConcurrentModificationException();
			}
			if (this.calledRemove) {
				listOfIterators.remove(this);
				throw new IllegalStateException();
			}
			if (this.numberOfNodesRead == 0) {
				listOfIterators.remove(this);
				throw new NoSuchElementException();
			}

			Integer[] position = new Integer[1];
			Node temp = findLeafToInsert(compareString + " | 0");
			temp = this.getExactPostion(temp, position, true);

			if (temp == null) {
				listOfIterators.remove(this);
				throw new NoSuchElementException();
			}

			Integer id = (Integer)temp.child[position[0]];
			delete(root, root, temp.keys[position[0]]);
			this.imodefied = true;
			datafileObj.deleteRow(id, null);
			this.numberOfNodesRead--;
			this.calledRemove = true;
	
		}
	}

	public void updateIterator() {
	
		Iterator<IndexIterator> it =  this.listOfIterators.iterator();
		while (it.hasNext()) {
			IndexIterator obj = it.next();
			if (!obj.imodefied) {
				obj.indexModified = true;
			}
		}
	}
	
	public Iterator<Integer> iterator(String key) {

		String tempkey = key + " | 0";
		Node curNode = this.findLeafToInsert(tempkey);
		IndexIterator it = new IndexIterator(curNode, key);
		this.listOfIterators.add(it);

		return it;

	}

	public Index(String column, DataFile obj,String indexName) {
		//public Index(String column, String indexName) {
		this.column = column;
		this.root = null;
		this.indent = 0;
		this.dataFileName = obj.getFileName();
		//this.dataFileName = indexName;
		this.indexName=indexName;
		this.bf = new StringBuffer();
		this.listOfIterators = new ArrayList<IndexIterator>();
		this.datafileObj = obj;
	}

	public void setRoot(Node newRoot) {
		this.root = newRoot;
	}

	public String getColumn() {
		return this.column;
	}

	public void insertValue(int recordId, String columnValue) {

		/* concatenate columnValue to recordId for DUPLICATE HANDLING */
		columnValue=columnValue+" | "+ recordId;

		if (null == this.root) {
			/*
			 * No Tree. Create a new tree
			 * and return.
			 */
			this.root = new Node();
			this.root.keys[0] = columnValue;
			this.root.child[0] = new Integer(recordId);
			this.root.is_leaf = true;
			this.root.num_keys++;
			this.root.parent = null;
			return;
		}

		Node curNode = findLeafToInsert(columnValue);

		if (curNode.num_keys < curNode.keys.length) {
			/*
			 * Current node can still have space left in it to hold the keys and
			 * values
			 */
			insertToLeaf(curNode, columnValue, recordId);
			return;
		}

		/*
		 * Cannot insert in the current node. Split the current node and insert.
		 */
		splitAndInsertToLeaf(curNode, columnValue, recordId);
		return;
	}

	public void insertToLeaf(Node curNode, String columnValue, int recordId) {

		int position = 0;
		while (position < curNode.num_keys && curNode.keys[position].compareTo(columnValue) < 0 )
			position++;
		int i = curNode.num_keys - 1;
		while (i >= position) {
			curNode.keys[i + 1] = curNode.keys[i];
			curNode.child[i + 1] = curNode.child[i];
			i--;
		}
		curNode.keys[position] = columnValue;
		curNode.child[position] = new Integer(recordId);
		curNode.num_keys++;

	}

	public void splitAndInsertToLeaf(Node curNode, String columnValue, int recordId) {

		/* 
		 * First copy all the keys and recids.
		 * + 1 is to accomodate the new key.
		 */
		String[] tempkeys = new String[curNode.keys.length + 1];
		Object[] tempRecIds = new Object[curNode.child.length + 1];

		int i = 0;
		for (i = 0; i < curNode.keys.length; i++) {
			tempkeys[i] = curNode.keys[i];
			tempRecIds[i] = curNode.child[i];
		}

		/* 
		 * Find the position for the new columnValue.
		 * and Record ids in temp array and insert it.
		 */
		int position = 0;
		while (position < tempkeys.length - 1 && tempkeys[position].compareTo(columnValue) < 0 )
			position++;

		/*
		 * If the position is at the end,
		 * there is nothing to shift.
		 */
		if (position != curNode.keys.length) {

			i = curNode.keys.length - 1;
			while (i >= position) {
				tempkeys[i + 1] = tempkeys[i];
				tempRecIds[i + 1] = tempRecIds[i];
				i--;
			}
		}

		tempkeys[position] = columnValue;
		tempRecIds[position] = recordId;
		/*
		 * For leaves the rightmost child will 
		 * point to its node at the right.
		 * Copy this pointer.
		 */

		tempRecIds[tempRecIds.length - 1] = (Object)curNode.child[curNode.child.length - 1];

		/*
		 * Copying of all pointers and recordids
		 * are done. Now its time to split the node.
		 */
		int splitpostion = split(tempkeys.length);
		for (i = 0; i < curNode.keys.length; i++) {
			curNode.keys[i] = null;
			curNode.child[i] = null;
		}
		/* 
		 * Copy half to the current node.
		 */
		curNode.num_keys = 0;
		for (i = 0; i < splitpostion; i++) {
			curNode.keys[i] = tempkeys[i];
			curNode.child[i] = tempRecIds[i];
			curNode.num_keys++;
		}
		/*
		 * Copy rest to new node.
		 */
		Node newNode = new Node();
		for (i = 0; splitpostion < tempkeys.length; i++, splitpostion++) {
			newNode.keys[i] = tempkeys[splitpostion];
			newNode.child[i] = tempRecIds[splitpostion];
			newNode.num_keys++;
		}
		newNode.is_leaf = true;
		/*
		 *  copy the right links 
		 */
		if (curNode.child[curNode.child.length - 1] != null) {
			((Node)(curNode.child[curNode.child.length - 1])).previous = newNode;
		}
		if (curNode.child[curNode.keys.length] != null) {
			((Node)curNode.child[curNode.keys.length]).previous = newNode;
		}
		curNode.child[curNode.keys.length] = newNode;
		newNode.child[newNode.keys.length] = tempRecIds[tempRecIds.length - 1];
		newNode.parent = curNode.parent;
		newNode.previous = curNode;

		if (newNode.parent == null) {
			/*
			 * It was the root. So now 
			 * create new root.
			 */
			createNewRoot(curNode, newNode, newNode.keys[0]);
		} else {
			if (curNode.parent.num_keys == curNode.parent.keys.length) {
				/* parent is also full */
				splitAndInsertToNode(newNode, newNode.parent, newNode.keys[0]);
			} else {
				/* Insert in to intermediate Node */
				insertToNode(newNode, newNode.parent, newNode.keys[0]);
			}
		}
	}

	private void splitAndInsertToNode(Node child, Node curNode, String childKey) {

		/* 
		 * First copy all the keys and recids.
		 * + 1 is to accomodate the new key.
		 */
		String[] tempkeys = new String[curNode.keys.length + 1];
		Object[] tempRecIds = new Object[curNode.child.length + 1];

		String key = childKey;
		int i = 0;
		for (i = 0; i < curNode.keys.length; i++) {
			tempkeys[i] = curNode.keys[i];
			tempRecIds[i] = curNode.child[i];
		}

		/*
		 * recids are one more than 
		 * the keys. Store this key in tempRecIds.
		 */
		tempRecIds[i] = curNode.child[i];

		int position = 0;
		while (position < tempkeys.length - 1 && tempkeys[position].compareTo(key) < 0 )
			position++;

		if (position != tempkeys.length - 1) {
			i = curNode.keys.length - 1;
			while(i >=  position) {
				tempkeys[i + 1] = tempkeys[i];
				i--;
			}
			i = curNode.child.length - 1;
			while(i >=  position) {
				tempRecIds[i + 1] = tempRecIds[i];
				i--;
			}
		}
		tempkeys[position] = key;
		tempRecIds[position + 1] = child;

		int splitpostion = split(tempkeys.length);
		for (i = 0; i < curNode.keys.length; i++) {
			curNode.keys[i] = null;
			curNode.child[i] = null;
		}

		curNode.child[i] = null;
		curNode.num_keys = 0;
		for (i = 0; i < splitpostion - 1; i++) {
			curNode.keys[i] = tempkeys[i];
			curNode.child[i] = tempRecIds[i];
			curNode.num_keys++;
		}
		curNode.child[i] = tempRecIds[i];
		String minKey = tempkeys[i];
		i++;
		Node newNode = new Node();
		newNode.is_leaf = false;
		newNode.parent = curNode.parent;
		int j = 0;
		for (j = 0; i < tempkeys.length; i++, j++) {
			newNode.child[j] = tempRecIds[i];
			newNode.keys[j] = tempkeys[i];
			newNode.num_keys++;
		}

		newNode.child[j] = tempRecIds[i];
		newNode.previous = curNode;
		if (curNode.next != null)
			curNode.next.previous = newNode;
		newNode.next = curNode.next;
		curNode.next = newNode;

		/* 
		 * Upadte childs parents. After splitting 
		 * the intermediate node.
		 */
		for (i = 0; i <= newNode.num_keys; i++) {
			Node temp = (Node)newNode.child[i];
			temp.parent = newNode;
		}
		for (i = 0; i < curNode.num_keys; i++) {
			Node temp = (Node) curNode.child[i];
			temp.parent = curNode;
		}

		if (newNode.parent == null) {
			createNewRoot(curNode, newNode, minKey);
		} else {
			if (curNode.parent.num_keys == curNode.parent.keys.length) {
				/* parent is also full */
				splitAndInsertToNode(newNode, newNode.parent, minKey);
			} else {
				/* Insert in to intermediate Node */
				insertToNode(newNode, newNode.parent, minKey);
			}
		}
	}

	private void insertToNode(Node curNode, Node parent, String minKey) {

		int position = 0;
		String key = minKey;
		while (position < parent.num_keys && parent.keys[position].compareTo(key) < 0 )
			position++;

		if (position != parent.num_keys) {
			int i = parent.num_keys - 1;
			while (i >= position) {
				parent.keys[i + 1] = parent.keys[i];
				i--;
			}
			i = parent.num_keys;
			while (i >= position + 1) {
				parent.child[i + 1] = parent.child[i];
				i--;
			}
		}
		parent.keys[position] = key;
		parent.child[position + 1] = curNode;
		parent.num_keys++;

	}

	private void createNewRoot(Node left, Node right, String minKey) {

		Node newRoot = new Node();
		newRoot.is_leaf = false;
		newRoot.parent = null;

		newRoot.keys[0] = minKey;
		newRoot.child[0] = left;
		newRoot.child[1] = right;
		newRoot.num_keys++;
		left.parent = newRoot;
		right.parent = newRoot;
		this.root = newRoot;
	}

	private int split(int input) {
		if (input % 2 == 0) {
			return input / 2;
		} else {
			return ((input / 2) + 1);
		}
	}

	public Node findLeafToInsert(String key) {

		Node temp = this.root;

		while (!temp.is_leaf) {
			int i = 0;
			for (i = 0; i < temp.num_keys; i++) {

				if (temp.keys[i].compareTo(key) > 0) {
					break;
				}
			}
			temp = (Node) temp.child[i];
		}
		return temp;
	}

	private String getIndentBuf(int indent) {
		StringBuffer buf = new StringBuffer();
		while(indent > 0) {
			buf.append("\t");
			indent--;
		}
		return buf.toString();
	}

	public void dumpIndex() {
		/* 
		 * Since the index name is unique only in 
		 * DataFiles create a directory named after
		 * DataFile name. and create an file for index inside this directory.
		 */
		StringBuffer buf = new StringBuffer();
		String path = "data/" + this.dataFileName + "/Index";
		File file = new File(path);
		if (! file.exists() ) {
			if ( ! file.mkdirs() ) {
				System.out.println("Unable to create directory : " + path);
				System.out.println("Dump of the Index is Unsuccessfull");
				return;
			}
		}
		path = "data/" + this.dataFileName + "/Index/" + this.indexName;
		File dumpfile = new File(path);
		try {
			FileOutputStream out = new FileOutputStream(dumpfile);
			buf.append(this.column);
			buf.append("\n");
			String str = getStringRep();
			buf.append(str);
			//System.out.println(buf.toString());
			out.write(buf.toString().getBytes());
			out.close();
		} catch(Exception e) {
			System.out.println("Caught Excption " + e.getClass());
		}
	}

	private String getStringRep() {

		Node temp = this.root;
		StringBuffer buf = new StringBuffer();
		while( ! temp.is_leaf ) {
			temp = (Node) temp.child[0];
		}
		/*
		 * Now we are at the left most child
		 * of  the btree. Pass through the entire
		 * list of childs untill you meet null.
		 */

		while (null != temp) {
			for (int i = 0; i < temp.num_keys; i++) {
				StringTokenizer keyToken=new StringTokenizer(temp.keys[i],"|");
				String key = keyToken.nextElement().toString();
				buf.append(key);
				buf.append(" ");
				buf.append(temp.child[i].toString());
				buf.append("\n");
			}
			temp = (Node) temp.child[temp.child.length - 1];
		}

		return buf.toString();
	}

	public void checkConsistency(Node parent, Node child) {

		int i = 0;
		for (i =0; i < parent.num_keys + 1; i++) {
			if (parent.child[i] == child)
				break;
		}
		if (i != 0) {
			for (int j = 0; j < child.num_keys; j++) {
				if (child.keys[j].compareTo(parent.keys[i - 1]) < 0) {
					System.out.println("Here i have error");
					System.out.println("Parent key = " + parent.keys[i - 1] + " child Key " + child.keys[j]);
				}
			}
		}
	}
	private void callRecursive(Node curNode) {

		if ( ! curNode.is_leaf ) {
			for(int i = 0; i < curNode.num_keys + 1; i++) {
				this.indent++;
				callRecursive((Node)curNode.child[i]);
				this.indent--;
				String indentBuf = getIndentBuf(this.indent);
				/*if (curNode.parent != null)
					checkConsistency(curNode.parent, curNode); */
				if (curNode.num_keys != i) {
					System.out.print(indentBuf);

					/* To print keys */
					/*StringTokenizer keyToken=new StringTokenizer(curNode.keys[i],"|");
					String key = keyToken.nextElement().toString().trim(); */ 
					this.bf.append(indentBuf);
					this.bf.append(curNode.keys[i]);
					this.bf.append("\n");
				}
			}
		}

		if (curNode.is_leaf) {
			StringBuffer buf = new StringBuffer();
			for (int i = 0; i < curNode.num_keys; i++) {
				String indentBuf = getIndentBuf(this.indent);
				buf.append(indentBuf);
				/* seperating "|" from key name */
				StringTokenizer leafKeyToken=new StringTokenizer(curNode.keys[i],"|");
				buf.append(leafKeyToken.nextElement().toString().trim());
				buf.append(" ");
				/* to print records */
				buf.append(curNode.child[i].toString().trim());
				buf.append("\n");
				//if (curNode.parent != null)
				//checkConsistency(curNode.parent, curNode);
				/*if (i != 0) {
					if (curNode.keys[i].compareTo(curNode.keys[i - 1]) < 0) {
						System.out.println("current key = " + curNode.keys[i] + " Previous key " + curNode.keys[i - 1]);
						System.out.println("I have an error");
					}
				}*/
				/*if (curNode.parent != null) {
					if (curNode.parent.num_keys == 0) {
						System.out.println("Here i have error " + curNode.keys[i]);
					}
				}*/
			}
			this.bf.append(buf.toString());
		}
	}

	public String viewIndex() {
		this.bf.setLength(0);
		this.bf.append("\n\n");
		callRecursive(this.root);
		//		System.out.print(this.bf.toString());
		return this.bf.toString();
	}

	/*public static void main(String[] args) {
		Index obj = new Index("shashankcolumn", "shashank");


		obj.insertValue(0, "aa");
		obj.insertValue(1, "ab");
		obj.insertValue(2, "ac");
		obj.insertValue(3, "ad");
		obj.insertValue(4, "ae");
		obj.insertValue(9, "af");
		obj.insertValue(5, "ag");
		obj.insertValue(6, "ah");
		obj.insertValue(7, "ai");
		obj.insertValue(8, "aj");
		obj.insertValue(9, "ak");
		obj.insertValue(10, "al");
		obj.insertValue(11, "am");
		obj.insertValue(121, "an");
		obj.insertValue(122, "an");
		obj.insertValue(123, "an");
		obj.insertValue(124, "an");
		obj.insertValue(125, "an");
		obj.insertValue(126, "an");
		obj.insertValue(127, "an");
		obj.insertValue(128, "an");
		obj.insertValue(129, "an");
		obj.insertValue(120, "an");
		obj.insertValue(1211, "an");
		obj.insertValue(1212, "an");
		obj.insertValue(1234, "an");
		obj.insertValue(1223, "an");
		obj.insertValue(1278, "an");
		obj.insertValue(1295, "an");
		obj.insertValue(1276, "an");
		obj.insertValue(1263, "an");
		obj.insertValue(1289, "an");
		obj.insertValue(1224, "an");
		obj.insertValue(12642, "an");
		obj.insertValue(1213, "an");
		obj.insertValue(1267, "an");
		obj.insertValue(1237, "an");
		obj.insertValue(1297, "an");
		obj.insertValue(1258, "an");
		obj.insertValue(1209, "an");
		obj.insertValue(13, "ao");
		obj.insertValue(13, "ap");
		obj.insertValue(13, "aq");
		obj.insertValue(13, "ar");
		obj.insertValue(13, "as");
		obj.insertValue(13, "at");
		obj.insertValue(13, "au");
		obj.insertValue(13, "av");
		obj.insertValue(13, "ax");
		obj.insertValue(13, "ay");
		obj.insertValue(13, "az");
		obj.insertValue(13, "aaa");
		obj.insertValue(13, "aab");
		obj.insertValue(13, "aba");
		obj.insertValue(13, "abb");
		obj.insertValue(13, "abc");
		obj.insertValue(13, "abd");
		obj.insertValue(13, "abe");
		obj.insertValue(13, "abf");
		obj.insertValue(13, "abg");
		obj.insertValue(13, "aba");
		obj.insertValue(13, "abi");
		obj.insertValue(13, "abj");
		obj.insertValue(13, "abk");
		obj.insertValue(13, "abl");
		obj.insertValue(13, "abm");
		obj.insertValue(13, "aan");
		obj.insertValue(13, "aada");
		obj.insertValue(13, "aadb");
		obj.insertValue(13, "aadc");
		obj.insertValue(13, "aadd");
		obj.insertValue(13, "aade");
		obj.insertValue(13, "aadf");
		obj.insertValue(13, "aadg");
		obj.insertValue(13, "aadh");
		obj.insertValue(13, "aadi");
		obj.insertValue(13, "aadj");
		obj.insertValue(13, "aadk");
		obj.insertValue(13, "aadl");
		obj.insertValue(13, "aadn");
		obj.insertValue(13, "aaem");
		obj.insertValue(13, "aafo");
		obj.insertValue(13, "aagp");
		obj.insertValue(13, "aahq");
		obj.insertValue(14, "aaha");
		obj.insertValue(14, "aahb");
		obj.insertValue(14, "aahc");
		obj.insertValue(14, "aahd");
		obj.insertValue(14, "aahf");
		obj.insertValue(14, "aahe");
		obj.insertValue(14, "aahg");
		obj.insertValue(14, "aahh");
		obj.insertValue(14, "aahi");
		obj.insertValue(14, "aahk");
		obj.insertValue(14, "aahl");
		obj.insertValue(14, "aahm");
		obj.insertValue(14, "aahn");
		obj.insertValue(14, "aaho");
		obj.insertValue(15, "aahp");
		obj.insertValue(16, "aahq");
		obj.insertValue(17, "aahr");
		obj.insertValue(18, "aahs");
		obj.insertValue(19, "aaht");
		obj.insertValue(20, "aahu");
		obj.insertValue(21, "aah");
		obj.insertValue(22, "aah");
		obj.insertValue(23, "aah");
		obj.insertValue(24, "aah");
		obj.insertValue(25, "aah");

		//System.out.println(obj.viewIndex());
		obj.delete(obj.getRoot(), obj.getRoot(), "av | 13");
		obj.delete(obj.getRoot(), obj.getRoot(), "ax | 13");
		obj.delete(obj.getRoot(), obj.getRoot(), "aa | 0");
		obj.delete(obj.getRoot(), obj.getRoot(), "aaa | 13");
		obj.delete(obj.getRoot(), obj.getRoot(), "aab | 13");
		obj.delete(obj.getRoot(), obj.getRoot(), "aab | 13");
		obj.delete(obj.getRoot(), obj.getRoot(), "aada | 13");
		obj.delete(obj.getRoot(), obj.getRoot(), "aadb | 13");
		obj.delete(obj.getRoot(), obj.getRoot(), "aadn | 13");
		obj.delete(obj.getRoot(), obj.getRoot(), "aaem | 13");
		obj.delete(obj.getRoot(), obj.getRoot(), "aafo | 13");
		obj.delete(obj.getRoot(), obj.getRoot(), "aagp | 13");
		obj.delete(obj.getRoot(), obj.getRoot(), "aah | 21");
		obj.delete(obj.getRoot(), obj.getRoot(), "aah | 22");
		obj.delete(obj.getRoot(), obj.getRoot(), "aah | 23");
		obj.delete(obj.getRoot(), obj.getRoot(), "aah | 24");
		obj.delete(obj.getRoot(), obj.getRoot(), "aah | 25");
		obj.delete(obj.getRoot(), obj.getRoot(), "aaha | 14");
		//obj.delete(obj.getRoot(), obj.getRoot(), "ay | 13");
		//obj.delete(obj.getRoot(), obj.getRoot(), "az | 13");
		//obj.delete(obj.getRoot(), obj.getRoot(), "az | 13");
		//obj.delete(obj.getRoot(), obj.getRoot(), "az | 13");
		//obj.delete(obj.getRoot(), obj.getRoot(), "az | 13");
		//obj.delete(obj.getRoot(), obj.getRoot(), "az | 13");



		System.out.println(obj.viewIndex());
	}*/

}
