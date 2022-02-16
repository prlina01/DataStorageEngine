package MerkleTree

import (
	"awesomeProject5/Application/WriteAheadLog"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

type MerkleRoot struct {
	root *Node
}


func (mr *MerkleRoot) String() string{
	return mr.root.String()
}

type Node struct {
	data [20]byte
	left *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}



type MerkleTree struct{
	root MerkleRoot
}

func CreateNodes(Lines []WriteAheadLog.Line) []Node {
	var nodes []Node
	for i:=range Lines{
		nodes = append(nodes, Node{Hash(WriteAheadLog.SerializeLine(Lines[i])),nil,nil})
	}
	return nodes
}
func (mt *MerkleTree) BuildMT( lines []Node) []Node {
	var nodes []Node
	var i int
	for i = 0; i < len(lines); i += 2 {
		if i + 1 < len(lines) {
			var data []byte
			data = append(data,lines[i].data[:]...)
			data = append(data,lines[i+1].data[:]...)
			nodes = append(nodes, Node{data: Hash(data), left: &lines[i], right: &lines[i+1]})
		} else {
			var data []byte
			data = append(data,lines[i].data[:20]...)
			nodes = append(nodes, Node{data: Hash(data), left: &lines[i], right: nil})
		}
	}
	if len(nodes) == 1 {
		mt.root.root = &nodes[0]
		return nodes
	} else {
		return mt.BuildMT(nodes)
	}

}

func (mt *MerkleTree) WriteTree(filename string) {
	_, _ = os.Create(filename)
	file2,_ := os.OpenFile(filename, os.O_APPEND, 0600)
	writeNode(mt.root.root, 0,file2)
	err := file2.Close()
	if err != nil {
		return
	}
}

func writeNode(node *Node, level int, file *os.File) {
	_, err2 := fmt.Fprintf(file,"(%d) %s %s\n", level, strings.Repeat(" ", level), node.String())
	if err2 != nil {
		return 
	}
	if node.left != nil  {
		writeNode(node.left, level+1, file)
	}
	if node.right != nil  {
		writeNode(node.right, level+1, file)
	} 
}


func main(){
	fmt.Println()
}
