package merkledag

import (
	"hash"
)
const (
	K := 1 << 10
	BLOCK_SIZE = 256 * K
)
type Link struct {
	Name string
	Hash []byte
	Size int
}
type Object struct {
	Links []Link
	Data  []byte
}
func Add(store KVStore, node Node, h hash.Hash) []byte {
	switch node.Type() {
	case FILE:
		return StoreFile(store, node.(File), h)
	case DIR:
		return StoreDir(store, node.(Dir), h)
	default:
		return nil
	}
}
func StoreFile(store KVStore, node File, h hash.Hash) []byte {
	t := []byte("blob")
	if node.Size() > BLOCK_SIZE {
		t = []byte("list")
	}
	data := node.Bytes()
	chunks := chunkData(data)
	var merkleRoot []byte
	for _, chunk := range chunks {
		hashValue := hashData(chunk, h)
		if err := store.Put(hashValue, chunk); err != nil {
			return nil
		}
		merkleRoot = append(merkleRoot, hashValue...)
	}
	return merkleRoot
}
func StoreDir(store KVStore, dir Dir, h hash.Hash) []byte {
	obj := Object{}

	it := dir.It()
	for it.Next() {
		node := it.Node()
		switch node.Type() {
		case FILE:
			file := node.(File)
			data := file.Bytes()
			hashValue := hashData(data, h)
			if err := store.Put(hashValue, data); err != nil {
				return nil
			}
			obj.Links = append(obj.Links, Link{Name: "file", Hash: hashValue, Size: int(file.Size())})
		case DIR:
			dir := node.(Dir)
			childMerkleRoot := Add(store, dir, h)
			obj.Links = append(obj.Links, Link{Name: "dir", Hash: childMerkleRoot, Size: 0})
		}
	}

	
	var serializedObj []byte
	
	serializedObj = serializeObject(obj)

	objHash := hashData(serializedObj, h)
	if err := store.Put(objHash, serializedObj); err != nil {
		return nil
	}

	return objHash
}

func chunkData(data []byte) [][]byte {
	var chunks [][]byte
	const chunkSize = BLOCK_SIZE
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunks = append(chunks, data[i:end])
	}
	return chunks
}

func hashData(data []byte, h hash.Hash) []byte {
	h.Reset()
	h.Write(data)
	return h.Sum(nil)
}

func serializeObject(obj Object) []byte {
	
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(obj)
	if err != nil {
		
		return nil
	}
	return buf.Bytes()
}