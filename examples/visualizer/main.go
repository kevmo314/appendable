package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/kevmo314/appendable/pkg/bptree"
	"github.com/kevmo314/appendable/pkg/handlers"
	"github.com/kevmo314/appendable/pkg/mmap"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"golang.org/x/sys/unix"
)

func main() {
	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer f.Close()

	df, err := os.Open(os.Args[2])
	if err != nil {
		panic(err)
	}
	defer df.Close()

	mmdf, err := mmap.NewMemoryMappedFile(df, unix.PROT_READ)
	if err != nil {
		panic(err)
	}

	// create a new pagefile
	pf, err := pagefile.NewPageFile(f)
	if err != nil {
		panic(err)
	}

	lmps := []int64{4096} // store a list of the linked meta pages.
	fps := []int64{}      // store a list of the free pages.

	fmt.Printf("<!doctype html>\n<html>\n<head>\n<title>Appendable Visualizer</title>\n</head>\n<body>\n<ol>\n")

	// read the free page index
	fmt.Printf("<li id='#%d'><details><summary>Free Page Index</summary>", 0)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}
	buf := make([]byte, pf.PageSize())
	if _, err := f.Read(buf); err != nil {
		panic(err)
	}
	for j := 0; j < pf.PageSize()/8; j++ {
		val := binary.LittleEndian.Uint64(buf[j*8 : j*8+8])
		if val == 0 {
			break
		}
		fmt.Printf("<p>%d</p>", val)
		fps = append(fps, int64(val))
	}
	fmt.Printf("</details></li>")

	slices.Sort(fps)

	for i := int64(0); i < pf.PageCount(); i++ {
		offset, err := pf.Page(int(i))
		if err != nil {
			panic(err)
		}
		// read the page
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			panic(err)
		}
		if len(fps) > 0 && i == fps[0] {
			// this is a free page
			fps = fps[1:]
			fmt.Printf("<li id='#%d'><details><summary>Free Page</summary>", offset)
			fmt.Printf("</details></li>")
		} else if len(lmps) > 0 && offset == lmps[0] {
			// this is a linked meta page
			lmps = lmps[1:]

			// metaPage, err := metapage.NewMultiBPTree(pf, int(i))
			// if err != nil {
			// 	panic(err)
			// }
			fmt.Printf("<li id='#%d'><summary>Linked Meta Page (TODO)</summary></li>", offset)

			// root, err := metaPage.Root()
			// if err != nil {
			// 	panic(err)
			// }
			// next, err := metaPage.Next()
			// if err != nil {
			// 	panic(err)
			// }
			// exists, err := next.Exists()
			// if err != nil {
			// 	panic(err)
			// }
			// if exists {
			// 	fmt.Printf("<p><a href='#%d'>Root (%x)</a> - <a href='#%d'>Next (%x)</a></p>", root.Offset, root.Offset, next.MemoryPointer().Offset, next.MemoryPointer().Offset)
			// 	lmps = append(lmps, int64(next.MemoryPointer().Offset))
			// } else {
			// 	fmt.Printf("<p><a href='#%d'>Root (%x)</a> - <span>Next (nil)</span></p>", root.Offset, root.Offset)
			// }
			// fmt.Printf("<p>Metadata</p>")
			// md, err := metaPage.Metadata()
			// if err != nil {
			// 	panic(err)
			// }
			// fmt.Printf("<pre>%x</pre>", md)
			// fmt.Printf("</details></li>")
		} else {
			// try to read the page as a bptree node
			node := &bptree.BPTreeNode{}
			node.Data = mmdf.Bytes()
			node.DataParser = &handlers.JSONLHandler{}

			if _, err := f.Seek(offset, io.SeekStart); err != nil {
				panic(err)
			}
			buf := make([]byte, pf.PageSize())
			if _, err := f.Read(buf); err != nil {
				panic(err)
			}
			if err := node.UnmarshalBinary(buf); err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}

			if node.Leaf() {
				fmt.Printf("<li id='#%d'><details><summary>B+ Tree Leaf Node</summary>", offset)
			} else {
				fmt.Printf("<li id='#%d'><details><summary>B+ Tree Node</summary>", offset)
			}
			fmt.Printf("<p>Keys</p>")
			for _, k := range node.Keys {
				fmt.Printf("<pre>%x</pre>", k.Value)
			}
			fmt.Printf("<p>Pointers</p>")
			for j := 0; j < node.NumPointers(); j++ {
				if node.Leaf() {
					fmt.Printf("<p>[%x:%x]</p>", node.Pointer(j).Offset, node.Pointer(j).Offset+uint64(node.Pointer(j).Length))
				} else {
					fmt.Printf("<p><a href='#%d'>%x</a></p>", node.Pointer(j).Offset, node.Pointer(j).Offset)
				}
			}
			fmt.Printf("</details></li>")
		}
	}
	fmt.Printf("</ol>\n</body>\n</html>\n")

}
