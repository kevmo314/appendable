# kevmo314/appendable/btree

This package implements an on-disk B+ tree, taking some inspiration from
https://github.com/spy16/kiwi/tree/master/index/bptree.

## On the significance of the 4kB page size

The B+ tree is designed to be stored on disk, and as such, it is designed to
take advantage of the 4kB page size of most disks. However, in practice we
don't see a material impact on performance when using alternative sizes. So
why do we choose to use 4kB pages?

In order to garbage collect old B+ tree nodes, we want to have pointers to
freed pages to deallocate them entirely. That is, if we did not use page sizes
and stored nodes contiguously, it would be difficult to garbage collect the exact
number of bytes and we would end up with fragmentation. By using page sizes, we
can simply store a list of freed pages and deallocate them entirely and we can
be sure that the freed page will be sufficient to store the new node.

Therefore, we must choose a page size that is large enough to store a node.
In practice, the choice of 4kB specifically is arbitrary, but it is a nice way
to align with the page size of most disks.
