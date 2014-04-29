# datomic-rtree

An R-tree based spatial indexer for Datomic.

The intention is for the R-tree to be implemented in Datomic itself.

Currently single entry insertion has been implemented. A database function is provided that producess the correct transactions to build up the tree on entry insertion.

A couple of simple search functions have been implemented but I have not decided how they will work with queries yet.

The implementation is a fairly vanilla implementation from the original Antonin Guttman paper:
[R-trees: a Dynamic Index Structure For Spatial Searching](http://www-db.deis.unibo.it/courses/SI-LS/papers/Gut84.pdf)

There is also an implementation of a parallel bulk-loading scheme using hilbert curves and a scoring mechanism similar to the one found in [Sort-based Parallel Loading of R-trees](http://www.mathematik.uni-marburg.de/~achakeye/publications/big_spatial_2012.pdf).

Now includes schema for supporting insertion of Meridian [Shapes](http://github.com/jsofra/shapes) and [Features](http://github.com/jsofra/features)

## Todo

This is currently only a exploration of the idea, there is a lot more to do.

* Retractions and updates
* Batch insertions
* More search and query support
* Investigate other R-trees; R* tree, R+ tree
* And lots, lots more.

## Usage

Nothing much to show yet but you can see some example code in a scratch namespace [here](https://github.com/jsofra/datomic-rtree/blob/master/examples/mem_tree.clj).

## License

Copyright © 2013 FIXME

Distributed under the Eclipse Public License, the same as Clojure.
