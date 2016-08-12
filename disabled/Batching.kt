package com.mongodb.impossible.do_it

import org.bson.Document
import com.mongodb.client.model.Filters.*

class Batching : Scenario {
    val collection = Constants.DB.getCollection("batched")

    override fun name(): String {
        return "Batching"
    }

    override fun initial(check: Boolean): Long {
        val start = System.currentTimeMillis()
        // count
        // let's cheat and suppose we've pre-aggregated the document count
        Constants.DB.getCollection("batched.count").find().limit(1).first()
        collection.aggregate(listOf(
                Document().append("\$unwind", "\$docs"),
                Document().append("\$sort", "\$docs.serialno"),
                Document().append("\$limit", Constants.PAGESIZE)
        ))
        return System.currentTimeMillis() - start
    }

    override fun initialSkip(check: Boolean): Long {
        val start = System.currentTimeMillis()
        // count
        // let's cheat and suppose we've pre-aggregated the document count
        Constants.DB.getCollection("batched.count").find().limit(1).first()
        collection.aggregate(listOf(
                Document().append("\$unwind", "\$docs"),
                Document().append("\$sort", "\$docs.serialno"),
                Document().append("\$skip", Constants.PAGESIZE*Constants.SKIPTOPAGE),
                Document().append("\$limit", Constants.PAGESIZE)
        ))
        return System.currentTimeMillis() - start
    }

    override fun filter(check: Boolean): Long {
        val start = System.currentTimeMillis()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$group", Document().
                        append("_id", null).
                        append("count", Document().append("\$sum", Document().append("\$size", "\$docs") ) )
                )
        )).first()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$unwind", "\$docs"),
                Document().append("\$sort", Document().append("docs.serialno", 1) ),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()

        return System.currentTimeMillis() - start
    }

    override fun filterSkip(check: Boolean): Long {
        val start = System.currentTimeMillis()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                      append("docs.HINCP", 8040).
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$group", Document().
                        append("_id", null).
                        append("count", Document().append("\$sum", Document().append("\$size", "\$docs") ) )
                )
        )).first()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                        append("docs.HINCP", 8040).
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$unwind", "\$docs"),
                Document().append("\$sort", Document().append("docs.serialno", 1) ),
                Document().append("\$skip", Constants.PAGESIZE*Constants.SKIPTOPAGE),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()

        return System.currentTimeMillis() - start
    }

    override fun filterAndSort(check: Boolean): Long {
        val start = System.currentTimeMillis()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$group", Document().
                        append("_id", null).
                        append("count", Document().append("\$sum", Document().append("\$size", "\$docs") ) )
                )
        )).first()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$unwind", "\$docs"),
                Document().append("\$sort", Document().append("docs.HINCP", 1) ),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()

        return System.currentTimeMillis() - start
    }

    override fun filterAndSortSkip(check: Boolean): Long {

        val start = System.currentTimeMillis()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$group", Document().
                        append("_id", null).
                        append("count", Document().append("\$sum", Document().append("\$size", "\$docs") ) )
                )
        )).first()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$unwind", "\$docs"),
                Document().append("\$sort", Document().append("docs.HINCP", 1) ),
                Document().append("\$skip", Constants.PAGESIZE*Constants.SKIPTOPAGE ),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()

        return System.currentTimeMillis() - start
    }

    override fun filterAndSort2(check: Boolean): Long {
        val start = System.currentTimeMillis()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$group", Document().
                        append("_id", null).
                        append("count", Document().append("\$sum", Document().append("\$size", "\$docs") ) )
                )
        )).first()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$unwind", "\$docs"),
                Document().append("\$sort", Document().append("docs.ELEP", 1) ),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()

        return System.currentTimeMillis() - start
    }

    override fun filterAndSort2Skip(check: Boolean): Long {
        val start = System.currentTimeMillis()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$group", Document().
                        append("_id", null).
                        append("count", Document().append("\$sum", Document().append("\$size", "\$docs") ) )
                )
        )).first()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$unwind", "\$docs"),
                Document().append("\$sort", Document().append("docs.ELEP", 1) ),
                Document().append("\$skip", Constants.PAGESIZE*Constants.SKIPTOPAGE ),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()

        return System.currentTimeMillis() - start
    }

    override fun rangeOnly(check: Boolean): Long {

        val start = System.currentTimeMillis()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$group", Document().
                        append("_id", null).
                        append("count", Document().append("\$sum", Document().append("\$size", "\$docs") ) )
                )
        )).first()

        collection.aggregate(listOf(
                Document().append("\$match", Document().
                        append("ELEP", Document("\$gt", 100)).
                        append("FULP", Document("\$lt", 1000))),
                Document().append("\$unwind", "\$docs"),
                Document().append("\$sort", Document().append("docs.serialno", 1) ),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()

        return System.currentTimeMillis() - start
    }

    override fun equalityOnly(check: Boolean): Long {

        val start = System.currentTimeMillis()


        val match = Document().append("\$match", Document().
                append("docs", Document().
                        append("\$elemMatch", Document().append("BATH", 2).append("NP", 3))
                ))
        val project = Document().append("\$project", Document().
                append("DIVISION", 1).
                append("PUMA", 1).
                append("ELEP", 1).
                append("FULP", 1).
                append("docs", Document().
                        append("\$filter", Document()
                                .append("input", "\$docs")
                                .append("as", "doc")
                                .append("cond", Document()
                                        .append("\$and", listOf(
                                                Document().append("\$eq", listOf("\$\$doc.BATH", 2)),
                                                Document().append("\$eq", listOf("\$\$doc.NP", 3))
                                        ))
                                )
                        )
                ))
        collection.aggregate(listOf(
                match,
                project,
                Document().append("\$group", Document().
                        append("_id", null).
                        append("count", Document().append("\$sum", Document().append("\$size", "\$docs") ) )
                )
        )).first()

        collection.aggregate(listOf(
                match,
                project,
                Document().append("\$unwind", "\$docs"),
                Document().append("\$sort", Document().append("docs.serialno", 1) ),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()

        return System.currentTimeMillis() - start
    }
}