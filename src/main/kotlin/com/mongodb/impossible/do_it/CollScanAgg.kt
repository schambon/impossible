package com.mongodb.impossible.do_it

import com.mongodb.MongoClient
import org.bson.Document

/**
 * Created by sylvainchambon on 09/08/16.
 */
class CollScanAgg : Scenario {

    val collection = Constants.DB.getCollection("survey")

    override fun name(): String {
        return "CollScanAgg"
    }

    fun project(): Document {
        return Document().append("_id", 1).
                append("serialno", 1).
                append("DIVISION", 1).
                append("ST", 1).
                append("NP", 1).
                append("BATH", 1).
                append("YBL", 1).
                append("HINCP", 1).
                append("ELEP", 1)
    }

    override fun initial(check: Boolean): Pair<Long,Long> {
        val start = System.currentTimeMillis()
        collection.count()
        val counted = System.currentTimeMillis()
        collection.aggregate(listOf(
                Document().append("\$project", project()),
                Document().append("\$sort", Document().append("serialno", 1)),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()
        return Pair(System.currentTimeMillis() - start, counted-start)
    }

    override fun initialSkip(check: Boolean): Pair<Long, Long> {
        val start = System.currentTimeMillis()
        collection.count()
        val counted = System.currentTimeMillis()
        collection.aggregate(listOf(
                Document().append("\$project", project()),
                Document().append("\$sort", Document().append("serialno", 1)),
                Document().append("\$skip", Constants.PAGESIZE*Constants.SKIPTOPAGE),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()
        return Pair(System.currentTimeMillis()-start,counted-start)
    }

    override fun filter(check: Boolean): Pair<Long, Long> {

        val start = System.currentTimeMillis()
        collection.aggregate(listOf(
                Document().append("\$project", project()),
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document().append("\$gt", 100)).
                        append("FULP", Document().append("\$lt", 1000))
                        ),
                Document().append("\$group", Document().append("_id", null).append("count", Document().append("\$sum", 1)))
        )).first()
        val counted = System.currentTimeMillis()
        val pipeline = listOf(
                Document().append("\$project", project()),
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document().append("\$gt", 100)).
                        append("FULP", Document().append("\$lt", 1000))
                ),
                Document().append("\$sort", Document().append("serialno", 1)),
                Document().append("\$limit", Constants.PAGESIZE)
        )
        val list = collection.aggregate(pipeline).toList()

        if(check) assert(list.size == Constants.PAGESIZE)
        return Pair(System.currentTimeMillis()-start, counted-start)
    }

    override fun filterSkip(check: Boolean): Pair<Long, Long> {

        val start = System.currentTimeMillis()
        collection.aggregate(listOf(
                Document().append("\$project", project()),
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document().append("\$gt", 100)).
                        append("FULP", Document().append("\$lt", 1000))
                        ),
                Document().append("\$group", Document().append("_id", null).append("count",  Document().append("\$sum", 1)))
        )).first()
        val counted = System.currentTimeMillis()
        collection.aggregate(listOf(
                Document().append("\$project", project()),
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document().append("\$gt", 100)).
                        append("FULP", Document().append("\$lt", 1000))
                ),
                Document().append("\$sort", Document().append("serialno", 1)),
                Document().append("\$skip", Constants.PAGESIZE*Constants.SKIPTOPAGE),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()
        return Pair(System.currentTimeMillis()-start, counted-start)
    }

    override fun filterAndSort(check: Boolean): Pair<Long, Long> {
        val start = System.currentTimeMillis()
        collection.aggregate(listOf(
                Document().append("\$project", project()),
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document().append("\$gt", 100)).
                        append("FULP", Document().append("\$lt", 1000))
                ),
                Document().append("\$group", Document().append("_id", null).append("count",  Document().append("\$sum", 1)))
        )).first()
        val counted = System.currentTimeMillis()
        collection.aggregate(listOf(
                Document().append("\$project", project()),
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document().append("\$gt", 100)).
                        append("FULP", Document().append("\$lt", 1000))
                ),
                Document().append("\$sort", Document().append("HINCP", 1)),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()
        return Pair(System.currentTimeMillis()-start,counted-start)
    }

    override fun filterAndSortSkip(check: Boolean): Pair<Long, Long> {
        val start = System.currentTimeMillis()
        collection.aggregate(listOf(
                Document().append("\$project", project()),
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document().append("\$gt", 100)).
                        append("FULP", Document().append("\$lt", 1000))
                ),
                Document().append("\$group", Document().append("_id", null).append("count",  Document().append("\$sum", 1)))
        )).first()
        val counted = System.currentTimeMillis()
        collection.aggregate(listOf(
                Document().append("\$project", project()),
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document().append("\$gt", 100)).
                        append("FULP", Document().append("\$lt", 1000))
                ),
                Document().append("\$sort", Document().append("HINCP", 1)),
                Document().append("\$skip", Constants.PAGESIZE*Constants.SKIPTOPAGE),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()
        return Pair(System.currentTimeMillis()-start, counted -start)
    }

    override fun filterAndSort2(check: Boolean): Pair<Long,Long> {
        val start = System.currentTimeMillis()
        collection.aggregate(listOf(
                Document().append("\$project", project()),
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document().append("\$gt", 100)).
                        append("FULP", Document().append("\$lt", 1000))
                ),
                Document().append("\$group", Document().append("_id", null).append("count",  Document().append("\$sum", 1)))
        )).first()
        val counted = System.currentTimeMillis()
        collection.aggregate(listOf(
                Document().append("\$project", project()),
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document().append("\$gt", 100)).
                        append("FULP", Document().append("\$lt", 1000))
                ),
                Document().append("\$sort", Document().append("ELEP", 1)),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()
        return Pair(System.currentTimeMillis()-start, counted-start)
    }

    override fun filterAndSort2Skip(check: Boolean): Pair<Long,Long> {
        val start = System.currentTimeMillis()
        collection.aggregate(listOf(
                Document().append("\$project", project()),
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document().append("\$gt", 100)).
                        append("FULP", Document().append("\$lt", 1000))
                ),
                Document().append("\$group", Document().append("_id", null).append("count",  Document().append("\$sum", 1)))
        )).first()
        val counted = System.currentTimeMillis()
        collection.aggregate(listOf(
                Document().append("\$project", project()),
                Document().append("\$match", Document().
                        append("DIVISION", 1).
                        append("ELEP", Document().append("\$gt", 100)).
                        append("FULP", Document().append("\$lt", 1000))
                ),
                Document().append("\$sort", Document().append("ELEP", 1)),
                Document().append("\$skip", Constants.PAGESIZE*Constants.SKIPTOPAGE),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()
        return Pair(System.currentTimeMillis()-start,counted-start)
    }
    override fun rangeOnly(check: Boolean): Pair<Long,Long> {
        throw UnsupportedOperationException("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun equalityOnly(check: Boolean): Pair<Long,Long> {
        throw UnsupportedOperationException("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}