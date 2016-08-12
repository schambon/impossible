package com.mongodb.impossible.do_it

import com.mongodb.MongoClient
import org.bson.Document

/**
 * Created by sylvainchambon on 09/08/16.
 */
class IxScanAgg : Scenario {
    val collection = Constants.DB.getCollection("survey")

    override fun name(): String {
        return "IxScanAgg"
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

    fun test(count: () -> Unit, page: () -> Unit): Pair<Long, Long> {
        val start = System.currentTimeMillis()
        count()
        val counted = System.currentTimeMillis()
        page()
        return Pair(System.currentTimeMillis() - start, counted - start)
    }

    override fun initial(check: Boolean): Pair<Long,Long> {
        return test(
                {collection.count()},
                {collection.aggregate(listOf(
                Document().append("\$sort", Document().append("serialno", 1)),
                Document().append("\$project", project()),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()})
    }

    override fun initialSkip(check: Boolean): Pair<Long,Long> {
        return test({
            collection.count()},
                {

        collection.aggregate(listOf(
                Document().append("\$sort", Document().append("serialno", 1)),
                Document().append("\$project", project()),
                Document().append("\$skip", Constants.PAGESIZE*Constants.SKIPTOPAGE),
                Document().append("\$limit", Constants.PAGESIZE)
        )).toList()
                }
                )

    }

    override fun filter(check: Boolean): Pair<Long,Long> {
        return test(
                {collection.aggregate(listOf(
                        Document().append("\$match", Document().
                                append("DIVISION", 1).
                                append("ELEP", Document().append("\$gt", 100)).
                                append("FULP", Document().append("\$lt", 1000))
                        ),
                        Document().append("\$project", project()),
                        Document().append("\$group", Document().append("_id", null).append("count", Document().append("\$sum", 1)))
                )).first()},
                {val pipeline = listOf(
                        Document().append("\$match", Document().
                                append("DIVISION", 1).
                                append("ELEP", Document().append("\$gt", 100)).
                                append("FULP", Document().append("\$lt", 1000))
                        ),
                        Document().append("\$project", project()),
                        Document().append("\$sort", Document().append("serialno", 1)),
                        Document().append("\$limit", Constants.PAGESIZE)
                )
                    val list = collection.aggregate(pipeline).toList()

                    assert(list.size == Constants.PAGESIZE)}
        )
    }

    override fun filterSkip(check: Boolean): Pair<Long,Long> {
        return test({
            collection.aggregate(listOf(
                    Document().append("\$match", Document().
                            append("DIVISION", 1).
                            append("ELEP", Document().append("\$gt", 100)).
                            append("FULP", Document().append("\$lt", 1000))
                    ),
                    Document().append("\$project", project()),
                    Document().append("\$group", Document().append("_id", null).append("count",  Document().append("\$sum", 1)))
            )).first()
        },
                {
                    collection.aggregate(listOf(
                            Document().append("\$match", Document().
                                    append("DIVISION", 1).
                                    append("ELEP", Document().append("\$gt", 100)).
                                    append("FULP", Document().append("\$lt", 1000))
                            ),
                            Document().append("\$project", project()),
                            Document().append("\$sort", Document().append("serialno", 1)),
                            Document().append("\$skip", Constants.PAGESIZE*Constants.SKIPTOPAGE),
                            Document().append("\$limit", Constants.PAGESIZE)
                    )).toList()
                })
    }

    override fun filterAndSort(check: Boolean): Pair<Long,Long> {
        return test(
                {
                    collection.aggregate(listOf(
                            Document().append("\$match", Document().
                                    append("DIVISION", 1).
                                    append("ELEP", Document().append("\$gt", 100)).
                                    append("FULP", Document().append("\$lt", 1000))
                            ),
                            Document().append("\$project", project()),
                            Document().append("\$group", Document().append("_id", null).append("count",  Document().append("\$sum", 1)))
                    )).first()
                },
                {
                    collection.aggregate(listOf(
                            Document().append("\$match", Document().
                                    append("DIVISION", 1).
                                    append("ELEP", Document().append("\$gt", 100)).
                                    append("FULP", Document().append("\$lt", 1000))
                            ),
                            Document().append("\$project", project()),
                            Document().append("\$sort", Document().append("HINCP", 1)),
                            Document().append("\$limit", Constants.PAGESIZE)
                    )).toList()
                }
        )

    }

    override fun filterAndSortSkip(check: Boolean): Pair<Long,Long> {
        return test(
                {
                    collection.aggregate(listOf(
                            Document().append("\$match", Document().
                                    append("DIVISION", 1).
                                    append("ELEP", Document().append("\$gt", 100)).
                                    append("FULP", Document().append("\$lt", 1000))
                            ),
                            Document().append("\$project", project()),
                            Document().append("\$group", Document().append("_id", null).append("count",  Document().append("\$sum", 1)))
                    )).first()
                },
                {
                    collection.aggregate(listOf(
                            Document().append("\$match", Document().
                                    append("DIVISION", 1).
                                    append("ELEP", Document().append("\$gt", 100)).
                                    append("FULP", Document().append("\$lt", 1000))
                            ),
                            Document().append("\$project", project()),
                            Document().append("\$sort", Document().append("HINCP", 1)),
                            Document().append("\$skip", Constants.PAGESIZE*Constants.SKIPTOPAGE),
                            Document().append("\$limit", Constants.PAGESIZE)
                    )).toList()
                }
        )
    }

    override fun filterAndSort2(check: Boolean): Pair<Long, Long> {
        return test(
                {
                    collection.aggregate(listOf(
                            Document().append("\$match", Document().
                                    append("DIVISION", 1).
                                    append("ELEP", Document().append("\$gt", 100)).
                                    append("FULP", Document().append("\$lt", 1000))
                            ),
                            Document().append("\$project", project()),
                            Document().append("\$group", Document().append("_id", null).append("count",  Document().append("\$sum", 1)))
                    )).first()
                },
                {
                    collection.aggregate(listOf(
                            Document().append("\$match", Document().
                                    append("DIVISION", 1).
                                    append("ELEP", Document().append("\$gt", 100)).
                                    append("FULP", Document().append("\$lt", 1000))
                            ),
                            Document().append("\$project", project()),
                            Document().append("\$sort", Document().append("ELEP", 1)),
                            Document().append("\$limit", Constants.PAGESIZE)
                    )).toList()
                }
        )
    }

    override fun filterAndSort2Skip(check: Boolean): Pair<Long,Long> {
        return test(
                {
                    collection.aggregate(listOf(
                            Document().append("\$match", Document().
                                    append("DIVISION", 1).
                                    append("ELEP", Document().append("\$gt", 100)).
                                    append("FULP", Document().append("\$lt", 1000))
                            ),
                            Document().append("\$project", project()),
                            Document().append("\$group", Document().append("_id", null).append("count",  Document().append("\$sum", 1)))
                    )).first()
                },
                {
                    collection.aggregate(listOf(
                            Document().append("\$match", Document().
                                    append("DIVISION", 1).
                                    append("ELEP", Document().append("\$gt", 100)).
                                    append("FULP", Document().append("\$lt", 1000))
                            ),
                            Document().append("\$project", project()),
                            Document().append("\$sort", Document().append("ELEP", 1)),
                            Document().append("\$skip", Constants.PAGESIZE*Constants.SKIPTOPAGE),
                            Document().append("\$limit", Constants.PAGESIZE)
                    )).toList()
                }
        )
    }

    override fun rangeOnly(check: Boolean): Pair<Long, Long> {
        return test(
                {
                    collection.aggregate(listOf(
                            Document().append("\$match", Document().
                                    append("ELEP", Document().append("\$gt", 100)).
                                    append("FULP", Document().append("\$lt", 1000))
                            ),
                            Document().append("\$project", project()),
                            Document().append("\$group", Document().append("_id", null).append("count", Document().append("\$sum", 1)))
                    )).first()
                },
                {
                    val pipeline = listOf(
                            Document().append("\$match", Document().
                                    append("ELEP", Document().append("\$gt", 100)).
                                    append("FULP", Document().append("\$lt", 1000))
                            ),
                            Document().append("\$project", project()),
                            Document().append("\$sort", Document().append("serialno", 1)),
                            Document().append("\$limit", Constants.PAGESIZE)
                    )
                    collection.aggregate(pipeline).toList()
                }
        )
    }

    override fun equalityOnly(check: Boolean): Pair<Long, Long> {
        return test(
                {
                    collection.aggregate(listOf(
                            Document().append("\$match", Document().
                                    append("BATH", 2).
                                    append("NP", 3)
                            ),
                            Document().append("\$project", project()),
                            Document().append("\$group", Document().append("_id", null).append("count", Document().append("\$sum", 1)))
                    )).first()
                },
                {
                    val pipeline = listOf(
                            Document().append("\$match", Document().
                                    append("BATH", 2).
                                    append("NP", 3)
                            ),
                            Document().append("\$project", project()),
                            Document().append("\$sort", Document().append("serialno", 1)),
                            Document().append("\$limit", Constants.PAGESIZE)
                    )
                    collection.aggregate(pipeline).toList()
                }
        )

    }
}