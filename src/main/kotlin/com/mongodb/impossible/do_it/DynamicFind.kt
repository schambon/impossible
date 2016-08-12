package com.mongodb.impossible.do_it

import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.Sorts.ascending
import org.bson.Document
import java.util.*

/**
 * Created by sylvainchambon on 08/08/16.
 */
class DynamicFind : Scenario {

    val collection = Constants.DB.getCollection("dynamic")



    override fun name(): String {
        return "Dynamic find"
    }

    override fun initial(check: Boolean): Pair<Long, Long> {
        val cursor = { collection.find(eq("props.key","serialno")) }
        val start = System.currentTimeMillis()
        collection.count()
        val counted = System.currentTimeMillis()
        var list = cursor().sort(ascending("props.value")).limit(Constants.PAGESIZE).toList()

        val result = System.currentTimeMillis() - start

        if (check) {
            val serialnumbers = list.map { doc -> (doc.get("props", ArrayList::class.java).toList().filter { prop -> (prop as Document).getString("key") == "serialno" }.first() as Document).getLong("value")  }
            assert(Constants.isSorted(serialnumbers))
        }

        return Pair(result, counted-start)
    }

    override fun initialSkip(check: Boolean): Pair<Long, Long> {
        val cursor = { collection.find(eq("props.key","serialno")) }
        val start = System.currentTimeMillis()
        collection.count()
        val counted = System.currentTimeMillis()
        val list = cursor().sort(ascending("props.value")).skip(Constants.PAGESIZE * Constants.SKIPTOPAGE).limit(Constants.PAGESIZE).toList()

        if (check) {
            val serialnumbers = list.map { doc -> (doc.get("props", ArrayList::class.java).toList().filter { prop -> (prop as Document).getString("key") == "serialno" }.first() as Document).getLong("value")  }
            assert(Constants.isSorted(serialnumbers))
        }

        return Pair(System.currentTimeMillis() - start, counted-start)
    }



    override fun filter(check: Boolean): Pair<Long,Long> {

        return internalFilterSort("serialno", 0, check)

    }

    override fun filterSkip(check: Boolean): Pair<Long,Long> {
        return internalFilterSort("serialno", Constants.PAGESIZE, check)
    }

    fun internalFilterSort(sort:String, skip:Int, check: Boolean): Pair<Long, Long>{
//        val query = and(all("props",
//                CustomFilters.elemMatch(eq("key", "DIVISION"), eq("value", 1)),
//                CustomFilters.elemMatch(eq("key", "ELEP"), gt("value", 100)),
//                CustomFilters.elemMatch(eq("key", "FULP"), lt("value", 1000))),
//                eq("props.key", sort))

        val query = Document().append("props", Document().
                append("\$all", listOf(
                        Document().append("\$elemMatch", Document().append("key", "DIVISION").append("value", 1)),
                        Document().append("\$elemMatch", Document().append("key", "ELEP").append("value", Document().append("\$gt", 100))),
                        Document().append("\$elemMatch", Document().append("key", "FULP").append("value", Document().append("\$lt", 1000)))
                ))
        ).append("props.key", "serialno")

        val start = System.currentTimeMillis()
        collection.count(query)
        val counted = System.currentTimeMillis()
//        print("${counted-start}ms. ")
        val list = collection.find(query).sort(ascending("props.value")).limit(Constants.PAGESIZE).skip(skip*Constants.PAGESIZE).toList()
        val result = Pair(System.currentTimeMillis() - start, counted-start)

        if (check) {
//            assert(count == Constants.PAGESIZE)
            val values = list.map { doc ->
                    val r: Long
                    var v = (doc.get("props", ArrayList::class.java).toList().filter { prop -> (prop as Document).getString("key") == sort }.first() as Document).get("value")
                    if (v is Int) {
                        r = v.toLong()
                    } else if (v is Long) {
                        r = v
                    } else {
                        r = 0L
                    }
                    r
             }
            assert(Constants.isSorted(values))
        }
        return result
    }

    override fun filterAndSort(check: Boolean): Pair<Long, Long> {
        return internalFilterSort("HINCP", 0, check)
    }

    override fun filterAndSortSkip(check: Boolean): Pair<Long, Long>{
        return internalFilterSort("HINCP", Constants.SKIPTOPAGE, check)
    }

    override fun filterAndSort2(check: Boolean): Pair<Long, Long> {
        return internalFilterSort("ELEP", 0, check)
    }

    override fun filterAndSort2Skip(check: Boolean): Pair<Long, Long> {
        return internalFilterSort("ELEP", Constants.SKIPTOPAGE, check)
    }

    override fun rangeOnly(check: Boolean): Pair<Long, Long> {

        val query = Document().append("props", Document().
                append("\$all", listOf(
                        Document().append("\$elemMatch", Document().append("key", "ELEP").append("value", Document().append("\$gt", 100))),
                        Document().append("\$elemMatch", Document().append("key", "FULP").append("value", Document().append("\$lt", 1000)))
                ))
        ).append("props.key", "serialno")


        val start = System.currentTimeMillis()
        collection.count(query)
        val counted = System.currentTimeMillis()
        collection.find(query).sort(ascending("props.value")).limit(Constants.PAGESIZE).toList()
        return Pair(System.currentTimeMillis() - start, counted-start)


    }

    override fun equalityOnly(check: Boolean): Pair<Long, Long> {
        val query = Document().append("props", Document().
                append("\$all", listOf(
                        Document().append("\$elemMatch", Document().append("key", "BATH").append("value", 2)),
                        Document().append("\$elemMatch", Document().append("key", "NP").append("value", 3))
                ))
        ).append("props.key", "serialno")

        val start = System.currentTimeMillis()
        collection.count(query)
        val counted = System.currentTimeMillis()
        collection.find(query).sort(ascending("props.value")).limit(Constants.PAGESIZE).toList()
        return Pair(System.currentTimeMillis() - start, counted-start)
    }
}
