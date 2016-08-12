package com.mongodb.impossible.do_it

import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.*
import org.bson.Document


class CollScanFind : Scenario {
    val collection = Constants.DB.getCollection("survey")

    override fun name(): String {
        return "Naive"
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
        collection.find().projection(project()).sort(eq("serialno",1)).limit(Constants.PAGESIZE).toList()

        return Pair(System.currentTimeMillis() - start,counted-start)
    }

    override fun initialSkip(check: Boolean): Pair<Long,Long> {
        val start = System.currentTimeMillis()
        collection.count()
        val counted = System.currentTimeMillis()
        collection.find().projection(project()).sort(eq("serialno",1)).limit(Constants.PAGESIZE).skip(Constants.PAGESIZE*Constants.SKIPTOPAGE).toList()
        return Pair(System.currentTimeMillis()-start,counted -start)
    }

    override fun filter(check: Boolean): Pair<Long,Long> {
        val start = System.currentTimeMillis()
        collection.count(and(eq("DIVISION", 1), gt("ELEP", 100), lt("FULP", 1000)))
        val counted = System.currentTimeMillis()
        collection.find(and(eq("DIVISION", 1), gt("ELEP", 100), lt("FULP", 1000))).projection(project()).sort(eq("serialno", 1)).toList()

        return Pair(System.currentTimeMillis()-start, counted-start)
    }

    override fun filterSkip(check: Boolean): Pair<Long,Long> {
        val start = System.currentTimeMillis()
        collection.count(and(eq("DIVISION", 1), gt("ELEP", 100), lt("FULP", 1000)))
        val counted = System.currentTimeMillis()
        collection.find(and(eq("DIVISION", 1), gt("ELEP", 100), lt("FULP", 1000))).projection(project()).skip(Constants.PAGESIZE*Constants.SKIPTOPAGE).limit(Constants.PAGESIZE).sort(eq("serialno", 1)).toList()

        return Pair(System.currentTimeMillis()-start,counted-start)
    }

    override fun filterAndSort(check: Boolean): Pair<Long,Long> {
        val start = System.currentTimeMillis()
        collection.count(and(eq("DIVISION", 1), gt("ELEP", 100), lt("FULP", 1000)))
        val counted = System.currentTimeMillis()
        collection.find(and(eq("DIVISION", 1), gt("ELEP", 100), lt("FULP", 1000))).projection(project()).sort(eq("HINCP", 1)).toList()

        return Pair(System.currentTimeMillis()-start,counted-start)
    }

    override fun filterAndSortSkip(check: Boolean): Pair<Long,Long> {
        val start = System.currentTimeMillis()
        collection.count(and(eq("DIVISION", 1), gt("ELEP", 100), lt("FULP", 1000)))
        val counted = System.currentTimeMillis()
        collection.find(and(eq("DIVISION", 1), gt("ELEP", 100), lt("FULP", 1000))).projection(project()).skip(Constants.PAGESIZE*Constants.SKIPTOPAGE).limit(Constants.PAGESIZE).sort(eq("HINCP", 1)).toList()

        return Pair(System.currentTimeMillis()-start, counted-start)
    }

    override fun filterAndSort2(check: Boolean): Pair<Long,Long> {
        val start = System.currentTimeMillis()
        collection.count(and(eq("DIVISION", 1), gt("ELEP", 100), lt("FULP", 1000)))
        val counted = System.currentTimeMillis()
        collection.find(and(eq("DIVISION", 1), gt("ELEP", 100), lt("FULP", 1000))).projection(project()).sort(eq("ELEP", 1)).toList()

        return Pair(System.currentTimeMillis()-start, counted-start)
    }

    override fun filterAndSort2Skip(check: Boolean): Pair<Long,Long> {
        val start = System.currentTimeMillis()
        collection.count(and(eq("DIVISION", 1), gt("ELEP", 100), lt("FULP", 1000)))
        val counted = System.currentTimeMillis()
        collection.find(and(eq("DIVISION", 1), gt("ELEP", 100), lt("FULP", 1000))).projection(project()).skip(Constants.PAGESIZE*Constants.SKIPTOPAGE).limit(Constants.PAGESIZE).sort(eq("ELEP", 1)).toList()

        return Pair(System.currentTimeMillis()-start,counted-start)
    }

    override fun rangeOnly(check: Boolean): Pair<Long,Long> {

        val start = System.currentTimeMillis()
        collection.count(and(gt("ELEP", 100), lt("FULP", 1000)))
        val counted = System.currentTimeMillis()
        collection.find(and(gt("ELEP", 100), lt("FULP", 1000))).projection(project()).skip(Constants.PAGESIZE*Constants.SKIPTOPAGE).limit(Constants.PAGESIZE).sort(eq("serialno", 1)).toList()

        return Pair(System.currentTimeMillis()-start, counted-start)
    }

    override fun equalityOnly(check: Boolean): Pair<Long,Long> {
        val start = System.currentTimeMillis()
        collection.count(and(eq("BATH",2), eq("NP",3)))
        val counted = System.currentTimeMillis()
        collection.find(and(eq("BATH",2), eq("NP",3))).projection(project()).limit(Constants.PAGESIZE).sort(eq("serialno", 1)).toList()
        return Pair(System.currentTimeMillis() - start, counted-start)
    }
}