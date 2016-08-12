package com.mongodb.impossible.do_it

import com.mongodb.MongoClient

/**
 * Created by sylvainchambon on 08/08/16.
 */
object Constants {

    val PAGESIZE = 50
    val SKIPTOPAGE = 300

    val DB = MongoClient("schambon-test-9.schambon-test.9852.mongodbdns.com").getDatabase("hus")
//    val DB = MongoClient("schambon-microshard-0.schambon-test.9852.mongodbdns.com").getDatabase("hus")

    fun isSorted(l: List<Long>): Boolean {
        if (l.size < 2) {
            return true
        }
        val first = l.first()
        val tail = l.drop(1)
        val second = tail.first()

        return first <= second && isSorted(tail)
    }
}