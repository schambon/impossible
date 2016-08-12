package com.mongodb.impossible.do_it

/**
 * Created by sylvainchambon on 08/08/16.
 */
interface Scenario {

    fun name(): String
    fun initial(check: Boolean): Pair<Long, Long>
    fun initialSkip(check: Boolean): Pair<Long, Long>
    fun filter(check: Boolean): Pair<Long, Long>
    fun filterSkip(check: Boolean): Pair<Long, Long>
    fun filterAndSort(check: Boolean): Pair<Long, Long>
    fun filterAndSortSkip(check: Boolean): Pair<Long, Long>

    fun filterAndSort2(check: Boolean): Pair<Long, Long>
    fun filterAndSort2Skip(check: Boolean): Pair<Long, Long>

    fun rangeOnly(check: Boolean): Pair<Long, Long>
    fun equalityOnly(check: Boolean): Pair<Long, Long>
}