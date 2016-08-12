package com.mongodb.impossible.do_it

/**
 * Created by sylvainchambon on 09/08/16.
 */

val SAMPLESIZE=2
val WARMUP=true

fun test(name: String, subject: (check: Boolean) -> Pair<Long,Long>): Unit {
    if(WARMUP) {
        try {
            subject(true)
        } catch (e: UnsupportedOperationException) {
//            println("$name not implemented")
            return
        }
    }
    val times = Array<Long>(size = SAMPLESIZE, init = { 0L })
    val countTimes = Array<Long>(size = SAMPLESIZE, init = {0L})
    var ct = 1;
    for (j in 0..SAMPLESIZE-1) {
        try {
            val t = subject(false)
            ct = j + 1
            times[j] = t.first
            countTimes[j] = t.second
            if (t.first > 2000) {
                break
            }
        } catch(e: UnsupportedOperationException) {
            return
        }
    }
    val mean = times.sum().toDouble() / ct.toDouble()
    val countMean = countTimes.sum().toDouble() / ct.toDouble()
//    val stdDev = Math.sqrt(times.filter {t -> t != 0L}.map { t -> (t-mean)*(t-mean)}.sum() / ct)

    println("${name}\t${mean}\t${countMean}\t${times.toList()}\t${countTimes.toList()}")
}

fun runTests(sc: Scenario): Unit {
    test("${sc.name()}\t01 initial", {check -> sc.initial(check) })
    test("${sc.name()}\t02 initial, skip", {check -> sc.initialSkip(check)})
    test("${sc.name()}\t03 filter", {check -> sc.filter(check)})
    test("${sc.name()}\t04 filter, skip", {check -> sc.filterSkip(check)})
    test("${sc.name()}\t05 filter, sort", {check -> sc.filterAndSort(check)})
    test("${sc.name()}\t06 filter, sort, skip", {check -> sc.filterAndSortSkip(check)})
    test("${sc.name()}\t07 filter, sort 2", {check -> sc.filterAndSort2Skip(check)})
    test("${sc.name()}\t08 filter, sort 2, skip", {check -> sc.filterAndSort2Skip(check)})
    test("${sc.name()}\t09 range only", {check -> sc.rangeOnly(check)})
    test("${sc.name()}\t10 equality only", {check -> sc.equalityOnly(check)})
}

fun main(args: Array<String>) {
    println("Case\tTest\tmean\tcountMean\tvalues\tcounttimes")
    runTests(IxScanAgg())
    runTests(CollScanFind())
    runTests(CollScanAgg())
    runTests(DynamicFind())
    runTests(SortFirst())

//    runTests(Batching())
}
