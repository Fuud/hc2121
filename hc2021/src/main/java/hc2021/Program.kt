package hc2021

import java.io.File
import java.util.*
import kotlin.math.max
import kotlin.math.min


enum class Task(val fileName: String) {
    A("a"), B("b"), C("c"), D("d"), E("e"), F("f");

    companion object {
        fun toProcess(): List<Task> {
            return listOf(B, C, D, E, F)
        }
    }
}

data class Data(val D: Int, val F: Int, val streets: List<Street>, val paths: List<List<Int>>) {
    val idToStreet = streets.associateBy { it.id }
    val lengths = paths.mapIndexed { index, list ->
        index to list.stream()
                .skip(1)
                .mapToInt { id -> idToStreet[id]?.L ?: 0 }
                .sum()
    }.toMap()
    val cross: Map<Int, List<Street>> = streets.groupBy { it.to }
    val maxScore = lengths.map { F + D - it.value }.sum()
}

fun main() {
    val tasks = readData()
    val random = Random()
    val scores: MutableMap<Task, Int> = mutableMapOf()
    while (true) {
        for (entry in tasks) {
            val start = System.currentTimeMillis()
            val data = entry.value
            val lengths = data.lengths

            val streetWeight: MutableMap<Int, Int> = mutableMapOf()
            val streetCount: MutableMap<Int, Int> = mutableMapOf()
            data.paths.forEachIndexed { i, it ->
                val bonus = (data.D - lengths[i]!! + data.F)
                it.forEach { id ->
                    streetWeight.compute(id) { _, old -> if (old == null) bonus else old + bonus }
                    streetCount.compute(id) { _, old -> if (old == null) 1 else old + 1 }
                }
            }

            val period = random.nextInt(4) + 4
            val schedule: Map<Int, Schedule> = data.cross.mapValues { (_, streets) ->
                val sum = streets.map { streetWeight.getOrDefault(it.id, 0) }.sum()
                val list = mutableListOf<StreetAndTime>()
                val localPeriod = min(sum / (data.F + data.D), period)
                val vw: Map<Street, Double> = streets.filter { streetWeight.containsKey(it.id) }
                        .map { it to streetWeight[it.id]!! / sum.toDouble() }
                        .toMap()
                if (vw.size == 2) {
                    val iterator = vw.iterator()
                    val first = iterator.next()!!
                    val last = iterator.next()!!
                    if (first.value > 0.42 && first.value < 0.58) {
                        list.add(StreetAndTime(first.key, 1))
                        list.add(StreetAndTime(last.key, 1))
                    } else if (first.value > 0.29 && first.value < 0.71) {
                        if (first.value > last.value) {
                            list.add(StreetAndTime(first.key, 2))
                            list.add(StreetAndTime(last.key, 1))
                        } else {
                            list.add(StreetAndTime(first.key, 1))
                            list.add(StreetAndTime(last.key, 2))
                        }
                    } else if (first.value > 0.20 && first.value < 0.80) {
                        if (first.value > last.value) {
                            list.add(StreetAndTime(first.key, 3))
                            list.add(StreetAndTime(last.key, 1))
                        } else {
                            list.add(StreetAndTime(first.key, 1))
                            list.add(StreetAndTime(last.key, 3))
                        }
                    } else {
                        for (street in streets) {
                            val frequency = streetWeight[street.id] ?: 0
                            if (frequency > 0 && vw[street]!! > 0.001) {
                                val time = frequency * localPeriod / sum
                                list.add(StreetAndTime(street, if (time > 0) time else 1))
                            }
                        }
                    }
                } else {
                    for (street in streets) {
                        val frequency = streetWeight[street.id] ?: 0
                        if (frequency > 0 && vw[street]!! > 0.001) {
                            val time = frequency * localPeriod / sum
                            list.add(StreetAndTime(street, if (time > 0) time else 1))
                        }
                    }
                }
                Schedule(list.shuffled(random))
            }.filter { it.value.isNotEmpty() }

            val task = entry.key

//            val onDemandCrosses = data.cross.filter { (_, streets) ->
//                streets.filter { streetCount.containsKey(it.id) }
//                        .all { (streetCount[it.id]!!) == 1 }
//            }
//            println("${task.fileName} ${onDemandCrosses.size} from ${schedule.size}")

            val cars = Cars(data.paths, schedule, data.idToStreet, data.F, data.D)
            for (tick in 0 until data.D) {
                cars.tick(tick)
            }

            println("$task - $period - ${cars.score}(${cars.score / data.maxScore.toDouble() * 100}%) ${(System.currentTimeMillis() - start)/1000}s")
            if (scores[task] ?: 0 < cars.score) {
                File("${task.fileName}.${cars.score}.txt").printWriter().use { writer ->

                    writer.println(schedule.size)

                    schedule.forEach { (crossId, it) ->
                        writer.println(crossId)
                        val lights = it.lights
                        writer.println(lights.size)
                        lights.forEach {
                            writer.println("${it.street.name} ${it.time}")
                        }
                    }
                }
                scores[task] = cars.score
            }
        }
    }
}

private fun readData(): Map<Task, Data> {
    val tasks = Task.toProcess().map { task ->
        val scanner = Scanner(Street::class.java.classLoader.getResourceAsStream("${task.fileName}.txt"))
        // duration
        val D = scanner.nextInt()
        // intersection number
        val I = scanner.nextInt()
        // street number
        val S = scanner.nextInt()
        // number of cars
        val V = scanner.nextInt()
        // bonus point for car
        val F = scanner.nextInt()

        val streets: List<Street> = (0 until S).map {
            Street(
                    id = it,
                    // begin
                    from = scanner.nextInt(),
                    // end
                    to = scanner.nextInt(),
                    name = scanner.next(),
                    // length
                    L = scanner.nextInt()
            )
        }

        val nameToId = streets.associateBy(keySelector = { it.name }, valueTransform = { it.id })

        val paths: List<List<Int>> = (0 until V).map {
            val P = scanner.nextInt()
            (0 until P).map {
                nameToId[scanner.next()]!!
            }
        }
        val data = Data(D, F, streets, paths)
        val longJourney = data.lengths.filterValues { it > D * 0.8 }.size
        println("$task ${paths.size} cars $D ticks. 0.8=$longJourney, maxScore=${data.maxScore}")
        task to data
    }.toMap()
    return tasks
}

private fun dumblink(cross: Map<Int, List<Street>>) =
        cross.mapValues { (_, streets) ->
            Schedule(streets.map { StreetAndTime(it, 1) })
        }


data class Street(val id: Int, val from: Int, val to: Int, val name: String, val L: Int)

data class StreetAndTime(val street: Street, val time: Int)

data class Schedule(val lights: List<StreetAndTime>) {
    val period = lights.sumBy { it.time }

    fun greenStreet(tick: Int): Street {
        val reminder = tick % period
        var timeCount = 0
        for (strTime in lights) {
            timeCount += strTime.time
            if (timeCount > reminder) {
                return strTime.street
            }
        }
        throw IllegalStateException("Should not reach here")
    }

    fun isNotEmpty() = lights.isNotEmpty()
}

data class Car(val id: Int, val path: List<Street>) {
    var streetIdx: Int = 0
    var t: Int = -100_000
    val street: Street
        get() = path[streetIdx]

    fun atTheEnd(now: Int): Boolean = (streetIdx == path.lastIndex) && now - t + 1 >= path.last().L

    fun x(now: Int): Int = max(street.L - (now - t ), 0)
}

class Cars(paths: List<List<Int>>, val schedules: Map<Int, Schedule>, val streets: Map<Int, Street>, val F: Int, val D: Int) {
    var score: Int = 0
    val junctions: MutableMap<Int, MutableList<Car>> = paths.mapIndexed { id, path ->
        Car(id, path.map { streets[it]!! })
    }.groupByTo(mutableMapOf()) {
        it.street.id
    }

    fun tick(turn: Int) {
        val greenCars: List<Car> = junctions.filter { (id, cars) ->
            if(cars.isEmpty()) {
                return@filter false
            }
            val car = cars.first()
            val onTheLight = car.x(turn) == 0
            if (onTheLight) {
                val street = streets[id]!!
                return@filter schedules[street.to]!!.greenStreet(turn) == street
            }
            return@filter false
        }.map {
            val car = it.value.removeFirst()
            car.streetIdx++
            car.t = turn
            car
        }

        greenCars.forEach { car ->
            junctions.computeIfAbsent(car.street.id) { mutableListOf() }.add(car)
        }

        junctions.forEach { (_, cars) ->
                cars.removeIf{
                    val atTheEnd = it.atTheEnd(turn)
                    if(atTheEnd) {
                        score += F + (D - turn - 1)
                    }
                    atTheEnd
            }
        }
    }
}

