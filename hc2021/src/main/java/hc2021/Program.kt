package hc2021

import java.io.File
import java.io.PrintWriter
import java.util.*
import kotlin.math.max
import kotlin.math.min


enum class Task(val fileName: String, val period: Int) {
    A("a", 2),
    B("b", 3),
    C("c", 4),
    D("d", 2),
    E("e", 12),
    F("f", 9);

    companion object {
        fun toProcess(): List<Task> {
            return listOf(
                    B,
                    C,
                    D,
                    E,
                    F,
            )
        }

        fun toRecord(task: Task): Int {
            return when (task) {
                A -> 2002
                B -> 4_568_273
                C -> 1_304_510
                D -> 2_489_386
                E -> 717_618
                F -> 1_395_298
            }
        }
    }
}

data class Data(val D: Int, val F: Int, val streets: List<Street>, val paths: List<List<Int>>) {
    val idToStreet = streets.associateBy { it.id }

    // длины всех маршрутов
    val lengths = paths.mapIndexed { index, list ->
        index to list.stream()
            .skip(1)
            .mapToInt { id -> idToStreet[id]?.L ?: 0 }
            .sum()
    }.toMap()
    val cross: Map<Int, List<Street>> = streets.groupBy { it.to }

    // все машины поехали, не встретили препятствий. обычно недосягаемое.
    val maxScore = lengths.map { F + D - it.value }.sum()

    // берём всем маршруты с этой улицей и складываем их веса
    val streetWeight: MutableMap<Int, Int> = mutableMapOf()
    val spare: MutableMap<Int, Int> = mutableMapOf()

    val carsWeight: MutableMap<Int, Int> = mutableMapOf()

    // сколько маршрутов проходит через заданную улицу.
    val streetCount: MutableMap<Int, Int> = mutableMapOf()

    // все маршруты подходят к этим перекрёсткам с разных улиц. подмножество всех cross-ов.
    val onDemandCrosses: Map<Int, List<Street>>

    // первые перекрёстки в маршрутах
    val firstCrosses: MutableMap<Int, MutableSet<Int>> = mutableMapOf()

    val firstCars: MutableMap<Int, MutableList<Int>> = mutableMapOf()

    init {
        paths.forEachIndexed { i, it ->
            val bonus = (D - lengths[i]!! + F)
            carsWeight[i] = bonus
            it.forEach { id ->
                streetWeight.compute(id) { _, old -> if (old == null) bonus else old + bonus }
                spare.compute(id) { _, old -> if (old == null) bonus else old + bonus }
                streetCount.compute(id) { _, old -> if (old == null) 1 else old + 1 }
            }
            val firstStreetId = it[0]
            val crossId = idToStreet[firstStreetId]!!.to
            firstCrosses.computeIfAbsent(crossId) { mutableSetOf() }.add(firstStreetId)
            firstCars.computeIfAbsent(firstStreetId) { mutableListOf() }.add(i)
        }
        this.onDemandCrosses = cross.filter { (_, streets) ->
            streets.filter { streetCount.containsKey(it.id) }
                .all { (streetCount[it.id]!!) == 1 }
        }
    }
}

fun main() {
    val tasks = readData()
    val random = Random()
    val scores: MutableMap<Task, Int> = mutableMapOf()
    tasks.forEach { t, u -> scores[t] = Task.toRecord(t) }
    println("scores = ${scores}")

    var count = 0
    var newRecords = 0

    while (true) {
        for (entry in tasks) {
            val start = System.currentTimeMillis()
            val data = entry.value
            data.streetWeight.putAll(data.spare)
            val task = entry.key
            val period = random.nextInt(3) - 1 + task.period
            val schedule: Map<Int, Schedule> = data.cross.mapValues { (cross, streets) ->
                if (data.onDemandCrosses.containsKey(cross)) {
                    return@mapValues ScheduleOnDemand(streets.first(), data)
                }
                return@mapValues createSchedule(streets, data, period, random)
            }.filter { it.value.isNotEmpty() }

            val cars = Cars(data, schedule)
            for (tick in 0 until data.D) {
                cars.tick(tick)
            }

            if (scores[task] ?: 0 < cars.score) {
                newRecords++
                println(" + $task - $period - ${cars.score}(${cars.score / data.maxScore.toDouble() * 100}%) ${(System.currentTimeMillis() - start) / 1000}s")
                File("${task.fileName}.${cars.score}.txt").printWriter().use { writer ->

                    writer.println(schedule.size)

                    schedule.forEach { (crossId, it) ->
                        writer.println(crossId)
                        it.write(writer)
                    }
                }
                scores[task] = cars.score
            } else {
                val diffStr = String.format("%,d", ((scores[task] ?: 0) - cars.score))
                val carsScoreStr = String.format("%,d", cars.score)
                println(
                    "$task - $period - ${carsScoreStr} (${cars.score / data.maxScore.toDouble() * 100}%) ${(System.currentTimeMillis() - start) / 1000}s, " +
                            " count = $count new = $newRecords diff = $diffStr"
                )
            }
        }
        count++
    }
}

private fun createSchedule(
    streets: List<Street>,
    data: Data,
    period: Int,
    random: Random
): Schedule {
    val sum = streets.map { data.streetWeight.getOrDefault(it.id, 0) }.sum()
    val list = mutableListOf<StreetAndTime>()
    val localPeriod = min(sum / (data.F + data.D), period)
    val vw: Map<Street, Double> = streets.filter { data.streetWeight.containsKey(it.id) }
        .map { it to data.streetWeight[it.id]!! / sum.toDouble() }
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
                val frequency = data.streetWeight[street.id] ?: 0
                if (frequency > 0 && vw[street]!! > 0.00001 && sum!=0) {
                    val time = frequency * localPeriod / sum
                    list.add(StreetAndTime(street, if (time > 0) time else 1))
                }
            }
        }
    } else {
        for (street in streets) {
            val frequency = data.streetWeight[street.id] ?: 0
            if (frequency > 0 && vw[street]!! > 0.00001 && sum!=0) {
                val time = frequency * localPeriod / sum
                list.add(StreetAndTime(street, if (time > 0) time else 1))
            }
        }
    }
    val shuffled = list.shuffled(random)
//    if (list.isNotEmpty()) {
//        val cross = streets[0].to
//        val set = data.firstCrosses[cross]
//        if (set != null) {
//            return ScheduleImpl(shuffled.sortedWith { k1, k2 ->
//                if (set.contains(k1.street.id) && set.contains(k2.street.id)) {
//                    compareByFirstCars(data, k1, k2)
//                } else if (set.contains(k1.street.id)) {
//                    -1
//                } else if (set.contains(k2.street.id)) {
//                    1
//                } else {
//                    0
//                }
//            })
//        }
//    }

    return SortOnDemandSchedule(shuffled, data)
}

private fun compareByStreet(data: Data, k1: StreetAndTime, k2: StreetAndTime): Int {
    val k1Weight: Int = data.streetWeight.get(k1.street.id)!!
    val k2Weight: Int = data.streetWeight.get(k2.street.id)!!

    return k1Weight.compareTo(k2Weight)
}

private fun compareByFirstCars(data: Data, k1: StreetAndTime, k2: StreetAndTime): Int {
    val k1Weight = data.firstCars[k1.street.id]?.take(k1.time)?.map { data.carsWeight[it]!! }?.sum() ?: 0
    val k2Weight = data.firstCars[k2.street.id]?.take(k2.time)?.map { data.carsWeight[it]!! }?.sum() ?: 0

    return k1Weight.compareTo(k2Weight)
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

        val paths: List<List<Int/*street id*/>> = (0 until V).map {
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
        ScheduleImpl(streets.map { StreetAndTime(it, 1) })
    }


data class Street(val id: Int, val from: Int, val to: Int, val name: String, val L: Int)

data class StreetAndTime(val street: Street, val time: Int)

interface Schedule {
    fun getGreenCar(cars: List<Car>, tick: Int): Car?
    fun isNotEmpty(): Boolean
    fun write(writer: PrintWriter)
}

class ScheduleOnDemand(val defaultStreet: Street, val data: Data) : Schedule {
    val lights: MutableMap<Int, Street> = TreeMap()

    override fun getGreenCar(cars: List<Car>, tick: Int): Car? {
        if (lights.containsKey(tick)) {
            return cars.find { it.street == lights[tick] }
        }
        val car = cars.maxByOrNull { data.streetWeight[it.street.id]!! }
        if (car != null) {
            lights[tick] = car.street
        }
        return car
    }

    override fun isNotEmpty(): Boolean = true

    override fun write(writer: PrintWriter) {
        if (lights.isEmpty()) {
            writer.println("1")
            writer.println("${defaultStreet.name} 1")
        } else {
            writer.println(lights.size)
            var turn = 0
            for ((tick, street) in lights) {
                writer.println("${street.name} ${tick - turn + 1}")
                turn = tick
            }
        }
    }
}

class SortOnDemandSchedule(initial: List<StreetAndTime>, val data: Data) : Schedule {
    val period = initial.sumBy { it.time }
    val result: MutableList<StreetAndTime> = initial.toMutableList()
    val swapByTime: MutableMap<Int, MutableList<StreetAndTime>> = initial.groupByTo(mutableMapOf()) { it.time }
    val swapByStreet: MutableMap<Street, StreetAndTime> = initial.map { it.street to it }.toMap(mutableMapOf())
    var timeToInsert = 0
    var indexToInsert = 0

    override fun getGreenCar(cars: List<Car>, tick: Int): Car? {
        val reminder = tick % period
        if (timeToInsert >= 0) {
            if (reminder < timeToInsert) {
                val greenStreet = greenStreet(tick)
                val theCar = cars.find { it.street == greenStreet.street } ?: return null
                reduceStreetWeight(theCar)
                return theCar
            } else {
                val delta = reminder - timeToInsert
                val nextCar = cars.filter { swapByStreet[it.street]?.time ?: 0 > delta }
                    .maxByOrNull { data.streetWeight[it.street.id]!! + data.streetCount[it.street.id]!! }
                if (nextCar != null) {
                    val streetAndTime = swapByStreet.remove(nextCar.street)!!
                    result.remove(streetAndTime)
                    result.add(indexToInsert, streetAndTime)
                    swapByTime[streetAndTime.time]!!.remove(streetAndTime)
                    timeToInsert += streetAndTime.time
                    indexToInsert++
                    reduceStreetWeight(nextCar)
                    return nextCar
                }
                timeToInsert = -1
            }
        }
        val currentGreenStreet = greenStreet(tick)
        if (swapByStreet.containsKey(currentGreenStreet.street)) {
            val time = swapByStreet[currentGreenStreet.street]!!.time
            val candidates = swapByTime[time]!!.map { it.street to it }.toMap()
            val nextCar = cars.filter { candidates.containsKey(it.street) }
                .maxByOrNull { data.streetWeight[it.street.id]!! + data.streetCount[it.street.id]!! }
            if (nextCar != null) {
                val streetAndTime = swapByStreet.remove(nextCar.street)!!
                swapByTime[streetAndTime.time]!!.remove(streetAndTime)
                val current = result.indexOf(currentGreenStreet)
                val next = result.indexOf(streetAndTime)
                if (current != next) {
                    result[current] = streetAndTime
                    result[next] = currentGreenStreet
                }
                reduceStreetWeight(nextCar)
                return nextCar
            }
            return null
        } else {
            val theCar = cars.find { it.street == currentGreenStreet.street } ?: return null
            reduceStreetWeight(theCar)
            return theCar
        }
    }

    private fun reduceStreetWeight(theCar: Car) {
        val put = data.streetWeight.get(theCar.street.id)!! - data.lengths[theCar.id]!!
        data.streetWeight[theCar.street.id] = put
    }

    override fun write(writer: PrintWriter) {
        writer.println(result.size)
        result.forEach {
            writer.println("${it.street.name} ${it.time}")
        }
    }

    override fun isNotEmpty() = result.isNotEmpty()


    fun greenStreet(tick: Int): StreetAndTime {
        val reminder = tick % period
        var timeCount = 0
        for (strTime in result) {
            timeCount += strTime.time
            if (timeCount > reminder) {
                return strTime
            }
        }
        throw IllegalStateException("Should not reach here")
    }
}

data class ScheduleImpl(val lights: List<StreetAndTime>) : Schedule {
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

    override fun getGreenCar(cars: List<Car>, tick: Int): Car? {
        val greenStreet = greenStreet(tick)
        return cars.find { it.street == greenStreet }
    }

    override fun isNotEmpty() = lights.isNotEmpty()

    override fun write(writer: PrintWriter) {
        writer.println(lights.size)
        lights.forEach {
            writer.println("${it.street.name} ${it.time}")
        }
    }
}

data class Car(val id: Int, val path: List<Street>) {
    var streetIdx: Int = 0
    var t: Int = -100_000
    val street: Street
        get() = path[streetIdx]

    fun atTheEnd(now: Int): Boolean = (streetIdx == path.lastIndex) && now - t + 1 >= path.last().L

    fun x(now: Int): Int = max(street.L - (now - t), 0)
}

class Cars(val data: Data, val schedules: Map<Int, Schedule>) {
    var score: Int = 0
    val junctions: MutableMap<Int, MutableList<Car>> = data.paths.mapIndexed { id, path ->
        Car(id, path.map { data.idToStreet[it]!! })
    }.groupByTo(mutableMapOf()) {
        it.street.id
    }

    fun tick(turn: Int) {
        val greenCars = junctions.mapNotNull { (_, cars) ->
            val car = cars.firstOrNull()
            if (car != null && car.x(turn) == 0) {
                return@mapNotNull car
            }
            null
        }.groupBy {
            it.street.to
        }.mapNotNull { (cross, cars) ->
            val car = schedules[cross]?.getGreenCar(cars, turn)
            if (car != null) {
                junctions[car.street.id]!!.removeFirst()
                car.streetIdx++
                car.t = turn
            }
            car
        }


        greenCars.forEach { car ->
            junctions.computeIfAbsent(car.street.id) { mutableListOf() }.add(car)
        }

        junctions.forEach { (_, cars) ->
            cars.removeIf {
                val atTheEnd = it.atTheEnd(turn)
                if (atTheEnd) {
                    score += data.F + (data.D - turn - 1)
                }
                atTheEnd
            }
        }
    }
}

