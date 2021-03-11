package hc2021

import java.io.File
import java.io.PrintWriter
import java.text.DecimalFormat
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max
import kotlin.math.min


enum class Task(val fileName: String, val period: Int, val threshold: Double, val randomAtStreet: Double, val randomAtCar: Double) {
    A("a", 2, 0.0, 0.0, 0.4),
    B("b", 4, 0.0, 0.0, 0.4),
    C("c", 4, 0.0, 0.0, 0.4),
    D("d", 2, 0.0, 0.5, 0.4),
    E("e", 7, 0.0027, 0.4, 0.4),
    F("f", 10, 0.0032, 0.0, 0.0);

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
                B -> 4_569_151
                C -> 1_308_690
                D -> 2_499_952
                E -> 722_515
                F -> 1_446_515
            }
        }
    }
}

data class Data(val D: Int, val F: Int, val streets: List<Street>, val paths: List<List<Int>>, val task: Task) {
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
    val carsWeightSpare: MutableMap<Int, Int> = mutableMapOf()

    // сколько маршрутов проходит через заданную улицу.
    val streetCount: MutableMap<Int, Int> = mutableMapOf()
    val countSpare: MutableMap<Int, Int> = mutableMapOf()

    // все маршруты подходят к этим перекрёсткам с разных улиц. подмножество всех cross-ов.
    val onDemandCrosses: Map<Int, List<Street>>

    val twoWayCrosses: Map<Int, Pair<Street, Street>>

    // первые перекрёстки в маршрутах
    val firstCrosses: MutableMap<Int, MutableSet<Int>> = mutableMapOf()

    val firstCars: MutableMap<Int, MutableList<Int>> = mutableMapOf()

    init {
        paths.forEachIndexed { i, it ->
            val bonus = (D - lengths[i]!! + F)
            carsWeight[i] = bonus
            carsWeightSpare[i] = bonus
            it.forEach { id ->
                streetWeight.compute(id) { _, old -> if (old == null) bonus else old + bonus }
                spare.compute(id) { _, old -> if (old == null) bonus else old + bonus }
                streetCount.compute(id) { _, old -> if (old == null) 1 else old + 1 }
                countSpare.compute(id) { _, old -> if (old == null) 1 else old + 1 }
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
        this.twoWayCrosses = cross.filterNot { (id, _) -> onDemandCrosses.containsKey(id) }
                .filter { (_, streets) ->
                    streets.filter { streetCount.containsKey(it.id) }
                            .groupBy { streetCount[it.id]!! }.size == 2
                }.mapValues { it.value[0] to it.value[1] }
    }
}

val random = Random()

fun main() {
    val tasks = readData()
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
            data.streetCount.putAll(data.countSpare)
            data.carsWeight.putAll(data.carsWeightSpare)
            val task = entry.key
            val period = task.period + random.nextInt(3) - 1
            val onDemand = AtomicInteger()
            val twoWay = AtomicInteger()
            val twoStreet = AtomicInteger()
            val simple = AtomicInteger()
            val map = TreeMap<Int, Int>()

            val schedule: Map<Int, Schedule> = data.cross.mapValues { (cross, streets) ->
//                map.merge(streets.size, 1) { x, v -> v + 1 }
                val get: Int = map.get(streets.size) ?: 0
                map.put(streets.size, get + 1)

                // каждая дорога с одним маршрутом, по запросу включаем светофор.
                if (data.onDemandCrosses.containsKey(cross)) {
                    onDemand.incrementAndGet()
                    return@mapValues ScheduleOnDemand(streets.first(), data)
                }
                val createSchedule = createSchedule(streets, data, period, task.threshold, random)
                if (createSchedule is TwoWaySchedule) {
                    twoWay.incrementAndGet()
                } else {
                    if (streets.size == 2) {
                        twoStreet.incrementAndGet()
                    }
                    simple.incrementAndGet()
                }

                return@mapValues createSchedule
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
                val instance = DecimalFormat.getInstance()
                instance.maximumFractionDigits = 3
                instance.minimumFractionDigits = 3
                var message = "$task - $period - ${carsScoreStr}\t" +
                        "(${instance.format(cars.score / data.maxScore.toDouble() * 100)}%)\t" +
                        "${cars.finished}/${data.paths.size}\t" +
                        " ${(System.currentTimeMillis() - start) / 1000}s,\t" +
                        "count = $count\tnew = $newRecords\tdiff = $diffStr\t"
//                if(count % 10 == 0)
//                message += " stat simple = ${simple.get()} onDemand = ${onDemand.get()} twoWay = ${twoWay.get()} twoSize = ${twoStreet.get()} map = $map"

                println(message)
            }
        }
        count++
    }
}

private fun createSchedule(
        streets: List<Street>,
        data: Data,
        period: Int,
        threshold: Double,
        random: Random): Schedule {
    val sum = streets.map { data.streetWeight.getOrDefault(it.id, 0) }.sum()
    val list = mutableListOf<StreetAndTime>()
    val localPeriod = min(sum / (data.F + data.D), period)
    val vw: Map<Street, Double> = streets.filter { data.streetWeight.containsKey(it.id) }
            .map { it to data.streetWeight[it.id]!! / sum.toDouble() }
            .toMap()
    if (vw.size == 2) {
        twoSizedStreet(vw, list, streets, data, sum, localPeriod)
    } else {
        commonCase(streets, data, vw, threshold, localPeriod, sum, list)
    }
    val shuffled = list.shuffled(random)
    if (shuffled.size == 2) {
        val first = shuffled[0]
        val second = shuffled[1]
        if (data.streetCount[first.street.id] == 1) {
            return TwoWaySchedule(data, first.street, second.street)
        }
        if (data.streetCount[second.street.id] == 1) {
            return TwoWaySchedule(data, second.street, first.street)
        }
//        return ScheduleImpl(shuffled)
    }

    return SortOnDemandSchedule(shuffled, data)
}

private fun commonCase(
    streets: List<Street>,
    data: Data,
    vw: Map<Street, Double>,
    threshold: Double,
    localPeriod: Int,
    sum: Int,
    list: MutableList<StreetAndTime>
) {
    for (street in streets) {
        val frequency = data.streetWeight[street.id] ?: 0
        if (frequency > 0 && vw[street]!! > threshold) {
            val time = frequency * localPeriod / sum
            list.add(StreetAndTime(street, if (time > 0) time else 1))
        }
    }
}

private fun twoSizedStreet(
    vw: Map<Street, Double>,
    list: MutableList<StreetAndTime>,
    streets: List<Street>,
    data: Data,
    sum: Int,
    localPeriod: Int
) {
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
            if (frequency > 0 && sum != 0) {
                val time = frequency * localPeriod / sum
                list.add(StreetAndTime(street, if (time > 0) time else 1))
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

        val paths: List<List<Int/*street id*/>> = (0 until V).map {
            val P = scanner.nextInt()
            (0 until P).map {
                nameToId[scanner.next()]!!
            }
        }
        val data = Data(D, F, streets, paths, task)
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
//    val randomAtStreet = data.task == Task.E
//    val randomAtCar = data.task != Task.E
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
                        .maxByOrNull {
                            data.streetWeight[it.street.id]!! * (1 + data.task.randomAtStreet * random.nextDouble()) +
                                    data.streetCount[it.street.id]!!
                        }
//                        .maxByOrNull { data.streetWeight[it.street.id]!! }
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
                    .maxByOrNull { data.carsWeight[it.id]!! * (1 + data.task.randomAtCar * random.nextDouble()) }
//                    .maxByOrNull { data.carsWeight[it.id]!! + data.streetCount[it.street.id]!! }
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
                reduceCarWeight(cars.filter { candidates.containsKey(it.street) && !it.equals(nextCar) }.toList())
                return nextCar
            }
            return null
        } else {
            val theCar = cars.find { it.street == currentGreenStreet.street } ?: return null
            reduceStreetWeight(theCar)
            return theCar
        }
    }

    private fun reduceCarWeight(candidates: List<Car>) {
        candidates.forEach { data.carsWeight[it.id] = data.carsWeight[it.id]!! - 1 }
    }

    private fun reduceStreetWeight(theCar: Car) {
        val put = data.streetWeight.get(theCar.street.id)!! - data.lengths[theCar.id]!!
        data.streetWeight[theCar.street.id] = put
        val i: Int = data.streetCount.get(theCar.street.id) ?: 0
        data.streetCount[theCar.street.id] = i - 1
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

class TwoWaySchedule(val data: Data, val oneTime: Street, val manyTime: Street) : Schedule {
    var previousCar = false
    var schedule: Schedule? = null

    override fun getGreenCar(cars: List<Car>, tick: Int): Car? {
        val theSchedule = schedule
        if (theSchedule != null) {
            return theSchedule.getGreenCar(cars, tick)
        }
        val oneCar = cars.find { it.street == oneTime }
        if (oneCar != null) {
            if (previousCar) {
                schedule = ScheduleImpl(listOf(StreetAndTime(manyTime, tick), StreetAndTime(oneTime, 1)))
            } else {
                schedule = ScheduleImpl(listOf(StreetAndTime(oneTime, tick + 1), StreetAndTime(manyTime, data.D)))
            }
            return oneCar
        }
        val car = cars.find { it.street == manyTime }
        previousCar = previousCar || car != null
        return car
    }

    override fun isNotEmpty(): Boolean = true

    override fun write(writer: PrintWriter) {
        if (schedule == null) {
            schedule = ScheduleImpl(listOf(StreetAndTime(manyTime, data.D)))
        }
        schedule?.write(writer)
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
    var finished: Int = 0
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
                    finished += 1
                }
                atTheEnd
            }
        }
    }
}

