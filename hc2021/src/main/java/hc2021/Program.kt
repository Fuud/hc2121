package hc2021

import java.io.File
import java.io.PrintWriter
import java.util.*
import kotlin.math.max
import kotlin.math.min


enum class Task(val fileName: String, val period: Int, val threshold: Double) {
    A("a", 2, 0.0),
    B("b", 4, 0.0),
    C("c", 4, 0.0),
    D("d", 2, 0.0),
    E("e", 6, 0.0),
//    E("e", 6, 0.0027),

    //    F("f", 10, 0.0032)
    F("f", 10, 0.0)
    ;

    companion object {
        fun toProcess(): List<Task> {
            return listOf(
//                    B,
                C,
//                    D,
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
                E -> 720_620
                F -> 1_443_229
            }
        }
    }
}

data class Data(val D: Int, val F: Int, val streets: List<Street>, val paths: List<List<Int>>, val task: Task) {
    val idToStreet = streets.associateBy { it.id }
    val nameToStreet = streets.associateBy { it.name }

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

    val streetToCars: MutableMap<Int, MutableSet<Int>> = mutableMapOf()

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
        this.twoWayCrosses = cross.filterNot { (id, _) -> onDemandCrosses.containsKey(id) }
            .filter { (_, streets) ->
                streets.filter { streetCount.containsKey(it.id) }
                    .groupBy { streetCount[it.id]!! }.size == 2
            }.mapValues { it.value[0] to it.value[1] }

        paths.flatMapIndexed { i, list ->
            list.map { i to it }
        }.forEach { (carId, streetId) ->
            streetToCars.computeIfAbsent(streetId) { mutableSetOf() }.add(carId)
        }
    }
}

val random = Random()

fun improve() {
    val tasks = readData()
    val data = tasks[Task.F]!!
    val schedule = readSchedule(data, "f.1446564.txt")
    val carsImpl = UnarrivedCars(data, schedule)
    val (score, newSchedule) = carsImpl.emulate()

    File("${data.task.fileName}.$score.txt").printWriter().use { writer ->

        writer.println(newSchedule.size)

        newSchedule.forEach { (crossId, it) ->
            writer.println(crossId)
            it.write(writer)
        }
    }
}

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
//            data.streetWeight.putAll(data.spare)
            val task = entry.key
            val period = random.nextInt(3) - 1 + task.period
            val unArrived: MutableSet<Int> = mutableSetOf()
            var tryImprove: Boolean
            do {
                val schedule: Map<Int, Schedule> = data.cross.mapValues { (cross, streets) ->
//                    if (data.onDemandCrosses.containsKey(cross)) {
//                        return@mapValues ScheduleOnDemand(streets.first(), data)
//                    }
                    val nonEmptyStreets = streets.filterNot {
                        val cars = data.streetToCars[it.id]
                        if (cars == null) true else unArrived.containsAll(cars)
                    }.toSet()
                    if (nonEmptyStreets.isEmpty()) {
                        return@mapValues ScheduleImpl(emptyList())
                    }
                    return@mapValues createSchedule(nonEmptyStreets, data, period, task.threshold, random, unArrived)
                }.filter { it.value.isNotEmpty() }
                val cars = Cars(data, schedule)
                for (tick in 0 until data.D) {
                    cars.tick(tick)
                }
                var scheduleImpl = schedule.mapValues { it.value.def() }
                    .filter { it.value.isNotEmpty() }
                    .toMap()

                val carsImpl = UnarrivedCars(data, scheduleImpl)
                val (score, newSchedule) = carsImpl.emulate()
                scheduleImpl = newSchedule
                if (cars.score != score) {
                    println("${cars.score} !!! $score")
                }

                if (scores[task] ?: 0 < score) {
                    newRecords++
                    println(" + $task - $period - $score(${score / data.maxScore.toDouble() * 100}%) ${(System.currentTimeMillis() - start) / 1000}s")
                    File("${task.fileName}.$score.txt").printWriter().use { writer ->

                        writer.println(schedule.size)

                        scheduleImpl.forEach { (crossId, it) ->
                            writer.println(crossId)
                            it.write(writer)
                        }
                    }
                    scores[task] = score
                } else {
                    val diffStr = String.format("%,d", ((scores[task] ?: 0) - score))
                    val carsScoreStr = String.format("%,d", score)
                    println(
                        "$task - $period - ${carsScoreStr} (${score / data.maxScore.toDouble() * 100}%) ${(System.currentTimeMillis() - start) / 1000}s, " +
                                " count = $count new = $newRecords diff = $diffStr"
                    )
                }

                val findSlow = Cars(data, scheduleImpl)
                for (tick in 0 until data.D) {
                    findSlow.tick(tick)
                }

                val slowCars = findSlow.junctions.filter { (_, list) ->
                    list.isNotEmpty()
                }.flatMap { (_, list) ->
                    list
                }.sortedBy {
                    -it.path.takeLast(it.path.size - it.streetIdx - 1).map { it.L }.sum()
                }.map {
                    it.id
                }.toList()
                var removed = 0
                val limit = max((slowCars.size - unArrived.size) / 3, 1)
                slowCars.takeWhile {
                    if (unArrived.add(it)) {
                        removed++
                    }
                    removed < limit
                }
                tryImprove = removed > 0
                if (tryImprove) {
                    println("Try improve: remove $removed cars.")
                }
            } while (tryImprove)
        }
        count++
    }
}

private fun createSchedule(
    streets: Set<Street>,
    data: Data,
    period: Int,
    threshold: Double,
    random: Random,
    unArrived: Set<Int>
): Schedule {
    if (streets.size == 1) {
        return ScheduleImpl(listOf(StreetAndTime(streets.first(), 1)))
    }
    val onDemandSchedule = streets.map {
        val set = data.streetToCars[it.id]!!.toMutableSet()
        set.removeAll(unArrived)
        set.size to it
    }
    if (onDemandSchedule.all { it.first <= 1 }) {
        val onDemandStreets: List<Street> = onDemandSchedule.filter { it.first == 1 }
            .map { it.second }
        if (onDemandStreets.isEmpty()) {
            return ScheduleImpl(listOf())
        }
        if (onDemandStreets.size == 1) {
            return ScheduleImpl(listOf(StreetAndTime(onDemandStreets.first(), 1)))
        }
        return ScheduleOnDemand(onDemandStreets.toSet(), data, unArrived)
    }
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
                if (frequency > 0 && sum != 0) {
                    val time = Math.min(frequency * localPeriod / sum, data.streetCount[street.id]!!)
                    list.add(StreetAndTime(street, if (time > 0) time else 1))
                }
            }
        }
    } else {
        for (street in streets) {
            val frequency = data.streetWeight[street.id] ?: 0
            if (frequency > 0 && vw[street]!! > threshold) {
                val time = Math.min(frequency * localPeriod / sum, data.streetCount[street.id]!!)
                list.add(StreetAndTime(street, if (time > 0) time else 1))
            }
        }
    }
    val shuffled = list.shuffled(random)
    if (shuffled.size == 2) {
        val first = shuffled[0]
        val second = shuffled[1]
        if (data.streetCount[first.street.id] == 1) {
            return TwoWaySchedule(data, first.street, second.street, unArrived)
        }
        if (data.streetCount[second.street.id] == 1) {
            return TwoWaySchedule(data, second.street, first.street, unArrived)
        }
    }

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

fun readSchedule(data: Data, fileName: String): MutableMap<Int, ScheduleImpl> {
    val scanner = Scanner(Street::class.java.classLoader.getResourceAsStream(fileName))
    val size = scanner.nextInt()
    val result: MutableMap<Int, ScheduleImpl> = mutableMapOf()
    for (i in 0 until size) {
        val crossId = scanner.nextInt()
        val lights = scanner.nextInt()
        val schedule: MutableList<StreetAndTime> = mutableListOf()
        for (j in 0 until lights) {
            val streetName = scanner.next()
            val times = scanner.nextInt()
            schedule.add(StreetAndTime(data.nameToStreet[streetName]!!, times))
        }
        result[crossId] = ScheduleImpl(schedule)
    }
    return result
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
    fun getGreenCar(cars: List<List<Car>>, tick: Int): Car?
    fun isNotEmpty(): Boolean
    fun def(): ScheduleImpl
}

class ScheduleOnDemand(val demandStreets: Set<Street>, val data: Data, val unArrived: Set<Int>) : Schedule {
    val lights: TreeMap<Int, Street> = TreeMap()

    override fun getGreenCar(cars: List<List<Car>>, tick: Int): Car? {
        if (lights.containsKey(tick)) {
            return cars.find { it.first().street == lights[tick] }?.first()
        }
        val streetList = cars.filter { demandStreets.contains(it.first().street) }
            .filter {
                val index = it.indexOfFirst { !unArrived.contains(it.id) }
                if (index >= 0) {
                    return@filter it[index].x(tick) <= index
                    //return@filter it[index].x(tick) ==0
                }
                false
            }
        if (lights.containsKey(tick - 1)) {
            val street = lights[tick - 1]!!
            val car = streetList.find {
                it.first().street == street
            }
            if (car != null) {
                lights[tick] = street
                if (car.first().x(tick) == 0) {
                    return car.first()
                } else {
                    println("so strange")
                    return null
                }
            }
        }
        val carList = streetList
            .maxByOrNull { data.streetWeight[it.first().street.id]!! }
        if (carList != null) {
            val street = carList.first().street
//            val index = carList.indexOfFirst { !unArrived.contains(it.id) }
//            for (i in 0..index) {
//                lights[tick + i] = street
//            }
            lights[tick] = street
        }
        return carList?.first()
    }

    override fun isNotEmpty(): Boolean = true

    override fun def(): ScheduleImpl {
        if (lights.isEmpty()) {
            return ScheduleImpl(emptyList())
        }
        val streets = mutableSetOf<Street>()
        val localLights: TreeMap<Int, Street> = TreeMap()
        lights.descendingMap().forEach { (tick, street) ->
            if (!streets.contains(street)) {
                streets.add(street)
                localLights[tick] = street
            }
        }
        val list: MutableList<StreetAndTime> = mutableListOf()
        var turn = 0
        for ((tick, street) in localLights) {
            list.add(StreetAndTime(street, tick - turn + 1))
            turn = tick
        }
        return ScheduleImpl(list)
    }
}

class SortOnDemandSchedule(initial: List<StreetAndTime>, val data: Data) : Schedule {
    val period = initial.sumBy { it.time }
    val result: MutableList<StreetAndTime> = initial.toMutableList()
    val swapByTime: MutableMap<Int, MutableList<StreetAndTime>> = initial.groupByTo(mutableMapOf()) { it.time }
    val swapByStreet: MutableMap<Street, StreetAndTime> = initial.map { it.street to it }.toMap(mutableMapOf())
    var timeToInsert = 0
    var indexToInsert = 0
    val randomAtStreet = data.task == Task.E
    val randomAtCar = data.task != Task.E

    override fun getGreenCar(cars: List<List<Car>>, tick: Int): Car? {
        val reminder = tick % period
        if (timeToInsert >= 0) {
            if (reminder < timeToInsert) {
                val greenStreet = greenStreet(tick)
                val theCar = cars.find { it.first().street == greenStreet.street }?.first()
//                reduceStreetWeight(theCar)
                return theCar
            } else {
                val delta = reminder - timeToInsert
                val nextCar = cars.filter { swapByStreet[it.first().street]?.time ?: 0 > delta }
                    .maxByOrNull { data.streetWeight[it.first().street.id]!! * if (randomAtStreet) (1 + 0.4 * random.nextDouble()) else 1.0 }
//                    .maxByOrNull { data.streetWeight[it.street.id]!! }
                if (nextCar != null) {
                    val streetAndTime = swapByStreet.remove(nextCar.first().street)!!
                    result.remove(streetAndTime)
                    result.add(indexToInsert, streetAndTime)
                    swapByTime[streetAndTime.time]!!.remove(streetAndTime)
                    timeToInsert += streetAndTime.time
                    indexToInsert++
//                    reduceStreetWeight(nextCar)
                    return nextCar.first()
                }
                timeToInsert = -1
            }
        }
        val currentGreenStreet = greenStreet(tick)
        if (swapByStreet.containsKey(currentGreenStreet.street)) {
            val time = swapByStreet[currentGreenStreet.street]!!.time
            val candidates = swapByTime[time]!!.map { it.street to it }.toMap()
            val nextCar = cars.filter { candidates.containsKey(it.first().street) }
                .maxByOrNull { data.carsWeight[it.first().id]!! * if (randomAtCar) (1 + 0.4 * random.nextDouble()) else 1.0 }
//                .maxByOrNull { data.carsWeight[it.id]!! }
            if (nextCar != null) {
                val streetAndTime = swapByStreet.remove(nextCar.first().street)!!
                swapByTime[streetAndTime.time]!!.remove(streetAndTime)
                val current = result.indexOf(currentGreenStreet)
                val next = result.indexOf(streetAndTime)
                if (current != next) {
                    result[current] = streetAndTime
                    result[next] = currentGreenStreet
                }
//                reduceStreetWeight(nextCar)
                return nextCar.first()
            }
            return null
        } else {
            val theCar = cars.find { it.first().street == currentGreenStreet.street }?.first()
//            reduceStreetWeight(theCar)
            return theCar
        }
    }

//    private fun reduceStreetWeight(theCar: Car) {
//        val put = data.streetWeight.get(theCar.street.id)!! - data.lengths[theCar.id]!!
//        data.streetWeight[theCar.street.id] = put
//    }

    override fun def(): ScheduleImpl = ScheduleImpl(result)

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

class TwoWaySchedule(val data: Data, val oneTime: Street, val manyTime: Street, val unArrived: Set<Int>) : Schedule {
    var previousCar = false
    var schedule: ScheduleImpl? = null

    override fun getGreenCar(cars: List<List<Car>>, tick: Int): Car? {
        val theSchedule = schedule
        if (theSchedule != null) {
            return theSchedule.getGreenCar(cars, tick)
        }
        val oneCar = cars.find {
            if (it.first().street == oneTime) {
                val index = it.indexOfFirst { !unArrived.contains(it.id) }
                if (index >= 0) {
                    return@find it[index].x(tick) <= index
                }
            }
            false
        }

        if (oneCar != null) {
            val times = oneCar.indexOfFirst { !unArrived.contains(it.id) } + 1
            if (previousCar) {
                if (times < tick * 2) {
                    schedule = ScheduleImpl(listOf(StreetAndTime(manyTime, tick), StreetAndTime(oneTime, times)))
                    return oneCar.first()
                }
            } else {
                schedule = ScheduleImpl(listOf(StreetAndTime(oneTime, tick + times), StreetAndTime(manyTime, data.D)))
                return oneCar.first()
            }
        }
        val car = cars.find { it.first().street == manyTime }
        previousCar = previousCar || car != null
        return car?.first()
    }

    override fun isNotEmpty(): Boolean = true

    override fun def(): ScheduleImpl {
        if (schedule == null) {
            schedule = ScheduleImpl(listOf(StreetAndTime(manyTime, data.D)))
        }
        return schedule!!
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

    override fun getGreenCar(cars: List<List<Car>>, tick: Int): Car? {
        val greenStreet = greenStreet(tick)
        return cars.find { it.first().street == greenStreet }?.first()
    }

    override fun def(): ScheduleImpl = this

    override fun isNotEmpty() = lights.isNotEmpty()

    fun write(writer: PrintWriter) {
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
                return@mapNotNull cars
            }
            null
        }.groupBy {
            it.first().street.to
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

class UnarrivedCars(val data: Data, val schedules: Map<Int, ScheduleImpl>) {
    var score: Int = 0
    val junctions: MutableMap<Int, MutableList<Car>> = initJunctions()
    var initScore = 0;
    var maxScore = 0
    var maxSchedule = schedules
    val street2Car: MutableMap<Int, MutableList<Int>> = mutableMapOf()

    private fun initJunctions() = data.paths.mapIndexed { id, path ->
        Car(id, path.map { data.idToStreet[it]!! })
    }.groupByTo(mutableMapOf()) {
        it.street.id
    }

    fun emulate(): Pair<Int, Map<Int, ScheduleImpl>> {
        val excluded: MutableSet<Int> = mutableSetOf()
        var tryImprove: Boolean = false
        val localSchedules = schedules.toMutableMap()
        do {
            score = 0
            street2Car.clear()
            junctions.clear()
            junctions.putAll(initJunctions())
            for (tick in 0 until data.D) {
                tick(tick, localSchedules)
            }
            if (score > maxScore) {
                maxScore = score
                maxSchedule = localSchedules.toMap()
            }
            if (initScore == 0) {
                initScore = score
            }

            tryImprove = false
            val unarrived = unarrived()
            for (id in unarrived) {
                if (!excluded.contains(id)) {
                    excluded.add(id)
                    street2Car.filter { (_, cars) ->
                        excluded.containsAll(cars)
                    }.forEach { (streetId, _) ->
                        val cross = data.idToStreet[streetId]!!.to
                        val schedule = localSchedules[cross]
                        if (schedule != null) {
                            val lights = schedule.lights.toMutableList()
                            if (lights.size == 1) {
                                localSchedules.remove(cross)
                                tryImprove = true
                            } else {
                                val index = lights.indexOfFirst { it.street.id == streetId }
                                if (index > -1) {
                                    val streetAndTime = lights[index]
                                    val mergeIndex = if (index == 0) 1
                                    else if (index == lights.size - 1) lights.size - 2
                                    else index + random.nextInt(2) * 2 - 1
                                    val mergeStreetAndTime = lights[mergeIndex]
                                    lights[index] =
                                        StreetAndTime(mergeStreetAndTime.street, mergeStreetAndTime.time + streetAndTime.time)
                                    lights.removeAt(mergeIndex)
                                    localSchedules[cross] = ScheduleImpl(lights)
                                    tryImprove = true
                                }
                            }
                        }
                    }
                }
                if (tryImprove) {
                    break
                }
            }
        } while (tryImprove)
        println("improved from $initScore to $maxScore")
        return maxScore to maxSchedule
    }

    fun tick(turn: Int, localSchedules: MutableMap<Int, ScheduleImpl>) {
        val greenCars = junctions.mapNotNull { (_, cars) ->
            val car = cars.firstOrNull()
            if (car != null && car.x(turn) == 0) {
                return@mapNotNull cars
            }
            null
        }.groupBy {
            it.first().street.to
        }.mapNotNull { (cross, cars) ->
            val car = localSchedules[cross]?.getGreenCar(cars, turn)
            if (car != null) {
                val streetId = car.street.id
                junctions[streetId]!!.removeFirst()
                street2Car.computeIfAbsent(streetId) { mutableListOf() }.add(car.id)
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

    fun unarrived(): List<Int> {
        return junctions.filter { (_, list) ->
            list.isNotEmpty()
        }.flatMap { (_, list) ->
            list
        }.sortedBy {
            -it.path.takeLast(it.path.size - it.streetIdx - 1).map { it.L }.sum()
        }.map {
            it.id
        }.toList()
    }

}


