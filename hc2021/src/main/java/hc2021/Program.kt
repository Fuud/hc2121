package hc2021

import java.io.File
import java.util.*


fun main() {
    val currentTimeMillis = System.currentTimeMillis()
    for (input in listOf("a", "b", "c", "d", "e", "f")) {
        val scanner = Scanner(Street::class.java.classLoader.getResourceAsStream("$input.txt"))
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

        val streets = (0 until S).map {
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

        val idToStreet = streets.associateBy { it.id }

        val nameToId = streets.associateBy(keySelector = { it.name }, valueTransform = { it.id })

        val paths: List<List<Int>> = (0 until V).map {
            val P = scanner.nextInt()
            (0 until P).map {
                nameToId[scanner.next()]!!
            }
        }
        println("$input ${paths.size} cars")

        val cross = streets.groupBy { it.to }

        val schedule: Map<Int, Schedule> = cross.mapValues { (_, streets) ->
            Schedule(streets.map { StreetAndTime(it, 1) })
        }

        val cars = Cars(paths, schedule, idToStreet, F, D)
        for (tick in 0 until D) {
            cars.tick(tick)
        }

        println(cars.score)
        println("time " + (System.currentTimeMillis() - currentTimeMillis))

        File("$input.${cars.score}.txt").printWriter().use { writer ->

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
    }
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

    fun isGreen(tick: Int, street: Street): Boolean {
        return greenStreet(tick) == street
    }
}

data class Car(val id: Int, val path: List<Street>, var streetIdx: Int, var x: Int) {
    val street: Street
        get() = path[streetIdx]

    val atTheEnd: Boolean
        get() = (streetIdx == path.lastIndex) && x == 0
}

class Cars(paths: List<List<Int>>, val schedules: Map<Int, Schedule>, val streets: Map<Int, Street>, val F: Int, val D: Int) {
    var score: Int = 0
    val junctions: MutableMap<Int, MutableMap<Int, MutableList<Car>>> = paths.mapIndexed { id, path ->
        Car(id, path.map { streets[it]!! }, 0, 0)
    }.groupBy {
        it.street.to
    }.mapValuesTo(mutableMapOf()) { entry ->
        entry.value.groupByTo(mutableMapOf()) { it.street.id }
    }

    fun tick(turn: Int) {
//        println("==============$turn")
        val greenStreets = schedules.mapValues { it.value.greenStreet(turn) }
//        println(greenStreets)
        val greenCars: Map<Int, Car?> = greenStreets.mapValues {
            val queue = junctions[it.key]?.get(it.value.id)
            val car = queue?.firstOrNull()
            if (car?.x == 0) {
                queue.removeFirst()
                car.streetIdx++
                car.x = car.street.L - 1
//                println("promote $car")
                return@mapValues car
            }
            return@mapValues null
        }

        junctions.forEach { (_, street2Cars) ->
            street2Cars.forEach { (_, cars) ->
                cars.forEach {
                    if (it.x > 0) {
                        it.x--
//                        println("move $it")
                    }
                }
            }
        }

        greenCars.forEach { (_, car) ->
            if (car != null) {
                junctions.computeIfAbsent(car.street.to) { mutableMapOf() }.computeIfAbsent(car.street.id) { mutableListOf() }.add(car)
            }
        }

        junctions.forEach { (_, street2Cars) ->
            street2Cars.forEach { (_, cars) ->
                cars.removeIf{
                    if(it.atTheEnd) {
                        score += F + (D - turn - 1)
//                    println("Score $car $score")
                    }
                    it.atTheEnd
                }
            }
        }
    }
}

