package hc2021

import java.io.BufferedWriter
import java.io.File
import java.lang.IllegalStateException
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
                B = scanner.nextInt(),
                // end
                E = scanner.nextInt(),
                name = scanner.next(),
                // length
                L = scanner.nextInt()
            )
        }

        val idToStreet = streets.associateBy { it.id }

        val nameToId = streets.associateBy(keySelector = { it.name }, valueTransform = { it.id })

        val paths = (0 until V).map {
            val P = scanner.nextInt()
            (0 until P).map {
                nameToId[scanner.next()]!!
            }
        }


//    val bestPath = paths.minBy {
//        it.size - 1 + it.map{idToStreet[it]!!.L}
//    }

        val cross = streets.groupBy { it.E }

        val schedule = cross.mapValues { (crossId, streets) ->
            streets.map { StreetAndTime(it, 1) }
        }

        fun greenStreet(tick: Int, cross: Int): Street {
            val times = schedule[cross]!!
            val sumTimes = times.sumBy { it.time }
            val reminder = tick % sumTimes
            var timeCount = 0
            for (strTime in times) {
                timeCount += strTime.time
                if (timeCount > reminder) {
                    return strTime.street
                }
            }
            throw IllegalStateException("Should not reach here")
        }

        fun isGreen(tick: Int, street: Street): Boolean {
            return greenStreet(tick, street.E) == street
        }

        data class Car(val id: Int, val path: List<Street>, var streetIdx: Int, var x: Int) {
            val street: Street
                get() = path[streetIdx]

            val atTheEnd: Boolean
                get() = (streetIdx == path.lastIndex) && x == 0
        }

        val cars = paths.mapIndexed { id, path ->
            val street = idToStreet[path.first()]!!
            Car(id, path.map { idToStreet[it]!! }, 0, 0)
        }.toMutableList()

        var score = 0

        for (tick in 0 .. D) {
            val carsPerStreet = mutableMapOf<Street, MutableList<Car>>()
            cars
                .forEach { car ->
                if (car.x > 0) {
                    car.x--
                } else {
                    carsPerStreet.computeIfAbsent(car.street) { mutableListOf<Car>() }.add(car)
                }
            }

            cars.filter { it.atTheEnd }.forEach { car ->
                score += F + (D - tick + 1)
                cars.removeIf { it.id == car.id }
                carsPerStreet.getOrDefault(car.street, mutableListOf()).removeIf { it.id == car.id }
            }

            val greenStreets = streets
                .filter { isGreen(tick, it) }
            greenStreets
                .mapNotNull { carsPerStreet[it]?.first() }
                .forEach {
                    it.streetIdx++
                    it.x = it.street.L
                }


        }


    System.out.println(score)
    System.out.println("time " + (System.currentTimeMillis() - currentTimeMillis))

    File("$input.$score.txt").printWriter().use { writer ->

        writer.println(I)

        (0 until I).forEach {
            writer.println(it)
            val streets = cross[it]!!
            writer.println(streets.size)
            streets.forEach {
                writer.println("${it.name} 1")
            }
        }
    }
}


}

fun BufferedWriter.writeLn(s: Any) {
    this.write(s.toString())
    this.newLine()
}

data class Street(val id: Int, val B: Int, val E: Int, val name: String, val L: Int)

data class StreetAndTime(val street: Street, val time: Int)

