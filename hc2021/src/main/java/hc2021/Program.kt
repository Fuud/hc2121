package hc2021

import java.io.BufferedWriter
import java.io.File
import java.util.*


fun main() {
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


        File("$input.out").printWriter().use { writer ->

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

