package hc2021

import java.util.*


fun main() {
    val scanner = Scanner(Street::class.java.classLoader.getResourceAsStream("a.txt"))

    val D = scanner.nextInt()
    val I = scanner.nextInt()
    val S = scanner.nextInt()
    val V = scanner.nextInt()
    val F = scanner.nextInt()

    val streets = (0 until S).map {
        Street(
            id = it,
            B = scanner.nextInt(),
            E = scanner.nextInt(),
            name = scanner.next(),
            L = scanner.nextInt()
        )
    }

    val idToStreet = streets.associateBy { it.id }

    val nameToId = streets.associateBy (keySelector = {it.name}, valueTransform = {it.id})

    val paths = (0 until V).map {
        val P = scanner.nextInt()
        (0 until P).map {
            nameToId[scanner.next()]!!
        }
    }

    println()
}

data class Street(val id: Int, val B: Int, val E: Int, val name: String, val L: Int)
