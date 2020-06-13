
trait CanSpeak {
    def speak = println("Hola.")
}

trait CanEat {
    def eat = println("nom nom nom")
}

class Thing {

}

val thing1 = new Thing with CanEat with CanSpeak {
    override def eat = println("thing1 nom nom nom")
}
val thing2 = new Thing with CanEat {
    override def eat = println("thing2 nom nom nom")
}

val things: List[Thing] = List(thing1, thing2)

for (thing <- things) {
    thing match {
        case x: CanSpeak with CanEat => x.speak; x.eat
        case x: CanEat => x.eat
        case _ => println("Cannot speak")
    }
}
