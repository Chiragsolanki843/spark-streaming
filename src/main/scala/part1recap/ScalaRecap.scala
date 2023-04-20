package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = false

  // expression
  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  val theUnit = println("Hello,Scala") // Unit => "no meaningful value" = void in other languages
  // printing something in console it will return unit type

  // functions
  def myFunction(x: Int) = 42

  // OOP
  class Animal

  class Cat extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }

  // singleton pattern
  object MySingleton {
    // have methods and members
  }

  // Companions
  object Carnivore

  // generics
  trait MyList[A]
  // contra-variant, co-variant those are two type of generics

  // method notation // below are infix methods notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming
  val incrementer: Int => Int = x => x + 1 // Anonymous Function or lambda

  val incremented = incrementer(42)

  // map, flatMap, filter (HOF => received function as arguments)
  val processedList = List(1, 2, 3).map(incrementer)

  // Pattern Matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case e: NullPointerException => "some returned value"
    //case _: NullPointerException => "some returned value" // this line also fine
    case _ => "something value"
  }

  // Future

  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"I've found $meaningOfLife")
    case Failure(ex) => println(s"I have failed : $ex")
  }

  // Partial functions
  val aPartialFunction: PartialFunction[Int, Int] = {
    // val aPartialFunction = (x:Int) => x match {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  // Implicits

  // auto-injection by the compiler
  def methodWithImplicitArgument(implicit x: Int) = x + 43

  implicit val ImplicitInt = 67
  val implicitCall = methodWithImplicitArgument // compiler implicitly add this value to method. no need to pass argument
  //val implicitCall = methodWithImplicitArgument(67) // passing argument explicitly its fine. no need to pass

  // Implicit conversions - implicit defs
  // case class - light weight Data Structure and also have bunch of utility methods already add by compiler
  case class Person(name: String) {
    def greet = println(s"Hi, my name is $name")
  }

  implicit def fromStringToPerson(name: String) = Person(name)

  "Bob".greet // fromStringToPerson("Bob").greet
  // compiler automatically converting string to Person and call to greet on that.

  // Implicit conversion - implicit classes
  implicit class Dog(name: String) {
    def bark = println("Bark!")
  }

  "Lassie".bark // convert automatically into Dog class argument then call bark method on that.

  /*
    - local scope -->  explicitly define --> implicit val ImplicitInt = 67
    - imported scope --> import scala.concurrent.ExecutionContext.Implicits.global
    - companion objects of the types involved in the method call
  */
  List(1, 2, 3).sorted // compiler looking for companion object of list or companion object of type[]
  // for Int we already have implicit ordering
  
}











