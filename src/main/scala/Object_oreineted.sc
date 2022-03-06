
abstract class Animal {
    def eat



  }


  trait coldBlood

  class dog extends Animal with coldBlood {
    override def eat: Unit = println("eat a lot")
}
  val Croc= new dog
Croc.eat

//case class

//1 class parameters are promoted to fields so we do not have to add val and vars

case class person(name:String, age: Int)

val person1= new person("pradip",25)

println(person1.age)

//2 case class have very handy toString method

println(person1.toString)

// 3 equals and hash code method alreday been implemented

val person2= new person("pradip",25)

println(person1==person2)

//4 companion object is already present we do not have to creat another

val person3=person.apply("pradip",25)

println(person3)


//5 it has very handy copy method

val person5=person1.copy(age = 26)

println(person5)

// case classes are serillizable

var a="hello";a="bye"