import org.apache.spark.sql.functions._

val p:Int= 8
val d=10
println(s"my name is $p")

val c=true

val df1=("combined results: "+p + d)

println(df1)


println("helo \n how are you")


val num=2
num match{
  case 1 => println("one")
  case 2 => println("two")
  case _ => println("else")
}

//For loop
for (x <- 2 to 100){
  val squared = x*x
  println(squared)

}

//while loop

var i=0

while(i<=10){

  println(i)
  i = i+1
}



//do while

i=0

do{
  println(i)
  i=i+1
}while(i <= 10
)


// in a block last statement is written statement
var df2={
  val x=20
7
}




// higher order function

def trippler (i: Int): Int={
  (i*3)
}

def funt(i: Int, f: Int => Int)= {

  f(i)
}

funt(5,trippler)






val func = udf( (s:String) => if(s.isEmpty) 0 else 1 )






































