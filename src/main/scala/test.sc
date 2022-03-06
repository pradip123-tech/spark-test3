import scala.Console.println

def greettingKid(name: String, age: Int) :Unit = {

  println(s"hi all my name is $name and i am $age years old")



}
println(greettingKid("pradip",25))

def factorial (n: Int): Int= {
  if (n <= 0) 1
  else n * factorial(n - 1)

}
println(factorial(5))


def isPrime(n: Int): Boolean = {
  def isPrimeUntill(t: Int): Boolean=
    if (t <=1) true
    else n % t != 0 && isPrimeUntill(t-1)

  isPrimeUntill(n/2)


}



print(isPrime(37))

