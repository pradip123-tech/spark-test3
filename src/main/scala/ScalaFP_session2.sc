//first class function



//we should be able to assign a function to variable
def doubler (i: Int): Int={
  i*2
}

val a=doubler(5)


// we should be able to assign function as a parameter

def trippler (i: Int): Int={
  i*3
}

def func(i:Int,f: Int=> Int)={
  f(i)
}

func(5,trippler)


//higher order function:

var a=5 to 15

a.map(doubler)

//loop vs recursion vs tail recursion
//loop
def factorial(input: Int): Int={
  var result: Int=1
  for (i <- 1 to input) {

      result=result*i
    }
    result
  }


factorial(5)

//recursion

def factorial (i :Int): Int={
if (i==1) 1
else i*factorial(i-1)
}

factorial(6)