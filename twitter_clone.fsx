// Create 3 things client, server and simulator-> The simulator will basically be the one who sends the number of followers

#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 
#r "nuget: MathNet.Numerics"
#r "nuget: MathNet.Numerics.Fsharp"
open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit
open FSharp.Control
open MathNet.Numerics.Random
open MathNet.Numerics.Distributions


// create a parametrized distribution instance


let system = System.create "Fsharp"
let mutable terminate = true
let mutable N = 100

let client(clientMailbox: Actor<_>) = 
    let rec loop() = 
        actor{
           let! message = clientMailbox.Receive()
           //Processing
        }
    loop()

let clientParent (childMailbox: Actor<_>) = 
    
    for i in 1..N do
        spawn system (i|>string)
    let rec loop() = 
        actor{
            let mutable             
        }
    