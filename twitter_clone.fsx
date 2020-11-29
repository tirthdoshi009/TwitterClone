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
// some probability distributions
let rnd = Random()
let system = System.create "system" (Configuration.defaultConfig())
let mutable terminate = true
let mutable N = 100
let zipf = Zipf(1.2,N)
let mutable array = Array.create 100 0 
zipf.Samples(array) 
printfn "%A" array
let client (id: int) (follower: seq<int>) (following: Set<int>) (clientMailbox: Actor<_>) = 
    let rec loop() = 
        actor{
           let! message = clientMailbox.Receive()
           //Processing
           return! loop()
        }
    loop()

let clientParent (followerMap: Map<int,seq<int>>) (followingMap: Map<int,Set<int>>) (childMailbox: Actor<_>) = 
    for i in 1..N do
        let mutable foundFollower,valueFollower = followerMap.TryGetValue i
        let mutable foundFollowing, valueFollowing = followingMap.TryGetValue i
        spawn system (i|>string) (client i valueFollower valueFollowing)
    // let sref = select "akka://Fsharp/user/simulator" system
    // sref !<"SelectRandom"
    let rec loop() = 
        actor{
            return! loop()             
        }
    loop()


let simulator (numNodes:int) (mailbox : Actor<_>) = 
    let zipf = Zipf(1.2, numNodes)
    let array = Array.create numNodes 0 
    zipf.Samples(array)
    let mutable followerMap = Map.empty
    let mutable followingMap = Map.empty
    for i in 1..numNodes do
        followingMap <- followingMap.Add(i, Set.empty)
    for i in 0..numNodes-1 do 
        let data = [|1..numNodes|]
        let mutable seq = Seq.empty
        seq <- (data
        |> Seq.sortBy (fun _ -> rnd.Next())
        |> Seq.take array.[i])
        followerMap <- followerMap.Add((i+1), seq)
        for follower in seq do
            let mutable found, value = followingMap.TryGetValue follower
            value <- value.Add (i+1)
            followingMap <- followingMap.Add(follower, value)

    let rec loop() = actor{
        let! message = mailbox.Receive()
        match message with
        "SelectRandom"->
            let mutable count = 1
            while count<numNodes*10 do
                let randomNumber = (rnd.Next()%numNodes)+1
                let randomNumberString = randomNumber|>string
                let path = "akka://Fsharp/user/"+randomNumberString
                let sref = select path system
                sref<!SendTweet()
                count<-count+1
        return! loop()
    }
    loop()