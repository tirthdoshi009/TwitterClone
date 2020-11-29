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
// open Akka.ActorSelection

let config =
    Configuration.parse
        @"akka {
                log-dead-letters = off
            }
        }"

type Input = Start 
            | RegisterUser of int
            | RegisterConfirm
            | Tweet of string*int
            | Live of string 
            | SubscriptionTweets of int
            | SubscriptionTweetsList of string list
            | HashtagTweets of string * int
            | HashtagTweetsList of string list
            | TweetsWithMention of int
            | MentionTweetsList of string list
            | GetTweetsForUser of int
            | TweetsForUserList of string list
            | AddSubscriber of int*int
            | DisconnectUser of int
            | LoginUser of int


let system = System.create "FSharp" (config)
let mutable terminate = true

let getHashTagMentions (text : string) = 
    let wordList = text.Split(' ') |> Array.toList
    let startsWithHashtag(word: string) = (word.[0] = '#')
    let hashtagList =
        List.choose(fun word -> 
                        match word with
                        | word when startsWithHashtag word -> Some(word)
                        | _-> None) wordList

    hashtagList

let getUserMentions (text: string) = 
    let wordList = text.Split(' ') |> Array.toList
    let startsWithUser(word: string) = (word.[0] = '@')
    let userList =
        List.choose(fun word -> 
                        match word with
                        | word when startsWithUser word -> Some(word)
                        | _-> None) wordList


    userList
let isValidClient(ClientList: Set<int>) (userId: int) = 
    ClientList.Contains userId

let server (mailbox : Actor<_>) = 
    let mutable clientList = Set.empty
    let mutable tweets = Map.empty
    let mutable hashtagMentions = Map.empty
    let mutable subscribedTo = Map.empty
    let mutable followers = Map.empty
    let mutable userMentions = Map.empty
    let rec loop() = actor{
        let! message = mailbox.Receive()
        let sender = mailbox.Sender()
        match message with
        | RegisterUser(userId) -> 
            clientList <- clientList.Add userId
            tweets <- tweets.Add (userId, List.empty)
            if subscribedTo.TryFind userId = None then
                subscribedTo <- subscribedTo.Add(userId, List.empty)
            if followers.TryFind userId = None then
                followers <- followers.Add(userId, List.empty) 
            sender <! RegisterConfirm
        | Tweet(text, userId) -> 
            if tweets.TryFind userId = None then
                tweets <- tweets.Add(userId, List.empty)
            let mutable _, tweetList = tweets.TryGetValue userId
            let mutable newList = List.empty
            tweetList <- List.append [text] tweetList
            tweets <- tweets.Add (userId, tweetList)
            let mutable hashtagList = getHashTagMentions text
            for hashtag in hashtagList do
                if hashtagMentions.TryFind hashtag = None then
                    hashtagMentions<-hashtagMentions.Add(hashtag,[])
                let mutable found,currentList = hashtagMentions.TryGetValue hashtag
                currentList<-List.append [text] currentList
                hashtagMentions<-hashtagMentions.Add(hashtag,currentList)
            let mutable mentionList = getUserMentions text
            for mention in mentionList do
                if userMentions.TryFind mention = None then
                    userMentions<-userMentions.Add(mention,[])
                let mutable found,currentList = userMentions.TryGetValue mention
                currentList<-List.append [text] currentList
                userMentions<-userMentions.Add(mention,currentList)
                let mutable mentionName = mention.[1..]
                let currentAnchor = system.ActorSelection("akka://FSharp/user/simulator/"+mentionName).Anchor
                let compare = currentAnchor.Equals(ActorRefs.Nobody)
                if compare then 
                    let mutable path = "akka://FSharp/user/simulator/"+mentionName
                    let mutable sref = select path system
                    sref <! Live(text)
                let mutable found,myFollowers = followers.TryGetValue userId
                for currentFollower in myFollowers do
                    let currentFollowerString = currentFollower |> string
                    let currentAnchor = system.ActorSelection("akka://FSharp/user/simulator/"+currentFollowerString).Anchor
                    let compare = currentAnchor.Equals(ActorRefs.Nobody)
                    if compare then 
                        let mutable path = "akka://FSharp/user/simulator/"+currentFollowerString
                        let sref = select path system
                        sref <! Live(text)
        | SubscriptionTweets(userId) -> 
            let mutable found, subscribeUsers = subscribedTo.TryGetValue userId
            let mutable subscribedTweetList = List.empty
            for subscribeUser in subscribeUsers do
                let mutable tweetsFound, tweetList = tweets.TryGetValue subscribeUser
                if tweetsFound then
                    subscribedTweetList <- List.append tweetList subscribedTweetList
            let mutable path = "akka://FSharp/user/simulator/"+ (userId |> string)
            let sref = select path system 
            sref <! SubscriptionTweetsList(subscribedTweetList)
        | HashtagTweets(hashtag, userId) ->
            let mutable hashtagFound, hashtagTweets = hashtagMentions.TryGetValue hashtag
            let mutable path = "akka://FSharp/user/simulator/"+ (userId |> string)
            let sref = select path system 
            // sref <! SubscriptionTweetsList(subscribedTweetList)f
            if hashtagFound then
                sref <! HashtagTweetsList(hashtagTweets)
            else 
                sref <! HashtagTweetsList(List.empty)
        | TweetsWithMention(userId) ->
            let mentionFound, mentionList = userMentions.TryGetValue ("@" + (userId |> string))
            let mutable path = "akka://FSharp/user/simulator/"+ (userId |> string)
            let sref = select path system 
            if mentionFound then
                sref <! MentionTweetsList(mentionList)
            else 
                sref <! MentionTweetsList(List.empty)
        | GetTweetsForUser(userId) -> 
            let tweetsFound, tweetsForUser = tweets.TryGetValue userId
            let mutable path = "akka://FSharp/user/simulator/"+ (userId |> string)
            let sref = select path system 
            if tweetsFound then
                sref<!TweetsForUserList(tweetsForUser)
            else
                sref<!TweetsForUserList(List.empty)
        | AddSubscriber(userId,subscriberId) ->
            let mutable subscriberFound, subscriberList = subscribedTo.TryGetValue userId
            subscriberList<-List.append [subscriberId] subscriberList
            subscribedTo<-subscribedTo.Add(userId,subscriberList)
            let mutable followerFound, followerList = followers.TryGetValue subscriberId
            followerList<-List.append [userId] followerList
            followers<-followers.Add(subscriberId,followerList)
        | DisconnectUser(userId)->
            clientList<-clientList.Remove userId
        | LoginUser(userId) ->
            clientList<-clientList.Add userId
        return! loop()
    }
    loop()

let application (userCount:int) (disconnectCount:int) (subscriberCounts: int array) (mailbox : Actor<_>) = 
    let mutable convergedCount = 0
    let mutable tweetTimeDiff = 0
    let mutable subscribeQueryDiff = 0
    let mutable hashtagQueryDiff = 0
    let mutable mentionQueryDiff = 0
    let mutable selfTweetsQueryDiff = 0
    let rec loop() = actor{
        let! message = mailbox.Receive()
        let sender = mailbox.Sender()
        match message with
        | Start -> 
            spawn mailbox "TwitterServer" server
            for i in 1..userCount do
                spawn mailbox (i |> string) (client )
        return! loop()
    }
    loop()

let simulate (userCount:int) (disconnectCount:int) (subscriberCounts:int array)= 
    printfn "Starting Pastry protocol for userCount = %i disconnectCount = %i" userCount disconnectCount
    let stopWatch = System.Diagnostics.Stopwatch.StartNew()
    let parentRef = spawn system "master" (application userCount disconnectCount subscriberCounts)
    parentRef <! Start
    while terminate do
        ignore
    stopWatch.Stop()
    printfn "Total time elapsed is given by %f milliseconds" stopWatch.Elapsed.TotalMilliseconds

let main() = 
    let userCount = int fsi.CommandLineArgs.[1] |> int
    let disconnectPercentage = fsi.CommandLineArgs.[2] |> int
    printfn "userCount=%i disconnectPercentage=%i" userCount disconnectPercentage
    let disconnectCount = int(0.01*float(disconnectPercentage)*float(userCount))
    let zipf = Zipf(1.2, userCount)
    let subscriberCounts = Array.create userCount 0 
    zipf.Samples(subscriberCounts)
    simulate userCount disconnectCount subscriberCounts

main()

