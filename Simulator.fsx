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

// TODO: Set Remote configuration
let config =
    Configuration.parse
        @"akka {
                log-dead-letters = on
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
            | Initialize of bool
            | PerformanceMetrics of float*float*float*float*float
            | Test of string

// TODO: Figure out what happens when we use remote actors... Then two actor systems are there, how will we manage it?
let system = System.create "FSharp" (config)
// let serverActor = spawn system "serverActor" server 
let mutable terminate = true
let rnd = Random()

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
    printfn "Server Started!"
    let mutable clientList = Set.empty
    let mutable tweets = Map.empty
    let mutable hashtagMentions = Map.empty
    let mutable subscribedTo = Map.empty
    let mutable followers = Map.empty
    let mutable userMentions = Map.empty
    let rec loop() = actor{
        let! message = mailbox.Receive()
        printfn "Message = %A" message
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
                printfn "mention = %s mentionName = %s mentionName length = %d " mention mentionName mentionName.Length
                // "akka://FSharp/user/simulator/"+ (i|> string) 
                let currentAnchor = system.ActorSelection("akka://FSharp/user/"+(1|>string)).Anchor
                let compare = currentAnchor.Equals(ActorRefs.Nobody)
                printfn "User id validity = %b Does actor exist? = %b" (isValidClient clientList userId) compare
                if (isValidClient clientList userId) then 
                    let mutable path = "akka://FSharp/user/"+mentionName
                    let mutable sref = select path system
                    printfn "Path of actor with userid = %d is %s" userId path
                    sref <! Live(text)
                    printfn "Live message sent to actor %s" mentionName
                let mutable found,myFollowers = followers.TryGetValue userId
                for currentFollower in myFollowers do
                    let currentFollowerString = currentFollower |> string
                    // let currentAnchor = system.ActorSelection("akka://FSharp/user/simulator/"+currentFollowerString).Anchor
                    // let compare = currentAnchor.Equals(ActorRefs.Nobody)
                    if (isValidClient clientList userId) then 
                        let mutable path = "akka://FSharp/user/"+currentFollowerString
                        let sref = select path system
                        sref <! Live(text)
        | SubscriptionTweets(userId) -> 
            let mutable found, subscribeUsers = subscribedTo.TryGetValue userId
            let mutable subscribedTweetList = List.empty
            for subscribeUser in subscribeUsers do
                let mutable tweetsFound, tweetList = tweets.TryGetValue subscribeUser
                if tweetsFound then
                    subscribedTweetList <- List.append tweetList subscribedTweetList
            if (isValidClient clientList userId) then  
                // let mutable path = "akka://FSharp/user/"+ (userId |> string)
                // let sref = select path system 
                // sref <! SubscriptionTweetsList(subscribedTweetList)
                sender <! SubscriptionTweetsList(subscribedTweetList)
        | HashtagTweets(hashtag, userId) ->
            if (isValidClient clientList userId) then 
                let mutable hashtagFound, hashtagTweets = hashtagMentions.TryGetValue hashtag
                let mutable path = "akka://FSharp/user/"+ (userId |> string)
                let sref = select path system 
                // sref <! SubscriptionTweetsList(subscribedTweetList)f
                if hashtagFound then
                    sender <! HashtagTweetsList(hashtagTweets)
                else 
                    sender <! HashtagTweetsList(List.empty)
        | TweetsWithMention(userId) ->
            if (isValidClient clientList userId) then 
                let mentionFound, mentionList = userMentions.TryGetValue ("@" + (userId |> string))
                let mutable path = "akka://FSharp/user/"+ (userId |> string)
                let sref = select path system 
                if mentionFound then
                    sender <! MentionTweetsList(mentionList)
                else 
                    sender <! MentionTweetsList(List.empty)
        | GetTweetsForUser(userId) -> 
            if (isValidClient clientList userId) then 
                let tweetsFound, tweetsForUser = tweets.TryGetValue userId
                let mutable path = "akka://FSharp/user/"+ (userId |> string)
                let sref = select path system 
                if tweetsFound then
                    sender<!TweetsForUserList(tweetsForUser)
                else
                    sender<!TweetsForUserList(List.empty)
        | AddSubscriber(userId,subscriberId) ->
            let mutable subscriberFound, subscriberList = subscribedTo.TryGetValue userId
            subscriberList<-List.append [subscriberId] subscriberList
            subscribedTo<-subscribedTo.Add(userId,subscriberList)
            let mutable followerFound, followerList = followers.TryGetValue subscriberId
            followerList<-List.append [userId] followerList
            followers<-followers.Add(subscriberId,followerList)
            printfn "AddSubscriber is done! for Userid %d subscriberId %d" userId subscriberId 
        | DisconnectUser(userId)->
            clientList<-clientList.Remove userId
        | LoginUser(userId) ->
            clientList<-clientList.Add userId
        | Test(text) -> printfn "Text received %s" text
        return! loop()
    }
    loop()

let serverActor = spawn system "serverActor" server 
let path = "akka://FSharp/user/serverActor"
let userServer = select path system
userServer<! Test("Hello Message")
// serverActor<! Test("Hello message")
// let compare = currentAnchor.Equals(ActorRefs.Nobody)
// printfn "Does server actor exist? = %b"  compare

let client (userId: int) (numOfTweets: int) (numToSubscribe: int) (mailbox : Actor<_>) = 
    // TODO : play around with subscriber and tweets count
    // TODO: Declare variables
    let rec loop() = actor{
        let! message = mailbox.Receive()
        printfn "Message = %A" message
        let sender = mailbox.Sender()
        match message with
        | Initialize(existingUser) -> 
            if existingUser then
                printfn "User %d is connected" userId
                serverActor <! LoginUser(userId)
                for i in 1..5 do
                    let text ="user " + (userId |> string) + "is tweeting that dos is a great course"  
                    serverActor <! Tweet(text, userId)
            let task = (serverActor <? RegisterUser(userId))
            let response = Async.RunSynchronously(task, 5000)
            printfn "User %d is registered!" userId
            if numToSubscribe > 0 then
                for subscriberId in 1..numToSubscribe do 
                    serverActor <! AddSubscriber(userId, subscriberId)
            let stopWatch = System.Diagnostics.Stopwatch.StartNew()
            // Mention
            let userToMention =  rnd.Next() % userId + 1
            let text = "User " + (userId |> string) + " Tweeting that @" + (userToMention |> string) 
            serverActor <! Tweet(text, userId)
            // Hashtag
            let text = "User " + (userId |> string) + " Tweeting that #COP5615 is a great course" 
            serverActor <! Tweet(text, userId)
            // Send tweets
            for i in 1..numOfTweets do
                let text ="user " + (userId |> string) + "is tweeting that dos is a great course"  
                serverActor <! Tweet(text, userId)
            // Handle retweets
            printfn "Awaiting response"
            let task = (serverActor <? SubscriptionTweets(userId))
            let taskResponse: Input = Async.RunSynchronously(task, 5000) 
            printfn "Got response"
            match taskResponse with 
            | SubscriptionTweetsList(list) -> 
                if not list.IsEmpty then
                    let text = list.Head
                    serverActor <! Tweet(text + " -RT", userId)
            stopWatch.Stop()
            let mutable tweetsTimeDifference = stopWatch.Elapsed.TotalMilliseconds
            // Queries
            let querySubscribedToStopWatch = System.Diagnostics.Stopwatch.StartNew()
            // Handle query subscribed to
            // What do I need to do -> 
            let queryTask = serverActor <? GetTweetsForUser(userId)
            let queryResponse: Input = Async.RunSynchronously(queryTask, 5000)
            match queryResponse with
                | TweetsForUserList(list) -> 
                    if not list.IsEmpty then
                        printfn "Tweets subscribed by userId : %d are %A" userId list
            querySubscribedToStopWatch.Stop()
            let querySubscribedToTimeDifference = querySubscribedToStopWatch.Elapsed.TotalMilliseconds
            let queryHashTagStopWatch = System.Diagnostics.Stopwatch.StartNew()
            let queryHashTagTask = serverActor <? HashtagTweets("#COP5615",userId)
            let queryHashTagResponse: Input = Async.RunSynchronously(queryHashTagTask, 5000)
            match queryHashTagResponse with
            | HashtagTweetsList(list) ->
                if not list.IsEmpty then
                    printfn "The tweets for the #COP5615 by userId %d is %A " userId list
            queryHashTagStopWatch.Stop()
            let queryHashTagElapsedTime = queryHashTagStopWatch.Elapsed.TotalMilliseconds

            //Mentions 
            
            let queryMentionsStopWatch = System.Diagnostics.Stopwatch.StartNew()
            let queryMentionsTask = serverActor <? TweetsWithMention(userId)
            let queryMentionsResponse: Input = Async.RunSynchronously(queryMentionsTask, 5000)
            match queryMentionsResponse with
            | MentionTweetsList(list) ->
                if not list.IsEmpty then
                    printfn "The tweets mentioning userId %d are %A" userId list
            queryMentionsStopWatch.Stop()
            let queryMentionsElapsedTime = queryHashTagStopWatch.Elapsed.TotalMilliseconds
            // Get all your own tweets
            let queryOwnTweetsStopWatch = System.Diagnostics.Stopwatch.StartNew()
            let queryOwnTweetsTask = serverActor <? GetTweetsForUser(userId)
            let queryOwnTweetsResponse: Input = Async.RunSynchronously(queryOwnTweetsTask, 5000)
            match queryOwnTweetsResponse with
            | TweetsForUserList(list) ->
                if not list.IsEmpty then
                    printfn "The tweets feed for the user %d are %A" userId list
            queryOwnTweetsStopWatch.Stop()
            let queryOwnTweetsElapsedTime = queryOwnTweetsStopWatch.Elapsed.TotalMilliseconds

            // Get tweet time difference
            let mutable averageTweetTimeDifference = float(tweetsTimeDifference)/float(3)
            // tweetTimeDiff
            let simulatorRef = select ("akka://FSharp/user/simulator") system 
            simulatorRef <! PerformanceMetrics(averageTweetTimeDifference,querySubscribedToTimeDifference,queryHashTagElapsedTime,queryMentionsElapsedTime,queryOwnTweetsElapsedTime)
        | Live(text) ->
            printfn "New Tweet received by %d. Tweet text is given by %s" userId text
        | _ -> ignore
        return! loop()
    }
    loop()

let clientSystem (userCount:int) (disconnectCount:int) (subscriberCounts: int) (mailbox : Actor<_>) = 
    let mutable convergedCount = 0
    let mutable tweetTimeDiff = float(0)
    let mutable subscribeQueryDiff = float(0)
    let mutable hashtagQueryDiff = float(0)
    let mutable mentionQueryDiff = float(0)
    let mutable selfTweetsQueryDiff = float(0)
    let rec loop() = actor{
        let! message = mailbox.Receive()
        printfn "Message = %A" message
        let sender = mailbox.Sender()
        match message with
        | Start -> 
            let zipf = Zipf(1.2,userCount)
            let mutable array = Array.create userCount 0 
            zipf.Samples(array)
            printfn "Zipf distribution = %A" array
            for i in 1..userCount do
                spawn system (i |> string) (client i (int(userCount/i)) array.[i-1])
            for i in 1..userCount do
                let mutable path = "akka://FSharp/user/"+ (i|> string) 
                let iActorRef = select path system
                printfn "iActorRefPath = %s" iActorRef.PathString
                iActorRef <! Initialize(false) 
        | PerformanceMetrics(averageTweetTimeDifference,querySubscribedToTimeDifference,queryHashTagElapsedTime,queryMentionsElapsedTime,queryOwnTweetsElapsedTime) ->
            convergedCount <- convergedCount + 1
            tweetTimeDiff <- averageTweetTimeDifference + tweetTimeDiff 
            subscribeQueryDiff <- querySubscribedToTimeDifference + subscribeQueryDiff
            hashtagQueryDiff <- hashtagQueryDiff + queryHashTagElapsedTime
            mentionQueryDiff <- mentionQueryDiff + queryMentionsElapsedTime
            selfTweetsQueryDiff<- queryOwnTweetsElapsedTime + selfTweetsQueryDiff
            if convergedCount = userCount then
                printfn "Final Metrics: "
                printfn "Avg. time to tweet: %f milliseconds" (float(tweetTimeDiff/float(userCount)))
                printfn "Avg. time to query tweets subscribe to: %f milliseconds" (float(subscribeQueryDiff/float(userCount)))
                printfn "Avg. time to query tweets by hashtag: %f milliseconds" (float(hashtagQueryDiff/float(userCount)))
                printfn "Avg. time to query tweets by mention: %f milliseconds" (float(mentionQueryDiff/float(userCount)))
                printfn "Avg. time to query all relevant tweets: %f milliseconds" (float(mentionQueryDiff/float(userCount))) 
                terminate <- false 
        return! loop()
    }
    loop()

let simulate (userCount:int) (disconnectCount:int) (maxSubscriberCount:int)= 
    printfn "Starting Pastry protocol for userCount = %i disconnectCount = %i" userCount disconnectCount
    let stopWatch = System.Diagnostics.Stopwatch.StartNew()
    let parentRef = spawn system "simulator" (clientSystem userCount disconnectCount maxSubscriberCount)
    parentRef <! Start
    while terminate do
        ignore
    stopWatch.Stop()
    printfn "Total time elapsed is given by %f milliseconds" stopWatch.Elapsed.TotalMilliseconds

let main() = 
    if fsi.CommandLineArgs.Length = 0 then
        spawn system "serverActor" server
        ()
    else
        // spawn system "serverActor" server 
        // let userCount = fsi.CommandLineArgs.[1] |> int
        let userCount = 100
        let maxSubscriberCount = 20
        let disconnectPercentage = 10
        // let maxSubscriberCount = fsi.CommandLineArgs.[2] |> int 
        // let disconnectPercentage = fsi.CommandLineArgs.[3] |> int
        printfn "userCount=%i maxSubscriberCount = %i disconnectPercentage=%i" userCount maxSubscriberCount disconnectPercentage
        let disconnectCount = int(0.01*float(disconnectPercentage)*float(userCount))
        // let subscriberCounts = Array.create userCount 0 
        // zipf.Samples(subscriberCounts)
        simulate userCount disconnectCount maxSubscriberCount

main()

