#!/bin/bash

#Partnered with Michael Edelman

#Constant Variables:
background_results=""
gossip_time=2.0
valid_code="public class Test{ public Test(){} public String run(){ return \"hello world\";}}"
gatewayId=7
gatewayPort=8070

#Assuming bash v4+
#server ID -> PID Map:
declare -A server_to_pid
#list of curl background tasks to wait for
curl_request_pids=()

compile_code () {
  cd src/main/java || exit
  javac edu/yu/cs/com3800/stage5/ZooKeeperPeerServerImpl.java
}
 create_server () {
   #$1 = serverID
   #$2 = port
   #$3 = isGateway
  java edu/yu/cs/com3800/stage5/ZooKeeperPeerServerImpl "$2" 1 "$1" "$3" &
  pid_var=$!
  server_to_pid[$1]=$pid_var
}
run_mvn_tests () {
  #for some reason, maven keeps trying to run each test in parallel. This tries to create multiple servers on the same
  #port, which is a failure. Unfortunately, this forces us to run each test individually
  mvn clean -Dtest=Stage5GossipTest#gossipTimerIsUpdated test
  mvn -Dtest=Stage5GossipTest#nodesDoNotArbitrarilyDie test
  mvn -Dtest=Stage5GossipTest#deadNodeDetectedAfterFullCleanup test
  mvn -Dtest=Stage5GossipTest#deadNodeMarkedBeforeDeleted test
  mvn -Dtest=Stage5GossipTest#gossipHttpTest test
  mvn -Dtest=Stage5GossipTest#clusterWorksAfterDeadWorkerCleanedUp test
  mvn -Dtest=Stage5GossipTest#clusterWorksAfterDeadWorkerMarkedNotYetCleaned test
  mvn -Dtest=Stage5GossipTest#clusterWorksAfterWorkerDeadNotYetMarked test
  mvn -Dtest=Stage5GossipTest#clusterWorksAfterTwoDeadWorkersCleanedUp test
  mvn -Dtest=Stage5GossipTest#clusterWorksAfterTwoDeadWorkerMarkedNotYetCleaned test
  mvn -Dtest=Stage5GossipTest#clusterWorksAfterTwoWorkersDeadNotYetMarked test
  mvn -Dtest=Stage5GossipTest#clusterWorksAfterDeadLeaderCleanedUp test
  mvn -Dtest=Stage5GossipTest#clusterWorksAfterDeadLeaderMarkedNotYetCleaned test
  mvn -Dtest=Stage5GossipTest#clusterWorksAfterLeaderDeadNotYetMarked test

  mvn -Dtest=Stage5HttpClientTest#singleRequestValidCode test
  mvn -Dtest=Stage5HttpClientTest#singleRequestIncorrectCode test
  mvn -Dtest=Stage5HttpClientTest#electLeaderAndEvaluateMultipleCorrectCode test
  mvn -Dtest=Stage5HttpClientTest#electLeaderAndEvaluateMultipleIncorrectCode test
  mvn -Dtest=Stage5HttpClientTest#electLeaderAndEvaluateActualNodeCountCorrectCode test
  mvn -Dtest=Stage5HttpClientTest#electLeaderAndEvaluateActualNodeCountIncorrectCode test
  mvn -Dtest=Stage5HttpClientTest#electLeaderAndEvaluateMoreThanNodeCountCorrectCode test
  mvn -Dtest=Stage5HttpClientTest#electLeaderAndEvaluateMoreThanNodeCountIncorrectCode test
}

 wait_election_completed () {
     #1 = port
     #blocks until there is a leader

     echo "Waiting until election is over"
     http_gossip_port=$(($1 + 1))
     election_completed=0
     while [ $election_completed -ne 1 ]; do
         #sleep a little to slow down
         sleep .5
         echo "checking on election"

         result=$(send_request $http_gossip_port 'getserverstatus')
         substring='=LEADING'
          if grep -q "$substring" <<< "$result"; then
            election_completed=1
            break
          fi
     done
     echo "Election complete"
   }
  get_gossip_info () {
      #1 = port
      #prints out gossip list
      http_gossip_port=$(($1 + 1))
      echo "Trying to get result of Gossip"
      local result=$(send_request $http_gossip_port 'getgossipinfo')
      sleep 2
      echo "$result"
    }
 get_server_status () {
      #1 = port
      #prints out gossip list
      http_gossip_port=$(($1 + 1))
      local result=$(send_request $http_gossip_port 'getserverstatus')
      sleep .5
      echo "$result"
  }

 get_leader_id () {
       #1 = port
      regex="([0-9])=LEADING"
      test_str=$(send_request $http_gossip_port 'getserverstatus')
      [[ $test_str =~ $regex ]]
      echo "Current leader: ${BASH_REMATCH[1]}"
 }
 kill_server () {
   #1 = id
   echo "Killing server id $1"
   pid="${server_to_pid[$1]}"
   kill -9 "$pid"
   echo "Killed pid $pid"
 }
 send_request () {
    #1 = port
    #2 = context
    #returns whatever curl returns
    # shellcheck disable=SC2155
    local result=$(curl --max-time 60 -s -d "$valid_code" -H "Content-Type: text/x-java-source" -X GET "http://localhost:$1/$2")
    echo "$result"
  }
   send_request_silent () {
      #1 = port
      #2 = context
      #returns whatever curl returns
      # shellcheck disable=SC2155
      local result=$(curl --max-time 60 -s -o /dev/null -d "$valid_code" -H "Content-Type: text/x-java-source" -X GET "http://localhost:$1/$2")
      echo "$result"
    }
 send_request_background () {
     #1 = port
     #2 = context
     send_request "$1" "$2" &
     curl_request_pids+=($!)
     #Need to sleep for a little bit so the pid is added to array before wait_requests_done
     #is called. Race condition...
     sleep .5
   }
 wait_requests_done () {
   for i in "${curl_request_pids[@]}"
      do
        echo "Waiting for curl pid $i to finish"
        wait "$i"
     done
 }

 ## STEPS ##

{
 #STEP 1 - Build code using mvn test
 # Given the long time to detecting node failure, this section may take up to 30 minutes. To reduce testing time,
 # reduce the gossip time
 echo "To adjust logging, look at the variable GossipServer.printGossipInfo"
run_mvn_tests
  #Pre-build:
  compile_code
  #This needs to be done separately because otherwise can't run main method

 #STEP 2 - Create 7 nodes and 1 gateway, starting each in array separate JVM
 for ((  i = 0;  i < 7;  i++ )); do
   (( port = 8000 + i * 10))
     create_server "$i" "$port" false
 done
 create_server $gatewayId $gatewayPort true
 #STEP 3 -
 #      a) Wait until election is completed
 wait_election_completed $gatewayPort
 #      b) If gateway has array leader, it should respond with the full list of nodes and their roles. Print out server ID and role
 get_server_status $gatewayPort
 #STEP 4 - Send 9 client requests, and print out both request and result. Wait until all are done
 for ((  i = 0;  i < 9;  i++ )); do
      echo "Sending request $valid_code"
      result=$(send_request "$gatewayPort" 'compileandrun')
      echo "Returned $result"
  done
 #STEP 5 -
 #      a) kill -9 a follower JVM, printing out which one you are killing.
 #before killing server, save the gossip log:
 send_request_silent 3 'getgossipinfo'
 kill_server 3

 #      b) Wait heartbeat interval * 10 time
 time_to_wait=$(echo "$gossip_time*11.0" | bc)
 echo "Waiting $time_to_wait"
 sleep "$time_to_wait"
 #      c) and then retrieve and display the list of nodes. The dead node should not be on the list
 get_server_status $gatewayPort
 result=$(get_server_status $gatewayPort)
 echo "$result"
 substring=' 3='
 if grep -q "$substring" <<< "$result"; then
   echo "Found server that was supposed to be dead. Test failed..."
   else echo "Did not find failed server. Success..."
 fi
 #STEP 6 -
 #      a) kill -9 the leader JVM and wait 1000ms
send_request_silent 6 'getgossipinfo'
kill_server 6

 sleep 1
 #      b) send/display 9 more client requests to the gateway, in the background
 for ((  i = 0;  i < 9;  i++ )); do
       send_request_background "$gatewayPort" 'compileandrun'
   done
 #STEP 7 -
 #      a) wait until gateway has a new leader and print out its ID.
 wait_election_completed $gatewayPort
 get_leader_id $gatewayPort
 #      b) Print out the responses from step 6b. Do not continue until all 9 requests have received responses
 wait_requests_done
 echo "$background_results"
 #STEP 8 - Send/display 1 more client request (in foreground), print response
last_request_result=$(send_request "$gatewayPort" 'compileandrun')
echo "Result $last_request_result"
 #STEP 9 - list the paths to the files containing Gossip messages received by each node
 for ((  i = 0;  i < 7;  i++ )); do
   (( port = 8001 + i * 10))
     send_request "$port" 'getgossipinfo'
 done
 echo "The gossip messages are found in the pwd $(pwd) in the format: Server[id]GossipArchiveAt[time]"
 echo "The complete log is found in the format: output.txt"
 #STEP 10 - shutdown all the nodes
 for ((  i = 0;  i < 8;  i++ )); do
    kill_server "$i"
  done
  } | tee src/main/java/output.txt