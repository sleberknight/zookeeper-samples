jps -lm | grep JoinGroup | awk '{print $1}' | xargs kill
