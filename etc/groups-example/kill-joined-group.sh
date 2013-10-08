jps -lm | grep JoinGroup | grep $1 | awk '{print $1}' | xargs kill
