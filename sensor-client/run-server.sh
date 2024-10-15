#!/bin/sh

me=`realpath $0`
me_dir=`dirname $me`
tracker_exe="python3 $me_dir/cpu-mem-tracker.py"

runner=$1
shift

runner_dir=`dirname $runner`
runner_exe="./$(basename $runner)"

cd "$runner_dir"
$runner_exe &
runner_pid=$!
echo "Started server with PID $runner_pid"

cd "$me_dir"
$tracker_exe $runner_pid &
tracker_pid=$!
echo "Started tracker with PID $tracker_pid"

scala-cli run App.scala --main-class runner -- http://localhost:8044 $@ >> run-server.log

echo "Done sending, waiting..."
sleep 10

echo "Stopping server..."
kill $runner_pid
wait $runner_pid

echo "Server stopped. Joining tracker..."
wait $tracker_pid

cd "$runner_dir"
echo "Logged $(wc -l log.txt) lines"
rm log.txt

cd "$me_dir"
tdir=`mktemp -d ./logs-XXXXXX`
mv trace-$runner_pid.csv $tdir/
mv sensorstats*.csv $tdir/
echo "$@" >> $tdir/config
echo "Saved stats and trace to $tdir"

echo "Done."

