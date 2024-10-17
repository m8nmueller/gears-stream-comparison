import time
import psutil
from datetime import datetime
import sys

procid=int(sys.argv[1])
outfile=f"trace-{procid}.csv"

# mem=pmem(rss=968753152, vms=425584623616, pfaults=86408, pageins=3244) cpu=pcputimes(user=25.216069632, system=4.269703936, children_user=0.0, children_system=0.0) at 1728814054477

p = psutil.Process(procid)
with open(outfile, "w") as out:
  print(f"[Tracker] Tracing {procid} to {outfile}")
  out.write("time,rss,vms,user,system\n")
  while psutil.pid_exists(procid):
    date = datetime.now()
    mem = p.memory_info()
    cpu = p.cpu_times()
    date = int(datetime.timestamp(date) * 1000)

    out.write(f"{date},{mem.rss},{mem.vms},{cpu.user},{cpu.system}\n")
    time.sleep(0.5)
  print("[Tracker] Done. Flushing trace...")

