#!/usr/bin/env bash
# setup_tc.sh  -- run this at container startup to configure tc

set -e

# 先删掉旧规则
tc qdisc del dev eth0 root 2>/dev/null || true
# 建 prio
tc qdisc add dev eth0 root handle 1: prio

tc qdisc add dev eth0 parent 1:1 handle 10: netem delay 0ms
tc qdisc add dev eth0 parent 1:2 handle 20: netem delay 25ms
tc qdisc add dev eth0 parent 1:3 handle 30: netem delay 50ms

# 根据容器里的 ENV 或 $HOSTNAME 决定是哪台，下面只是示例 node1 的
case "$ROLE" in
  # Coord=172.21.0.2
  # Node1=172.21.0.3，Node2=172.21.0.4，Node3=172.21.0.5
  # Client1=172.21.0.6，Client1=172.21.0.7，Client1=172.21.0.8
  coord)
    tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst 172.21.0.3/32 flowid 1:2
    tc filter add dev eth0 protocol ip parent 1:0 prio 2 u32 match ip dst 172.21.0.4/32 flowid 1:2
    tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst 172.21.0.5/32 flowid 1:2
    tc filter add dev eth0 protocol ip parent 1:0 prio 4 u32 match ip dst 172.21.0.6/32 flowid 1:2
    tc filter add dev eth0 protocol ip parent 1:0 prio 5 u32 match ip dst 172.21.0.7/32 flowid 1:2
    tc filter add dev eth0 protocol ip parent 1:0 prio 6 u32 match ip dst 172.21.0.8/32 flowid 1:2
    ;;
  node1)
    tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst 172.21.0.2/32 flowid 1:2
    tc filter add dev eth0 protocol ip parent 1:0 prio 2 u32 match ip dst 172.21.0.4/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst 172.21.0.5/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 4 u32 match ip dst 172.21.0.6/32 flowid 1:1
    tc filter add dev eth0 protocol ip parent 1:0 prio 5 u32 match ip dst 172.21.0.7/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 6 u32 match ip dst 172.21.0.8/32 flowid 1:3
    ;;
  node2)
    tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst 172.21.0.2/32 flowid 1:2
    tc filter add dev eth0 protocol ip parent 1:0 prio 2 u32 match ip dst 172.21.0.3/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst 172.21.0.5/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 4 u32 match ip dst 172.21.0.6/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 5 u32 match ip dst 172.21.0.7/32 flowid 1:1
    tc filter add dev eth0 protocol ip parent 1:0 prio 6 u32 match ip dst 172.21.0.8/32 flowid 1:3
    ;;
  node3)
    tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst 172.21.0.2/32 flowid 1:2
    tc filter add dev eth0 protocol ip parent 1:0 prio 2 u32 match ip dst 172.21.0.3/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst 172.21.0.4/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 4 u32 match ip dst 172.21.0.6/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 5 u32 match ip dst 172.21.0.7/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 6 u32 match ip dst 172.21.0.8/32 flowid 1:1
    ;;
  client1)
    tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst 172.21.0.2/32 flowid 1:2
    tc filter add dev eth0 protocol ip parent 1:0 prio 2 u32 match ip dst 172.21.0.3/32 flowid 1:1
    tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst 172.21.0.4/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 4 u32 match ip dst 172.21.0.5/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 5 u32 match ip dst 172.21.0.7/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 6 u32 match ip dst 172.21.0.8/32 flowid 1:3
    ;;
  client2)
    tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst 172.21.0.2/32 flowid 1:2
    tc filter add dev eth0 protocol ip parent 1:0 prio 2 u32 match ip dst 172.21.0.3/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst 172.21.0.4/32 flowid 1:1
    tc filter add dev eth0 protocol ip parent 1:0 prio 4 u32 match ip dst 172.21.0.5/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 5 u32 match ip dst 172.21.0.6/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 6 u32 match ip dst 172.21.0.8/32 flowid 1:3
    ;;
  client3)
    tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst 172.21.0.2/32 flowid 1:2
    tc filter add dev eth0 protocol ip parent 1:0 prio 2 u32 match ip dst 172.21.0.3/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst 172.21.0.4/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 4 u32 match ip dst 172.21.0.5/32 flowid 1:1
    tc filter add dev eth0 protocol ip parent 1:0 prio 5 u32 match ip dst 172.21.0.6/32 flowid 1:3
    tc filter add dev eth0 protocol ip parent 1:0 prio 6 u32 match ip dst 172.21.0.7/32 flowid 1:3
    ;;
  *)
    echo "Unknown ROLE: $ROLE" >&2
    exit 1
    ;;
esac

# 最后执行真正的命令
exec "$@"
