while true; do
  echo "$(date '+%H:%M:%S') | $(grep -i dirty /proc/meminfo | tr '\n' ' ')"
  sleep "$1"
done
