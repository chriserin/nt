while true; do
    date '+%H:%M:%S'
    ps u -T -C 'nt'
    sleep "$1"
done
