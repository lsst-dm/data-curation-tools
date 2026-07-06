#!/bin/sh

# input: rucio Obs dataset name, one per line
#   e.g. raw:Dataset/LSSTCam/raw/Obs/20251101/MC_O_20251101_000035

source /cvmfs/sw.lsst.eu/almalinux-x86_64/lsst_distrib/w_2026_22/loadLSST.sh
setup obs_lsst

prune_n_delete () {
  exposure=$1
  dirs=$2

  if [ -z "$exposure" -o -z "$dirs" ]; then
    echo \$exposure and/or \$dirs are empty. Dangerous! Quit !
    exit
  fi

  echo "butler prune-datasets $exposure"
  butler prune-datasets --where "instrument='LSSTCam' and exposure=$exposure" \
                        --no-confirm --quiet \
                        --datasets raw --unstore --purge \
                        LSSTCam/raw/all embargo 2> /dev/null
  echo "  delete $dirs"
  mc rm -r --force --quiet embargo_rw/rubin-summit/LSSTCam/$dirs > /dev/null
}

n=0
nMax=10
while read ds; do
  echo $ds | awk -F\/ '{print $NF}' | grep -q ^MC
  if [ ! $? -eq 0 ]; then
    echo $ds: wrong dataset name
    exit
  else
    exposure=$(echo $ds | awk -F\/ '{print $NF}' | sed -e 's/MC_._//g; s/_0//g')
    dirs=$(echo $ds | sed -e 's+raw:Dataset/LSSTCam/raw/Obs/++g')
  fi
  if [[ ! $exposure =~ ^[0-9]{13}$ ]]; then
    echo "$exposure: wrong exposure format"
    exit
  fi
  prune_n_delete $exposure $dirs &
  ((n=n+1))
  if [ $n -ge $nMax ]; then
    wait 
    n=0
  fi
done
wait
