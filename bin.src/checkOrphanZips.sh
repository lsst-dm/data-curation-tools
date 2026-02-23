#!/bin/sh

me=$(readlink -f $0)
mydir=$(dirname $me)
mystem=$(echo $me | awk -F\. '{print $1}')
mystem=$(basename $mystem)

YYYYMMDD=$1
Fexist=✅
FnoExt=❌

USDFrawROOT=/sdf/data/rubin/lsstdata/offline/instrument/LSSTCam

rucioDsList=$(mktemp -p /tmp $mystem.rucioDsList.XXXX)
rucioFnList=$(echo $rucioDsList | sed -e 's/rucioDsList/rucioFnList/g')

rucio did list --filter "type=dataset" --short raw:Dataset/LSSTCam/raw/Obs/$YYYYMMDD* > $rucioDsList
rucio did list --filter "type=file" --short raw:LSSTCam/$YYYYMMDD* > $rucioFnList

find $USDFrawROOT/$YYYYMMDD* -name "*.zip" | while read zip; do
  expoStr=$(echo $zip | sed -e "s+$USDFrawROOT++g; s+^/++g; s+\.zip++g")
  dimensionyaml=$(echo $zip | sed -e 's+\.zip+_dimensions\.yaml+g')

  echo -n "$expoStr : "
  grep zip $rucioFnList | grep -q $expoStr
  if [ $? -eq 0 ]; then
    echo -n "Skip checking Butler, zip in Rucio $Fexist "
  else
    expoid_p1=$(echo $expoStr | awk -F\/ '{print $1}')
    expoid_p2=$(echo $expoStr | cut -c25-)
    #echo "expoStr = $expoStr : p1 = $expoid_p1 : p2= $expoid_p2"
    expoid="$expoid_p1$expoid_p2"
    ndetectors=$(butler query-datasets main '*' --collections LSSTCam/raw/all --where \
                   "instrument='LSSTCam' and exposure=$expoid" | grep -c LSSTCam/raw/all)
    ndetectors=$(printf "%4d" $ndetectors)
    echo -n "$ndetectors dets  in Bulter, zip in Rucio $FnoExt "
  fi

  if [ -f $dimensionyaml ]; then
    echo -n "yaml exist $Fexist "
  else
    echo -n "yaml exist $FnoExt "
  fi

  grep yaml $rucioFnList | grep -q $expoStr
    if [ $? -eq 0 ]; then
    echo -n "in Rucio $Fexist "
  else
    echo -n "in Rucio $FnoExt "
  fi

  grep -q $expoStr $rucioDsList
  if [ $? -eq 0 ]; then
    echo "Obs dataset in Rucio $Fexist "
  else
    echo "Obs dataset in Rucio $FnoExt "
  fi
done #| grep $FnoExt

rm $rucioDsList $rucioFnList
