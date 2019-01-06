#set -x
#! /bin/sh

#Script Name    : DSJob_Prcs_Status.sh
#Description    : This script captures the status of Datastage job 
#Owner          : eComm DS Dev Team
#Date Created   : 2018-09-13
#Usage          : sh DSJob_Prcs_Status.sh Arg1 Arg2 
#		  where Arg1 is job name for which status is required and Arg2 is sleep interval in seconds 
#==========================================  CHANGES START  ==================================================
#
#SL_No  Change_Ref#     Changed_By      Change_Date     Change_Comments
#=====  ===========     ==========      ===========     ===============
#
#
#
#==========================================  CHANGES END  ====================================================

sleep 3

Sleep_Interval=$1 # read the argument which refers to #seconds the process should sleep before it checks whether BDA ingestion jobs are completed for a batch or not as part of BDA ingestion process

echo -e "\n"Sleep interval to validate a particular job is completed or not, in order to process next steps is $Sleep_Interval seconds"\n"

DSJob_Prcs_Cnt=`ps -ef | grep "phantom DSD.RUN" | grep $2 | wc -l`

echo `ps -ef | grep "phantom DSD.RUN" | grep $2`

echo -e "\n"Currently there is/are $DSJob_Prcs_Cnt Datastage BDA ingestion jobs running on $(hostname)"\n"

while [ $DSJob_Prcs_Cnt -gt 0 ]
do
   
   echo -e process is going to sleep for $Sleep_Interval seconds before, we check for the Datastage BDA ingestion jobs running on $(hostname) or not"\n"

   sleep $Sleep_Interval

   DSJob_Prcs_Cnt=`ps -ef | grep "phantom DSD.RUN" | grep $2 | wc -l`

   echo -e After sleeping for $Sleep_Interval seconds, Currently there is/are $DSJob_Prcs_Cnt Datastage BDA ingestion jobs running on $(hostname)"\n"

done
