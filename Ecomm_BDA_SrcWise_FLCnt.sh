#set -x
#!/bin/sh/

#Script Name    : Ecomm_BDA_SrcWise_FLCnt.sh 
#Description    : This script search a file for a word. Append the aggregated count of records of similar type 
#		  at end of each record
#Owner          : eComm DS Dev Team
#Date Created   : 2018-09-13
#Usage          : sh Ecomm_BDA_SrcWise_FLCnt.sh Arg1
#		  where Arg1 is the file on which script has to search for a word and add the aggregated 
#		  counts to end of each record	
#==========================================  CHANGES START  ==================================================
#
#SL_No  Change_Ref#     Changed_By      Change_Date     Change_Comments
#=====  ===========     ==========      ===========     ===============
#
#
#
#==========================================  CHANGES END  ====================================================


Base_DNA_Path="/opt/ftps/ISSuite/IS/Data/DNA/"
Temp_Path=$Base_DNA_Path/TempFiles/

Attrib_File=`basename $1`
cut -f2 -d'|' $1 | sort | uniq > $Temp_Path/Agg_Wrd_Srch_FL.txt

sed -i '/^$/d' $Temp_Path/Agg_Wrd_Srch_FL.txt

echo -e "\n"Unique Table list is/are"\n"$a

cat $Temp_Path/Agg_Wrd_Srch_FL.txt

Iteration=1

while read line;
do

        echo -e "\n"Iteration $Iteration started for $line"\n"
        cnt=`grep -wc $line $1`

        echo -e count of source files received for source table $line is/are $cnt"\n"

        sed -i "/${line}/s/$/|${cnt}/" $1

        Iteration=`expr $Iteration + 1`

done < $Temp_Path/Agg_Wrd_Srch_FL.txt
