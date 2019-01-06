#set -x
#!/bin/sh

#Script Name	: Ecomm_BDA_Pre_Valdn.sh 
#Description	: This script search the ecomm source file path for ecomm files and 
#		  report only the files which has schema, stats & data files with data
#Owner		: eComm DS Dev Team
#Date Created	: 2018-09-13
#Usage		: sh Ecomm_BDA_Pre_Valdn.sh Arg1 Arg2 
#		  where Arg1 is the ecomm source file path & Arg2 is the source system name ie ecomm
#
#==========================================  CHANGES START  ==================================================
#
#SL_No	Change_Ref#	Changed_By	Change_Date	Change_Comments
#=====	===========	==========	===========	===============
#
#
#
#==========================================  CHANGES END  ====================================================


Base_DNA_Path="/opt/ftps/ISSuite/IS/Data/DNA/"
Temp_Path=$Base_DNA_Path/TempFiles/
Script_Path=$Base_DNA_Path/ScriptFiles/

Script_Nm=`basename $0 | cut -f1 -d'.'`
DateTs_Strt=`date +%Y-%m-%d_%H-%M-%S_%N`

Log_File=${Script_Nm}_$DateTs_Strt.log
Err_Log_File=${Script_Nm}_$DateTs_Strt.err

echo -e "\n"Ecomm source file search process Script $Script_Nm started at $DateTs_Strt"\n"

it_cnt=$3
Ecomm_Src_Path=$1
Ecomm_Src_Sys_Nm=$2
Ecomm_Full_Path=$Ecomm_Src_Path/$Ecomm_Src_Sys_Nm/
Pfx_Schema='PROD4.'
Schema_Extn='.schema'
Stats_Extn='.stats.gz'

find $Ecomm_Full_Path -name '*.schema' -o -name '*data.dsv.gz' -o -name '*.stats.gz' | sort >  $Temp_Path/Src_FL_All_Lst.txt

find $Ecomm_Full_Path -name '*.schema' -size 0c -o -name '*data.dsv.gz' -size 0c -o -name '*.stats.gz' -size 0c | sort > $Temp_Path/Src_0B_FL_Lst.txt

find $Ecomm_Full_Path -name '*.schema' | sort >  $Temp_Path/Src_FL_All_Schema_Lst.txt

if [ `find $Ecomm_Full_Path -name '*data.dsv.gz' | wc -l` -eq 0 ]
then

	find $Ecomm_Full_Path -name '*data.dsv.gz' >  $Temp_Path/Src_FL_All_Data_Lst.txt
else

	ls -1t $(find $Ecomm_Full_Path -name '*data.dsv.gz') >  $Temp_Path/Src_FL_All_Data_Lst.txt
fi

find $Ecomm_Full_Path -name '*.stats.gz' | sort >  $Temp_Path/Src_FL_All_Stats_Lst.txt

Iteration=1

while read line;
do

   Src_FL_Cmpltd_Ind=`grep -wc $(basename $line) $Temp_Path/BDAIng_Stats_FL.csv`
   echo -e "\n"Source File Completed Indicator for $(basename $line) is $Src_FL_Cmpltd_Ind

   Src_FL_Prcs_Ind=`grep -wc $(basename $line | cut -f3-$(expr $(basename $line | awk -F_ '{print NF}') - 4 ) -d'_') $Ecomm_Full_Path/Mstr_Lst_Src_TBL_NM.txt`
   echo -e "\n"Source File Process Indicator for $(basename $line) is $Src_FL_Prcs_Ind

   if [ $Src_FL_Cmpltd_Ind -eq 0  ] && [ $Src_FL_Prcs_Ind -eq 1  ]
   then

	echo -e "\n"Iteration $Iteration started"\n"

	Src_FL=$line
	echo Processing iteration for $Src_FL
	
	Src_FL_Nm=`basename $Src_FL`
	echo Source file name is $Src_FL_Nm

	Src_FL_Path=`dirname $Src_FL`
	echo Source file name $Src_FL_Nm exists in path $Src_FL_Path/

	Dlmtr_Cnt=`echo "$Src_FL_Nm" | awk -F'_' '{ print NF }' | tr -d '\n'`

	Dlmtr_Cnt1=`expr $Dlmtr_Cnt - 4`

	Src_Nm=`echo $Src_FL_Nm | cut -f3-$Dlmtr_Cnt1 -d'_'`
	echo Source name from source file $Src_FL_Nm is $Src_Nm

	Schema_FL_Nm=`echo $Pfx_Schema$Src_Nm$Schema_Extn`
	echo Schema file name for source name $Src_FL_Nm is $Schema_FL_Nm

	Dlmtr_Cnt2=`expr $Dlmtr_Cnt - 1`

	Stats_FL_Nm_Sfx=`echo $Src_FL_Nm | cut -f1-$Dlmtr_Cnt2 -d'_'`

	Stats_FL_Nm=`echo $Stats_FL_Nm_Sfx$Stats_Extn`
	echo stats file name from source file name $Src_FL_Nm is $Stats_FL_Nm

	if [ -f $Src_FL_Path/$Schema_FL_Nm ] && [ -f $Src_FL_Path/$Stats_FL_Nm ] && [ -f $Src_FL_Path/$Src_FL_Nm  ]
	then
		echo -e "\n"Schema file $Schema_FL_Nm for $Src_FL_Nm exists, Stats file $Stats_FL_Nm for $Src_FL_Nm exists and Data file $Src_FL_Nm exists"\n"

		Schema_FL_0_Fl=`grep -wc $Src_FL_Path/$Schema_FL_Nm $Temp_Path/Src_0B_FL_Lst.txt`
		Data_FL_0_Fl=`grep -wc $Src_FL_Path/$Src_FL_Nm $Temp_Path/Src_0B_FL_Lst.txt`
		Stats_FL_0_Fl=`grep -wc $Src_FL_Path/$Stats_FL_Nm $Temp_Path/Src_0B_FL_Lst.txt`

		if [ $Schema_FL_0_Fl -eq 0 ] && [ $Data_FL_0_Fl -eq 0 ] && [ $Stats_FL_0_Fl -eq 0 ]
		then

			echo -e Schema file $Schema_FL_Nm , Stats file $Stats_FL_Nm  and Data file $Src_FL_Nm has data for BDA Ingestion process"\n"

			Dlmtr_Cnt3=`expr $Dlmtr_Cnt - 3`
			Src_FL_Dt=`echo $Src_FL_Nm | cut -f$Dlmtr_Cnt3 -d'_'`
			echo Date on source file $Src_FL_Nm is $Src_FL_Dt

			EXTRACT_DT=`date -d "$Src_FL_Dt + 1day" +"%Y-%m-%d"`
			echo Extract date for source file $Src_FL_Nm is $EXTRACT_DT

			TBL_NM=`echo $Src_Nm`
			echo Source table name for $Src_FL_Nm is $TBL_NM

			SRC_SYS=`echo $Ecomm_Src_Sys_Nm`
			echo Source system name for $Src_FL_Nm is $SRC_SYS

			SRC_FL_REC_CNT=`zcat $Src_FL_Path/$Src_FL_Nm | wc -l`
			echo Source file $Src_FL_Nm record count/s is/are $SRC_FL_REC_CNT

			echo -e "\n"BDA Ingestion attributes for $Src_FL_Nm are $Schema_FL_Nm'|'$Src_FL_Nm'|'$Stats_FL_Nm'|'$SRC_SYS'|'$TBL_NM'|'$EXTRACT_DT
			echo $SRC_SYS'|'$TBL_NM'|'$EXTRACT_DT'|'$Schema_FL_Nm'|'$Src_FL_Nm'|'$Stats_FL_Nm'|'$SRC_FL_REC_CNT >> $Temp_Path/BDA_Ingest_Attrib.txt

			awk -F'|' '!seen[$2]++' $Temp_Path/BDA_Ingest_Attrib.txt > $Temp_Path/BDA_Ingest_Attrib.tmp && mv $Temp_Path/BDA_Ingest_Attrib.tmp $Temp_Path/BDA_Ingest_Attrib.txt

			sed -i '/^$/d' $Temp_Path/BDA_Ingest_Attrib.txt

			cut -f5 -d'|' $Temp_Path/BDA_Ingest_Attrib.txt > $Temp_Path/BDA_Ingest_Attrib.bak

			while read line;
			do

				Mtch_Ind=`grep -wc $line $Temp_Path/BDAIng_Stats_FL.csv`

				if [  $Mtch_Ind -gt 0 ]
				then

					sed -i "/$line/d" $Temp_Path/BDA_Ingest_Attrib.txt

				fi

			done < $Temp_Path/BDA_Ingest_Attrib.bak

			rm $Temp_Path/BDA_Ingest_Attrib.bak

		else
			echo -e One or more files among Schema file $Schema_FL_Nm or Data file $Src_FL_Nm or Stats file $Stats_FL_Nm has no data. Hence, the source file $Src_FL_Nm for BDA ingestion process skipped"\n"
		
		fi

	else 
		echo -e "\n" One or more files among Schema file $Schema_FL_Nm or Data file $Src_FL_Nm or Stats file $Stats_FL_Nm is/are missing.

	fi
	
	Iteration=`expr $Iteration + 1`

   else

   echo -e "\n$(basename $line) is already ingested into BDA and hence skipped or the source table for source file is missing in master list of source tables to be processed and hence skipped"

   fi

done < $Temp_Path/Src_FL_All_Data_Lst.txt

DateTs_Cmpl=`date +%Y-%m-%d_%H-%M-%S_%N`

echo -e "\n"Ecomm source file search process Script $Script_Nm completed at $DateTs_Cmpl"\n"
