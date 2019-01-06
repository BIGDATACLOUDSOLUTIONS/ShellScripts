#set -x
#! /bin/sh

#Script Name    : Ecomm_BDA_Ing_Mstr.sh 
#Description    : This script ingests source files from NAS ecomm path to BDA.search the ecomm source file path for ecomm files and
#                 report only the files which has schema, stats & data files with data
#Owner          : eComm DS Dev Team
#Date Created   : 2018-09-13
#Usage          : sh Ecomm_BDA_Ing_Mstr.sh $(cat Ecomm_BDA_Ing.conf) 
#
#==========================================  CHANGES START  ==================================================
#
#SL_No  Change_Ref#     Changed_By      Change_Date     Change_Comments
#=====  ===========     ==========      ===========     ===============
#
#
#
#==========================================  CHANGES END  ====================================================


Base_DNA_Path="/opt/ftps/ISSuite/IS/Data/DNA/"
Base_NAS_Path="/opt/ftps/ISSuite/IS/NAS/"
Temp_Path=$Base_DNA_Path/TempFiles/
Script_Path=$Base_DNA_Path/ScriptFiles/
Src_Fl_NAS_DNA_Path=$Base_NAS_Path/DNA/SourceFiles/
Ecomm_Full_Path=$Src_Fl_NAS_DNA_Path/ecomm/
Script_Nm=`basename $0 | cut -f1 -d'.'`
FDP_Full_Path=$Base_NAS_Path/FDP/SourceFiles/eComm/
#Rcvr_Mail_Addr="suman.pothuri@worldpay.com,Ajay.Chaudhari@worldpay.com"
Rcvr_Mail_Addr="suman.pothuri@worldpay.com"


Prcs_Strtd_TS=$(date '+%Y%m%d%H%M%S')
Prcs_Strtd_TS_Fmt=`date +%Y-%m-%d_%H-%M-%S_%N`

Log_File=${Script_Nm}_$Prcs_Strtd_TS_Fmt.log
Err_Log_File=${Script_Nm}_Error_$Prcs_Strtd_TS_Fmt.log

exec 1> $Temp_Path/$Log_File 2> $Temp_Path/$Err_Log_File

echo -e BDA Ingestion process Script $0 started at $Prcs_Strtd_TS_Fmt"\n"
echo -e Source files NAS DNA path is $Src_Fl_NAS_DNA_Path"\n"
echo -e Ecomm Source file NAS DNA path is $Src_Fl_NAS_DNA_Path/ecomm/"\n"
echo -e Ecomm FDP Source files path is $FDP_Full_Path"\n"

echo -e Temp files truncation started at `date +%Y-%m-%d_%H-%M-%S_%N`"\n"
#truncate -s 0 $Temp_Path/BDAIng_Stats_FL.csv
truncate -s 0 $Temp_Path/Src_FL_All_Lst.txt $Temp_Path/Src_0B_FL_Lst.txt $Temp_Path/Src_FL_All_Schema_Lst.txt $Temp_Path/Src_FL_All_Data_Lst.txt $Temp_Path/Src_FL_All_Stats_Lst.txt $Temp_Path/Agg_Wrd_Srch_FL.txt $Temp_Path/BDA_Ingest_Attrib.txt $Temp_Path/ecomm_TABLE_LIST.txt $Temp_Path/BDAIng_Stats_FL.csv


echo -e Temp files truncation completed at `date +%Y-%m-%d_%H-%M-%S_%N`"\n"

cd /opt/IBM/InformationServer/Server/DSEngine/
. ./dsenv

Src_Data_FL_Exist_Ind=0

Delta_TS_Diff_Sec=0
Btch_Cntr=1
Pfx_Schema='PROD4.'
Schema_Extn='.schema'
Stats_Extn='.stats.gz'

Job_Run_Intrvl=$1
Job_Sleep_Intrvl=$2
Batch_Size=$3
Batch_Sleep_Intrvl=$4

echo Job Run Interval is $Job_Run_Intrvl
echo Job Sleep Interval is $Job_Sleep_Intrvl seconds
echo Batch Size is $Batch_Size
echo Batch Sleep Interval is $Batch_Sleep_Intrvl seconds $'\n'


Job_Run_Intrvl_Hr=${Job_Run_Intrvl:0:2}
Job_Run_Intrvl_Min=${Job_Run_Intrvl:3:2}
Job_Run_Intrvl_Sec=${Job_Run_Intrvl:6:2}


Job_Run_Intrvl_HrtoSec=`expr $Job_Run_Intrvl_Hr \* 60 \* 60`
Job_Run_Intrvl_MintoSec=`expr $Job_Run_Intrvl_Min \* 60 `
Job_Run_Intrvl_Secs=`expr $Job_Run_Intrvl_Sec \* 1 `
Job_Run_Intrvl_Tot_Seconds=`expr $Job_Run_Intrvl_HrtoSec + $Job_Run_Intrvl_MintoSec + $Job_Run_Intrvl_Sec`

echo Job Run Interval in hours equivalent to seconds is $Job_Run_Intrvl_HrtoSec seconds
echo Job Run Interval in minutes equivalent to seconds is $Job_Run_Intrvl_MintoSec seconds
echo Job Run Interval in seconds equivalent to seconds is $Job_Run_Intrvl_Secs seconds
echo Job Run Interval is $Job_Run_Intrvl and Job Run Interval has $Job_Run_Intrvl_Hr hours $Job_Run_Intrvl_Min minutes and $Job_Run_Intrvl_Sec seconds
echo Total Job Run Interval in seconds is/are $Job_Run_Intrvl_Tot_Seconds seconds$'\n'

while [ $Delta_TS_Diff_Sec -le $Job_Run_Intrvl_Tot_Seconds  ] || [ $Src_Data_FL_Exist_Ind -eq 1  ]
do
	
	echo -e Search for files required for FDP and copy them to NAS FDP path  started at `date +%Y-%m-%d_%H-%M-%S_%N`"\n"

	touch $Temp_Path/FDP_mail_msg.txt
	chmod 775 $Temp_Path/FDP_mail_msg.txt
	FDP_FL_CP_FL=0
	
	echo -e "Please find below FDM source files copy status\n" >> $Temp_Path/FDP_mail_msg.txt
	echo -e "List of ecomm sources in scope for FDM copy from NAS DNA to NAS FDP path on server\n" >> $Temp_Path/FDP_mail_msg.txt

	cat $Ecomm_Full_Path/Mstr_FDP_Copy_SRC_Lst.txt >> $Temp_Path/FDP_mail_msg.txt
	cat $Ecomm_Full_Path/Mstr_FDP_Copy_SRC_Lst.txt

        while read line
        do

                FDP_FL_CNT=`find $Ecomm_Full_Path/$line/ -name "*$line*data.dsv.gz" | wc -l`
		
		echo -e "\n"count of files available for source $line is/are $FDP_FL_CNT"\n"		

		if [ $FDP_FL_CNT -eq 0 ]
                then

			echo -e "\n"No source file exists for source $line in path $Ecomm_Full_Path/$line/ >> $Temp_Path/FDP_mail_msg.txt
			echo -e "\n"No source file exists for source $line in path $Ecomm_Full_Path/$line/
                fi

                while [ $FDP_FL_CNT -gt 0  ]
                do

                	Src_FL_Chk=`ls $Ecomm_Full_Path/$line/*$line*data.dsv.gz | head -$FDP_FL_CNT | tail -1 `
                        echo check for existence of source file $Src_FL_Chk

                        if [ -f $Src_FL_Chk ] && [ -f "$Ecomm_Full_Path/$line/PROD4.$line.schema" ] && [ -f "$(echo $Src_FL_Chk | sed 's/_data.dsv.gz//').stats.gz" ] && [ ! -f $FDP_Full_Path/$(basename $Src_FL_Chk) ]
                        then

                        	echo -e "\n"found files for source $line under $Ecomm_Full_Path/$line/ in iteration $FDP_FL_CNT"\n"
                                ls -ltr $Src_FL_Chk "$Ecomm_Full_Path/$line/PROD4.$line.schema" "$(echo $Src_FL_Chk | sed 's/_data.dsv.gz//').stats.gz"
				FDP_FL_CP_FL=1
				cp $Src_FL_Chk "$Ecomm_Full_Path/$line/PROD4.$line.schema" "$(echo $Src_FL_Chk | sed 's/_data.dsv.gz//').stats.gz" $FDP_Full_Path
                               
				 
				if [[ $? = 0 ]]
                                then

                                	echo -e "\n"Source file $Src_FL_Chk and its schema and stats file copied to the path $FDP_Full_Path >> $Temp_Path/FDP_mail_msg.txt
                                
				else

                                        echo -e "\n"Issue seen while copying Source file $Src_FL_Chk and its schema and stats file to the path $FDP_Full_Path >> $Temp_Path/FDP_mail_msg.txt

                                fi

			elif [ ! -f "$(echo $Src_FL_Chk | sed 's/_data.dsv.gz//').stats.gz" ] || [ ! -f "$Ecomm_Full_Path/$line/PROD4.$line.schema" ]
			then

                                echo -e "\n"Stats or Schema file for source $Src_FL_Chk is/are missing and hence copy to FDP NAS skipped>> $Temp_Path/FDP_mail_msg.txt
				echo -e "\n"Stats or Schema file for source $Src_FL_Chk is/are missing and hence copy to FDP NAS skipped

			else
				sleep 0.001
                        fi

                        FDP_FL_CNT=`expr $FDP_FL_CNT - 1`

		done
                
        done < $Ecomm_Full_Path/Mstr_FDP_Copy_SRC_Lst.txt

	chmod 775 $FDP_Full_Path/*data.dsv.gz*

	if [[ $FDP_FL_CP_FL = 1 ]] 
	then 
		cat $Temp_Path/FDP_mail_msg.txt | mail -s "FDM Source files availability in server `hostname` on `date +%Y-%m-%d`" "$Rcvr_Mail_Addr"
	fi

	rm $Temp_Path/FDP_mail_msg.txt
	
	echo -e "\n"Search for files required for FDP and copy them to NAS FDP path completed at `date +%Y-%m-%d_%H-%M-%S_%N`"\n"

	sh $Script_Path/Ecomm_BDA_Pre_Valdn.sh $Src_Fl_NAS_DNA_Path "ecomm"

	Src_Avbl_Cnt=`wc -l $Temp_Path/BDA_Ingest_Attrib.txt | cut -f1 -d' '`
	if [ $Src_Avbl_Cnt -gt 0  ]
	then

		echo -e Pull source file count from Attrib file started at `date +%Y-%m-%d_%H-%M-%S`"\n"

		sh $Script_Path/Ecomm_BDA_SrcWise_FLCnt.sh $Temp_Path/BDA_Ingest_Attrib.txt

		echo -e Pull source file count from Attrib file completed at `date +%Y-%m-%d_%H-%M-%S`"\n"

		Files_For_Ingest_Cnt=`wc -l $Temp_Path/BDA_Ingest_Attrib.txt | cut -f1 -d' ' | tr -d '/n'`
        	echo "Files for ingestion count is $Files_For_Ingest_Cnt"

        	if [ $Files_For_Ingest_Cnt -gt 0  ]
        	then

                	
			echo -e "\n"Attrib file contents in DS loop"\n"
		        cat $Temp_Path/BDA_Ingest_Attrib.txt

			while read line
			do

				Prcs_FL=$Ecomm_Full_Path$(echo $line | cut -d'|' -f2)/$(echo $line | cut -d'|' -f5)
				Prcs_FL_dsv=`echo $Ecomm_Full_Path$(echo $line | cut -d'|' -f2)/$(echo $line | cut -d'|' -f5) | cut -d'.' -f1-2`
				Metadata_FL=$Ecomm_Full_Path$(echo $line | cut -d'|' -f2)/$(echo $line | cut -d'|' -f4)
				Metadata_FL1=$Ecomm_Full_Path$(echo $line | cut -d'|' -f2)/$(echo $line | cut -d'|' -f4)1

				echo -e Process to Unzip source file, remove spaces and formatting dates started at `date +%Y-%m-%d_%H-%M-%S`"\n"

#				gunzip -d $Prcs_FL ; python $Script_Path/Src_FL_Scrub.py $Prcs_FL_dsv
#				gunzip -c $Prcs_FL > $Prcs_FL_dsv ; python $Script_Path/Src_FL_Scrub.py $Prcs_FL_dsv
#				gunzip -c $Prcs_FL > $Prcs_FL_dsv ; chmod 775 $Prcs_FL_dsv ; python $Script_Path/Src_FL_Scrub.py $Prcs_FL_dsv
				gunzip -d $Prcs_FL ; chmod 775 $Prcs_FL_dsv ; python $Script_Path/Src_FL_Scrub.py $Prcs_FL_dsv

				echo -e Process to Unzip source file, remove spaces and formatting dates completed at `date +%Y-%m-%d_%H-%M-%S`"\n"

				echo -e Process to handle special characters started at `date +%Y-%m-%d_%H-%M-%S`"\n"

				set -x
	                        sed -n '2,$ p' $Ecomm_Full_Path/Mstr_Lst_Spcl_Chars.txt | while read line
        	                do

                	                echo -e "\n"Scrubing "$line" special character from the source file"\n"
                        	        sed -i -e 's/'$line'//g' $Prcs_FL_dsv

                        	done
				set +x		

				echo -e Process to handle special characters completed at `date +%Y-%m-%d_%H-%M-%S`"\n"

				echo -e Process to modify schema file from character to variable character started at `date +%Y-%m-%d_%H-%M-%S`"\n"

				cp $Metadata_FL $Metadata_FL1
				chmod 775 $Metadata_FL1
				sed -i -e 's/string\[[1-9]/string\[max=&/g' $Metadata_FL1
				sed -i -e 's/=string\[/=/g' $Metadata_FL1

				echo -e Process to modify schema file from character to variable character completed at `date +%Y-%m-%d_%H-%M-%S`"\n"

			done < $Temp_Path/BDA_Ingest_Attrib.txt

			echo -e ETL Datastage Load process started at `date +%Y-%m-%d_%H-%M-%S`"\n"

			$DSHOME/bin/dsjob -run -mode NORMAL -param NOTIFY_PARAM_SET="(As pre-defined)" -param CTL_ECOMM_PARAM_SET="(As pre-defined)" -param PROJ_PATHS_PARAM_SET="(As pre-defined)" -param BATCH_SIZE=$Batch_Size -param SLP_INTRVL=$Batch_Sleep_Intrvl -param TEMP_PATH=$Temp_Path DNA SEQ_BDA_LAND_ECOMM_MSTR.$Btch_Cntr
                
			sh $Script_Path/DSJob_Prcs_Status.sh $Batch_Sleep_Intrvl SEQ_BDA_LAND_ECOMM_MSTR

			echo -e ETL Datastage Load process completed at `date +%Y-%m-%d_%H-%M-%S`"\n"

                	Btch_Cntr=`expr $Btch_Cntr + 1`
                	echo Batch counter is/are $Btch_Cntr

        	else

			echo -e "\n"Waiting for source files to arrive and will be sleeping for $Job_Sleep_Intrvl seconds"\n"
                	sleep $Job_Sleep_Intrvl

        	fi

	else

		echo -e "\n"Waiting for source files to arrive and will be sleeping for $Job_Sleep_Intrvl seconds"\n"
		sleep $Job_Sleep_Intrvl

	fi

        Curnt_TS=$(date '+%Y%m%d%H%M%S')

        echo current timestamp is $Curnt_TS

        datetime1=$Curnt_TS
        datetime2=$Prcs_Strtd_TS

        seconds1=$(date --date "$(echo "$datetime1" | sed -nr 's/(....)(..)(..)(..)(..)(..)/\1-\2-\3 \4:\5:\6/p')" +%s)
        seconds2=$(date --date "$(echo "$datetime2" | sed -nr 's/(....)(..)(..)(..)(..)(..)/\1-\2-\3 \4:\5:\6/p')" +%s)


        Delta_TS_Diff_Sec=$((seconds1 - seconds2))
        echo "difference of Timestamps is $Delta_TS_Diff_Sec seconds"    # "45197940 seconds"

	truncate -s 0 $Temp_Path/BDA_Ingest_Attrib.txt

	Src_Data_FL_Exist_Ind=0

	find $Ecomm_Full_Path -name '*data.dsv.gz' >  $Temp_Path/Src_FL_All_Data_Lst.txt

	echo -e Process to search for existence of data files, availability of data,schema,stats and they have data,not processed,source exists in master table list started at `date +%Y-%m-%d_%H-%M-%S`"\n"

	while read line;
	do

		Src_FL_Prcs_Ind1=`grep -wc $(basename $line | cut -f3-$(expr $(basename $line | awk -F_ '{print NF}') - 4 ) -d'_') $Ecomm_Full_Path/Mstr_Lst_Src_TBL_NM.txt`

		if [  `grep -wc $(basename $line) $Temp_Path/BDAIng_Stats_FL.csv` -eq 0 ] && [ $Src_FL_Prcs_Ind1 -eq 1  ]
		then
		
	
			if [ -s $line  ] && [ -s $(dirname $line)/$Pfx_Schema$(basename $line | cut -f3-$(expr $(basename $line | awk -F_ '{print NF}') - 4 ) -d'_')$Schema_Extn  ] && [ -s $(dirname $line)/$(basename $line | cut -f1-$(expr $(basename $line | awk -F_ '{print NF}') - 1 ) -d'_')$Stats_Extn  ]
			then

				Src_Data_FL_Exist_Ind=1
				echo -e "\n"Source file exists and Src_Data_FL_Exist_Ind value is $Src_Data_FL_Exist_Ind
				echo -e "\n"Source File Process Indicator is $Src_FL_Prcs_Ind1
				break

			else

				echo -e "\n"For Source $(basename $line) - Source data file is of zero size or Source Schema file is missing or Source Schema file is of zero size or Source Stats file is missing or Source Stats file is of zero size

			fi
		
		else
		
			Src_Data_FL_Exist_Ind=0
		
		fi
        
	done < $Temp_Path/Src_FL_All_Data_Lst.txt

	echo -e Process to search for existence of data files, availability of data,schema,stats and they have data,not processed,source exists in master table list completed at `date +%Y-%m-%d_%H-%M-%S`"\n"

	if [ $Src_Data_FL_Exist_Ind -eq 0  ]
	then

		echo -e "\n"No Source files seen and Src_Data_FL_Exist_Ind value is $Src_Data_FL_Exist_Ind
                echo -e "\n"Source File Process Indicator is $Src_FL_Prcs_Ind1

	fi

done

Prcs_Cmpltd_TS=`date +%Y-%m-%d_%H-%M-%S_%N`

echo $'\n'BDA ingestion process script $0 completed at $Prcs_Cmpltd_TS$'\n'
