#!/bin/bash
#
#
#
source ~/.profile
determinant=1

cd ~/openmrs_dump_processing

while [ $determinant -eq 1 ] 
 do
   if [ -z "$(ls -A /home/cdr-user/openmrs_dumps/openmrs*.gz)" ]; then
     determinant=0
     echo "Directory ~/openmrs_dumps is Empty. No files to process"
  else
      echo "Directory ~/openmrs_dumps Not Empty. Copying batch of 20 dumps for processing"

     determinant=1
     
     mv `ls /home/cdr-user/openmrs_dumps/openmrs*.gz | head -20` /home/cdr-user/openmrs_dump_processing/
     
     bgarray=()
     dbarray=()
     for file in $(ls openmrs*.gz)
      do
	{
		bname=$(basename "$file" | tr -d \'.-)
		dbname=${bname:0:60}
		mysql -u$mysql_db_user -p$mysql_db_pwd -se "drop schema if exists $dbname" 
                mysql -u$mysql_db_user -p$mysql_db_pwd -se "create schema if not exists $dbname" 
		zcat $file | mysql -u$mysql_db_user -p$mysql_db_pwd $dbname -f &


        } &> /dev/null
	pid=$!
	bgarray+=($pid)
	dbarray+=("$dbname")

      done

      echo "Batch is ready for processing"
      echo "The following background processes for dump restoration have been started:"
      printf "\n"
      echo  ${bgarray[@]}
      printf "\n"

      for job in ${bgarray[@]}
	  do
	    ps -ho pid | >/dev/null grep $job
	    echo "[Background process for dump restoration with id $job is still running. waiting for completion of this task and others..]"
	    wait $job
	  done
       
      ldbarray=()  
      for database in ${dbarray[@]}
	  do
          {
           bash rds_dump.sh 0 $database &
	  } &> /dev/null
	  pidd=$!
	  ldbarray+=($pidd)
          done
      echo "Batch is ready for processing"
      echo "The following background processes for rds dump generation have been started:"
      printf "\n"
      echo  ${ldbarray[@]}
      printf "\n"

      for jobb in ${ldbarray[@]}
       do
	ps -ho pid | >/dev/null grep $jobb
	echo "[Background process for rds dumps generation with id $jobb is still running. waiting for completion of this task and others..]"
	wait $jobb
       done


      
      ddbarray=()
      for database_1 in ${dbarray[@]}
          do
          {
           mysql -u$mysql_db_user -p$mysql_db_pwd -se "drop schema if exists $database_1" &
          } &> /dev/null
          piddd=$!
          ddbarray+=($piddd)
          done

      echo "Batch is ready for processing"
      echo "The following background processes for openmrs db drop have been started:"
      printf "\n"
      echo  ${ddbarray[@]}
      printf "\n"

      for jobbb in ${ddbarray[@]}
       do
        ps -ho pid | >/dev/null grep $jobbb
        echo "[Background process for rds dumps generation with id $jobbb is still running. waiting for completion of this task and others..]"
        wait $jobbb
       done

      for file_n in $(ls openmrs*.gz)
       do
        mv -f $file_n ~/openmrs_dump_processing/temp/
       done
    
      mv -f rds*.gz ~/openmrs_dump_processing/processed/
     
               

  fi      
  done




cd ~/openmrs_dump_processing/processed/

for sentf in $(cat sent_files.txt)
  do
   echo "Removing file : $sentf .. FILE ALREADY SENT TO SERVER !!"
   rm $sentf
  done

poc_schemas=($(mysql -h 10.44.0.43 -u$mysql_db_user -p$mysql_db_pwd quarterly_reporting  -se "select extract_digits(right((s.SCHEMA_NAME),5)) from information_schema.SCHEMATA s where lower(s.SCHEMA_NAME) like '%_sid_%'"))

for dfile in $(ls -tr rds*.gz)
 do
  echo "Checking if file is from a POC site"
  extracted_site_id=$(zgrep -m 3 -o -P '.{0,0}VALUES.{0,20}' "$dfile" | cut -c 9- | sed 's/,/\n/1;P;d' | egrep -o '.{1,5}$' | uniq | sed 's/^0*//')
  if [[ " ${poc_schemas[*]} " =~ " ${extracted_site_id} " ]]; then
    echo "File Validated as from a POC site.. Sending .. ${dfile}"
    rsync  -azP $dfile cdr-user@10.44.0.43:/home/cdr-user/CDR/dumps/rds/quarterly_dumps/loaded/
    mv -f $dfile  ~/openmrs_dump_processing/processed/sent/
    echo "$dfile" >> sent_files.txt
  else
     echo "File Validated as from a EMC site.. Sending .. ${dfile}"
     rsync  -azP $dfile emc-user@10.44.0.62:/home/emc-user/CDR/dumps/rds/
     mv -f $dfile  ~/openmrs_dump_processing/processed/sent/
     echo "$dfile" >> sent_files.txt
  fi
 done


cd ~/quarterly_stats_processing
bash moh_quarterly_stats_poc.sh
