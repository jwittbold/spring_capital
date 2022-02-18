#!/bin/bash
spark-submit \
--master local \
--py-files modules/springcapital.zip \
extract.py \
&&
spark-submit \
--master local \
--py-files modules/springcapital.zip \
EOD_load.py \
&&
spark-submit \
 --master local \
 --py-files modules/springcapital.zip \
analytical_ETL.py
