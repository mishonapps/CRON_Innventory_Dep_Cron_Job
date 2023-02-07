FROM openanalytics/r-base

# system libraries of general use
RUN apt-get update && apt-get install -y \
    sudo \
    pandoc \
    pandoc-citeproc \
    libcurl4-gnutls-dev \
    libcairo2-dev \
    libxt-dev \
    libssl-dev \
    libssh2-1-dev \
    libmysqlclient-dev \
    cron


# General R Packages
RUN Rscript -e "install.packages(c('dplyr','glue','data.table','intrval','stringr'), repos = 'http://cran.us.r-project.org', dependencies=TRUE)"

#Database/Connection Packages
RUN Rscript -e "install.packages(c('RCurl','DBI','RMySQL'), repos = 'http://cran.us.r-project.org', dependencies=TRUE)"



#Packages to trigger Email
RUN Rscript -e "install.packages(c('gmailr','tableHTML'), repos = 'http://cran.r-project.org', dependencies=TRUE)"

RUN apt-get update && apt-get install -y \
    libxml2-dev \
    libpq-dev

#Database/Connection Packages
RUN Rscript -e "install.packages(c('RPostgres'), repos = 'http://cran.us.r-project.org', dependencies=TRUE)"

#Additional Packages
RUN Rscript -e "install.packages(c('tidyr'), repos = 'http://cran.r-project.org', dependencies=TRUE)"

#Additional Packages
RUN Rscript -e "install.packages(c('stringi'), repos = 'http://cran.r-project.org', dependencies=TRUE)"


#test4


# copy the app to the image
RUN mkdir /root/cron
RUN mkdir /root/cron/data
RUN mkdir /root/cron/data/extractedData

COPY ./cronjob.R 				/root/cron/cronjob.R
COPY ./.Renviron 			                	/root/cron/.Renviron

COPY ./Seq1__companywideInventory.R             /root/cron/Seq1__companywideInventory.R
COPY ./Seq13_Slow_Moving_Products.R   /root/cron/Seq13_Slow_Moving_Products.R
COPY ./Seq14_Inventory_OOS.R   /root/cron/Seq14_Inventory_OOS.R


RUN touch /home/cron.log

RUN (crontab -l ; echo "40 9 * * * Rscript /root/cron/cronjob.R  >> /home/cron.log") | crontab

CMD cron && tail -f /home/cron.log