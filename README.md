# myback
Myback utility

this is cli utility for backing up mysql/mariadb databases in straigthforward manner and also uploading backups to AWS glacier. Features include:

    backup InnoDb databases online, MYISAM (not online) locally or remotely (it uses percona innobackupex utility)
    full/incremental backups
    list backups local/remote
    restore backups in one command
    dump database/databases from backups in one command
    upload backups to AWS glacier
    list backups in AWS glacier
    restore backups from AWS glacier in one command
    delete backups in AWS glacier

Requirements:

    libapp-mtaws-perl (= 1.120-0vdebian1~v7~mt1) - most important dependency for glacier storage, hosted on github, also has deb/rpm repos
    percona-xtrabackup (>= 2.2.0) - most important utilities for whole project
    ssh server/client - important for remote backups
    perl-doc
    perl-doc
    perl (>= 5.8)
    libdatetime-perl
    liblog-log4perl-perl
    libmoosex-log-log4perl-perl
    libmoose-perl
    libyaml-tiny-perl
    libxml-libxml-perl
    libtext-simpletable-perl
    libdbd-sqlite3-perl
    libnamespace-autoclean-perl
    pigz - this is default compression utility used, gzip format but parallel execution, which a lot speeds up backups
    gzip
    bzip2
    mysql-server (>= 5.1) | mariadb-server (>= 5.5)
    mysql-client (>= 5.1) | mariadb-client (>= 5.5)
