#!/usr/bin/perl
#
# This library will help you pre-warm MySQL cache for MyISAM and InnoDB 
# tables. Two storage engines have different pre-warm strategy. 
#
# For MyISAM, we will run "LOAD INDEX INTO CACHE xxx", MYISAM only support 
# caching index, data block cache will rely on OS filesystem cache. 
#
# For InnoDB, the engine doesn't support LOAD INDEX command, so we have to  
# manually run SELECT statement to fully scan the table, and then we need to 
# traverse each secondary index to cache them into buffer pool.
# ( reference: http://www.percona.com/blog/2008/05/01/quickly-preloading-innodb-tables-in-the-buffer-pool/ )
#
# NOTE!!! This code assume 75% of your RAM can hold entire your database, we
# can not use all of RAM for database, operating system need some, so 75% is
# quit reasonable!
#
# More NOTE!! DO NOT USE this on production!!!! This script intends to warm
# up your machine before rolling into production.
#
use strict;
use warnings;

use DBI;
use DBD::mysql;
use Time::HiRes qw/ time /;
use Data::Dumper;
use Getopt::Std;
use POSIX qw(strftime);
use Term::ReadKey;

my $my_db;
my $my_table; # optional
my $my_user;
my $my_host;
my $my_password;

my $DEBUG = 0;

my %OPTIONS = ();

###########
# 
# Part 1: Get input options
#
###########
&init();

###########
#
# Part 2: Connect to DB
#
###########
my $dsn = "dbi:mysql:$my_db:$my_host:3306";

my $dbh = DBI->connect($dsn, $my_user, $my_password,{'RaiseError' => 1});

my %IGNORE_TABLES = ( 'heartbeat' => 1, 'checksums' => 1 );
###########
# 
# Part 3: Pull tables/indexes information from information_schema database and 
#   
# WARM UP!
#
###########

my ($myisam, $innodb ) = GatherTablesInfo();

if( @$myisam )
{
  for my $t ( @$myisam )
  {
    &Debug( "Table: " . $t );
    next if( exists( $IGNORE_TABLES{$t} ) );
    
    my $sSQL = "LOAD INDEX INTO CACHE `$t`";
    
    &Debug($sSQL);
    my $sth = $dbh->prepare($sSQL) 
                                 or die( "Can not prepare: " . $dbh->errstr );
                                 
    $sth->execute() or die( "Can not execute: " . $sth->errstr );
    &Debug("Done");
  }
}

# InnoDB
if( @$innodb )
{
  my $current_t = '';
  for my $tdata ( @$innodb )
  {
    my $table_name               = @$tdata[0];
    
    # ignore some tables
    next if( exists( $IGNORE_TABLES{$table_name} ) ); 
    
    my $non_indexed_column       = @$tdata[1];
    
    # if the index ( $secondary_indexed_name ) has multiple column,
    # $secondary_indexed_columns will be comma delimit string, order by
    # the sequence of the index, so we can use the first column in the index
    # to traverse index
    my $secondary_indexed_name    = @$tdata[2];
    my $secondary_indexed_columns = @$tdata[3];
    
    my @secondary_index_first_columns = split(/,/, $secondary_indexed_columns );    
    
    # mark current working table
    # and full scan entire table, here we require table name, one of non
    # index column in order to use in triggering full table scan    
    if( $current_t ne $table_name )
    {
      &Debug("Table: " . $table_name . " Done" );
      $current_t = $table_name;
      &Debug("Table: " . $current_t );
      
      # some of table doesn't have non-index column, every column is indexed.
      my $sFullTableScanSQL = '';
      if( $non_indexed_column )
      {
        $sFullTableScanSQL
          = "SELECT COUNT(*) FROM `$current_t` "
          . "WHERE `$non_indexed_column` = 0";
      }
      else
      {
        $sFullTableScanSQL
          = "SELECT COUNT(*) FROM `$current_t`";          
      }
        
      &Debug("Full table scan");  
      &Debug($sFullTableScanSQL);
      
      my $sth = $dbh->prepare($sFullTableScanSQL) 
                                 or die( "Can not prepare: " . $dbh->errstr );
                                   
      $sth->execute() or die( "Can not execute: " . $sth->errstr );
      &Debug("Done");      
    }
    
    # Now warm up secondary index
    # no need for primary key for clustered index
    next if( $secondary_indexed_name eq 'PRIMARY' );
    
    &Debug("Secondary index '" . $secondary_index_first_columns[0] 
         . "' traversing");
    
    my $sSecondaryIndexTraverseSQL 
      = "SELECT COUNT(*) FROM `$current_t` "
      . "WHERE `"
      . $secondary_index_first_columns[0] 
      . "` LIKE '%0%';";
      
    &Debug($sSecondaryIndexTraverseSQL);
    my $sth = $dbh->prepare($sSecondaryIndexTraverseSQL) 
                               or die( "Can not prepare: " . $dbh->errstr );
                                 
    $sth->execute() or die( "Can not execute: " . $sth->errstr );
    &Debug("Done");       
  }
}

##############
#
# Helper Functions
#
##############

##############
#
# Query information_schema and gather tables/index information for either
# InnoDB or MyISQM
# 
# Return: two array reference, first one contains MyISAM tables information
#         , second one contains InnoDB tables information.
#
##############
sub GatherTablesInfo
{
  my @MyISAMData = ();
  my @InnoDBData = ();
  my $sSql;
  
  my $sTableWhere = ''; 
  if( $my_table ) 
  {
    $sTableWhere = " AND table_name='$my_table' ";
  }
  
  ####
  # MyISAM
  ####
  my $MyISAMSth 
    = $dbh->prepare( 'SELECT table_name FROM information_schema.tables '
                   . 'WHERE engine="myisam" AND table_schema="' 
                   . $my_db . '" ' 
                   . $sTableWhere )
                     or die "Couldn't prepare statement: " . $dbh->errstr;
                 
  $MyISAMSth->execute( )
                   or die "Couldn't execute statement: " . $MyISAMSth->errstr;
                         
  while( my @data = $MyISAMSth->fetchrow_array( ) )
  {
    push( @MyISAMData, $data[0] );
  }                                      
  
  ####
  # InnoDB
  ####
  $sSql  = 'SELECT tables.table_name, '
         . '( SELECT SUBSTRING_INDEX(GROUP_CONCAT(column_name),",",1) FROM information_schema.columns '
           . 'WHERE engine="innodb" AND table_schema="' . $my_db . '" '
           . 'AND column_key="" AND table_name=tables.table_name GROUP BY table_name ) AS non_index_column, ' 
         . 'index_name, '  
         . 'GROUP_CONCAT( statistics.column_name ORDER BY SEQ_IN_INDEX) '
         . 'FROM information_schema.tables JOIN '
         . '     information_schema.statistics USING(table_name,table_schema) '         
         . 'WHERE engine="innodb" AND table_schema="' 
         . $my_db . '" '
         . $sTableWhere
         . 'GROUP BY tables.table_name, index_name'; 
         
  &Debug($sSql);
           
  my $InnoDBSth
    = $dbh->prepare( $sSql ) 
                         or die "Couldn't prepare statement: " . $dbh->errstr;
                         
  $InnoDBSth->execute( )
                   or die "Couldn't execute statement: " . $InnoDBSth->errstr;

  while( my @data = $InnoDBSth->fetchrow_array( ) )
  {                  
    push( @InnoDBData, \@data );
  }      
  
  return ( \@MyISAMData, \@InnoDBData );
}

sub Debug($)
{
  my $sMsg = shift;
  
  return if( ! $DEBUG );   
  
  my $sDatestring = strftime "%F %H:%M:%S", localtime;
  
  print "$sDatestring: $sMsg\n";
}

##############
#
# Gather input options
#
##############
sub init( )
{
  getopts( 'hgd:t:u:s:p:', \%OPTIONS );

  if( ! exists( $OPTIONS{'d'} ) &&
      ! exists( $OPTIONS{'u'} ) &&
      ! exists( $OPTIONS{'s'} ) &&
      ! exists( $OPTIONS{'p'} ) )
  {
    &usage( );
  }

  if( exists( $OPTIONS{'h'} ) )
  {
    &usage( );
  }

  $my_db   = $OPTIONS{'d'};
  $my_user = $OPTIONS{'u'};
  $my_host = $OPTIONS{'s'};
  
  if( $OPTIONS{'p'} )
  {    
    print "Password:";
    ReadMode('noecho');
    chomp($my_password = <STDIN>);   
    ReadMode(0);
  }
  
  if( exists( $OPTIONS{'t'} ) )
  { 
    $my_table = $OPTIONS{'t'};
  }
  
  if( exists( $OPTIONS{'g'} ) )
  {
    $DEBUG = 1;
  }
}

##############
#
# Display usage information
#
##############
sub usage( )
{
  print
     "\n"
   . "  prewarm_mysql_cache.pl -d:t:u:h:p: -h\n"
   . "\n"
   . "    -d  mysql database name\n"
   . "    -t  [optional] mysql table name\n"
   . "    -u  mysql user name\n"
   . "    -s  mysql hostname\n"
   . "    -p  mysql password\n"   
   . "\n"
   . "    -g  enable debug output\n"
   . "    -h  show this information\n"
   . "\n\n"
   . 'ex: perl prewarm_mysql_cache.pl -salldb-ro-internal.naiad.db.flyingcroc.net -daccounts_archive -unaiad -p"xxxxx"'
   . "\n\n"
   ;

  exit;
}
